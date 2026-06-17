"""
dq_framework.monitoring.lineage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:class:`DQLineage` — table lineage impact analysis using Unity Catalog
system.access.table_lineage. When DQ fails on a table, traverse downstream
dependents and write an impact report to dq_lineage_impact_audit.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

_LINEAGE_TABLE = "system.access.table_lineage"

_IMPACT_SCHEMA = StructType([
    StructField("failed_table",      StringType(),  False),
    StructField("downstream_table",  StringType(),  False),
    StructField("hop_depth",         IntegerType(), False),
    StructField("failed_dq_score",   StringType(),  True),
    StructField("impact_severity",   StringType(),  False),
    StructField("downstream_owner",  StringType(),  True),
])


def _severity(hop: int, score: float | None) -> str:
    if hop == 1 and (score is None or score < 80):
        return "HIGH"
    if hop <= 2:
        return "MEDIUM"
    return "LOW"


class DQLineage:
    """
    Lineage impact analysis for DQ failures.

    Queries ``system.access.table_lineage`` to find all downstream tables
    that depend on tables where DQ has failed, then writes an impact report.

    Args:
        spark         : Active SparkSession.
        impact_table  : Fully qualified name of ``dq_lineage_impact_audit``.
        config_table  : Fully qualified name of ``dq_config`` (to resolve owners).
        max_depth     : Maximum lineage hops to traverse (default 3).
    """

    def __init__(
        self,
        spark:        SparkSession,
        impact_table: str,
        config_table: str,
        max_depth:    int = 3,
    ):
        self.spark        = spark
        self.impact_table = impact_table
        self.config_table = config_table
        self.max_depth    = max_depth

    # ------------------------------------------------------------------
    # Lineage traversal
    # ------------------------------------------------------------------

    def get_downstream(self, table_name: str) -> list[dict]:
        """
        BFS traversal of downstream tables up to ``max_depth`` hops.
        Returns a list of dicts: {downstream_table, hop_depth, source_table}.
        """
        logger.info(f"Traversing lineage downstream of '{table_name}' (max_depth={self.max_depth})")
        visited: set[str]       = {table_name.lower()}
        results: list[dict]     = []
        current_level: set[str] = {table_name}

        try:
            for hop in range(1, self.max_depth + 1):
                if not current_level:
                    break

                in_clause = ", ".join(f"'{t}'" for t in current_level)
                df = self.spark.sql(f"""
                    SELECT DISTINCT
                        source_table_full_name,
                        target_table_full_name
                    FROM {_LINEAGE_TABLE}
                    WHERE source_table_full_name IN ({in_clause})
                      AND target_table_full_name IS NOT NULL
                      AND target_table_full_name != source_table_full_name
                """)

                next_level: set[str] = set()
                for row in df.collect():
                    target = row["target_table_full_name"]
                    if target and target.lower() not in visited:
                        results.append({
                            "source_table":     row["source_table_full_name"],
                            "downstream_table": target,
                            "hop_depth":        hop,
                        })
                        next_level.add(target)
                        visited.add(target.lower())

                current_level = next_level

        except Exception as e:
            logger.exception(f"Lineage traversal failed for '{table_name}': {e}")

        logger.info(f"Found {len(results)} downstream table(s) for '{table_name}'")
        return results

    def get_upstream(self, table_name: str) -> DataFrame:
        """Returns tables that this table reads FROM (upstream sources)."""
        logger.info(f"Fetching upstream lineage for '{table_name}'")
        try:
            return self.spark.sql(f"""
                SELECT DISTINCT
                    source_table_full_name  AS upstream_table,
                    target_table_full_name  AS table_name,
                    event_date
                FROM {_LINEAGE_TABLE}
                WHERE target_table_full_name = '{table_name}'
                ORDER BY event_date DESC
            """)
        except Exception as e:
            logger.exception(f"Upstream lineage failed for '{table_name}': {e}")
            return self.spark.createDataFrame([], "upstream_table STRING, table_name STRING, event_date DATE")

    # ------------------------------------------------------------------
    # Impact report
    # ------------------------------------------------------------------

    def build_impact_report(
        self,
        failed_tables: list[tuple[str, float | None]],
    ) -> DataFrame:
        """
        Build an impact report for a list of (table_name, dq_score) tuples.

        Traverses lineage for each failed table, resolves downstream owners
        from dq_config, and returns a DataFrame with impact severity.
        """
        if not failed_tables:
            return self.spark.createDataFrame([], _IMPACT_SCHEMA)

        # Load owner map from dq_config
        try:
            owner_map: dict[str, str] = {
                row["table_name"]: (row["sla_owner"] or "")
                for row in self.spark.table(self.config_table)
                                     .select("table_name", "sla_owner")
                                     .collect()
            }
        except Exception:
            owner_map = {}

        rows = []
        for table_name, dq_score in failed_tables:
            downstream = self.get_downstream(table_name)
            for item in downstream:
                hop    = item["hop_depth"]
                target = item["downstream_table"]
                rows.append((
                    table_name,
                    target,
                    hop,
                    str(round(dq_score, 2)) if dq_score is not None else None,
                    _severity(hop, dq_score),
                    owner_map.get(target, None),
                ))

        if not rows:
            return self.spark.createDataFrame([], _IMPACT_SCHEMA)

        return self.spark.createDataFrame(rows, _IMPACT_SCHEMA)

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------

    def save_impact(self, df_impact: DataFrame) -> bool:
        """Appends the impact report to ``dq_lineage_impact_audit``."""
        if df_impact is None or df_impact.count() == 0:
            logger.info("No lineage impact rows to write.")
            return True
        try:
            (
                df_impact
                .withColumn("partition_date", F.current_date())
                .withColumn("created_at",     F.current_timestamp())
                .write
                .partitionBy("partition_date")
                .mode("append")
                .saveAsTable(self.impact_table)
            )
            logger.info(f"Lineage impact written to '{self.impact_table}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to write lineage impact: {e}")
            return False

    # ------------------------------------------------------------------
    # Convenience: run full pipeline in one call
    # ------------------------------------------------------------------

    def run(self, batch_results: list) -> DataFrame:
        """
        Full lineage impact pipeline from DQBatchResult list.

        Extracts failed tables, builds the impact report, saves it, and
        returns the impact DataFrame for downstream use (notifications etc.).
        """
        failed = [
            (r.table_name, getattr(r, "dq_score", None))
            for r in batch_results
            if not r.success
        ]

        if not failed:
            logger.info("No failed tables — lineage impact skipped.")
            return self.spark.createDataFrame([], _IMPACT_SCHEMA)

        logger.info(f"Building lineage impact for {len(failed)} failed table(s): {[f[0] for f in failed]}")
        df_impact = self.build_impact_report(failed)
        self.save_impact(df_impact)
        return df_impact


__all__ = ["DQLineage"]
