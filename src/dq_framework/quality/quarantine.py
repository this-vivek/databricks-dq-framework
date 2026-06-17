"""
dq_framework.quarantine
~~~~~~~~~~~~~~~~~~~~~~~~~
:class:`DQQuarantine` — routes rows with DQ errors to an isolated Delta table.

Quarantine tables are partitioned by ``_partition_date`` and accumulate bad rows
across runs. Each quarantined row retains the original ``_errors`` struct so
downstream consumers know exactly which rules failed.

Example::

    quarantine = DQQuarantine(spark, catalog_schema="prod.dq")

    # df_full = runner.apply_dq_rules(..., dq_flag=False)
    clean_df, ok = quarantine.route(df_full, source_table="prod.sales.orders")
    display(clean_df)    # valid rows only
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DQQuarantine:
    """
    Routes error rows from a DQ run to a separate quarantine Delta table.

    The quarantine table name is derived automatically as
    ``<catalog_schema>.<source_table_short_name>_quarantine``
    unless overridden with ``target_table``.

    Args:
        spark          : Active SparkSession.
        catalog_schema : Default ``catalog.schema`` prefix for quarantine tables.
    """

    def __init__(self, spark: SparkSession, catalog_schema: str):
        self.spark          = spark
        self.catalog_schema = catalog_schema

    def _target_name(self, source_table: str, target_table: str | None) -> str:
        if target_table:
            return target_table
        short = source_table.split(".")[-1]
        return f"{self.catalog_schema}.{short}_quarantine"

    def route(
        self,
        df_full:      DataFrame,
        source_table: str,
        target_table: str | None = None,
        write_mode:   str = "append",
    ) -> tuple[DataFrame, bool]:
        """
        Splits ``df_full`` into clean rows and error rows.

        Error rows (``_errors IS NOT NULL AND size(_errors) > 0``) are written to
        the quarantine table.  Clean rows are returned as a DataFrame (without
        ``_errors`` / ``_warnings`` columns) for the caller to use downstream.

        Args:
            df_full      : Full output from ``apply_dq_rules(dq_flag=False)`` —
                           all original columns plus ``_errors`` / ``_warnings``.
            source_table : Fully qualified source table name.
            target_table : Override quarantine table name.
            write_mode   : ``"append"`` (default) or ``"overwrite"``.

        Returns:
            ``(clean_df, write_success)``
        """
        if "_errors" not in df_full.columns:
            logger.warning("_errors column not found in df_full — skipping quarantine.")
            return df_full, True

        error_mask = F.col("_errors").isNotNull() & (F.size(F.col("_errors")) > 0)
        extra_cols = [c for c in ("_errors", "_warnings") if c in df_full.columns]

        df_errors = df_full.filter(error_mask)
        df_clean  = df_full.filter(~error_mask).drop(*extra_cols)

        error_count = df_errors.count()
        logger.info(f"Quarantine | source='{source_table}' | {error_count} error row(s).")

        if error_count == 0:
            return df_clean, True

        target = self._target_name(source_table, target_table)
        try:
            (
                df_errors
                .withColumn("_source_table",  F.lit(source_table))
                .withColumn("_quarantine_ts", F.current_timestamp())
                .withColumn("_partition_date", F.current_date())
                .write
                .partitionBy("_partition_date")
                .mode(write_mode)
                .saveAsTable(target)
            )
            logger.info(f"Quarantined {error_count} row(s) → '{target}'.")
            print(f"[DQQuarantine] {error_count} row(s) quarantined → '{target}'.")
            return df_clean, True
        except Exception as e:
            logger.exception(f"Failed to write quarantine for '{source_table}': {e}")
            return df_clean, False

    def get_quarantine(
        self,
        source_table: str,
        target_table: str | None = None,
    ) -> DataFrame:
        """Returns the quarantine DataFrame for the given source table."""
        target = self._target_name(source_table, target_table)
        return self.spark.table(target)

    def purge_quarantine(
        self,
        source_table: str,
        before_date:  str,
        target_table: str | None = None,
    ) -> bool:
        """
        Deletes quarantine rows older than ``before_date`` (``YYYY-MM-DD``).

        Uses Delta ``DELETE`` — requires the table to already exist.
        """
        target = self._target_name(source_table, target_table)
        try:
            self.spark.sql(f"""
                DELETE FROM {target}
                WHERE _partition_date < '{before_date}'
            """)
            logger.info(f"Purged quarantine rows before {before_date} from '{target}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to purge quarantine '{target}': {e}")
            return False


__all__ = ["DQQuarantine"]
