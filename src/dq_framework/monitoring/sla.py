"""
dq_framework.sla
~~~~~~~~~~~~~~~~~
:class:`DQSLAChecker` — detects SLA breaches by comparing DQ scores and null rates
against per-table thresholds stored in ``dq_config``.

Optional SLA columns in ``dq_config`` (added by :class:`dq_framework.setup.DQSetup`):
    - ``min_quality_pct`` : minimum acceptable DQ score (0-100).
    - ``max_null_rate``   : maximum acceptable null rate across all columns (0-100).
    - ``sla_owner``       : notification target (email / Slack handle).

Breaches are appended to ``dq_sla_breach_audit``.

Example::

    sla = DQSLAChecker(
        spark,
        audit_table  = "prod.dq.dq_audit",
        config_table = "prod.dq.dq_config",
        breach_table = "prod.dq.dq_sla_breach_audit",
    )
    df_breaches = sla.check_breaches()
    display(df_breaches)
    sla.save_breaches()
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class DQSLAChecker:
    """
    Checks DQ audit results against per-table SLA thresholds.

    Args:
        spark        : Active SparkSession.
        audit_table  : Fully qualified ``dq_audit`` table name.
        config_table : Fully qualified ``dq_config`` table name.
        breach_table : Fully qualified ``dq_sla_breach_audit`` table name.
    """

    def __init__(
        self,
        spark:        SparkSession,
        audit_table:  str,
        config_table: str,
        breach_table: str,
    ):
        self.spark        = spark
        self.audit_table  = audit_table
        self.config_table = config_table
        self.breach_table = breach_table

    def _latest_audit(self, partition_date: str | None = None) -> DataFrame:
        """Returns one audit row per table — the most recent run for the given date."""
        df = self.spark.table(self.audit_table).filter(F.col("active_flag") == 1)
        if partition_date:
            df = df.filter(F.col("partition_date") == F.lit(partition_date))
        else:
            max_dates = (
                df.groupBy("config_id", "table_name")
                  .agg(F.max("partition_date").alias("partition_date"))
            )
            df = df.join(max_dates, on=["config_id", "table_name", "partition_date"])

        return (
            df.withColumn("_rn", F.row_number().over(
                Window.partitionBy("config_id", "table_name", "partition_date")
                      .orderBy(F.col("audit_ts").desc())
            ))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select("config_id", "table_name", "partition_date", "dq_score", "audit_ts")
        )

    def check_breaches(self, partition_date: str | None = None) -> DataFrame:
        """
        Joins the latest audit run against SLA thresholds from dq_config.

        Args:
            partition_date : Restrict check to a specific date (``YYYY-MM-DD``).
                             Defaults to the latest partition per table.

        Returns:
            DataFrame of breach rows with columns:
            ``config_id, table_name, partition_date, dq_score,
              min_quality_pct, breach_type, metric_value, threshold,
              sla_owner, notified, breach_ts``
        """
        df_audit = self._latest_audit(partition_date)

        df_config = (
            self.spark.table(self.config_table)
            .select("config_id", "table_name", "min_quality_pct", "max_null_rate", "sla_owner")
            .filter(
                F.col("min_quality_pct").isNotNull() |
                F.col("max_null_rate").isNotNull()
            )
        )

        df_joined = df_audit.join(df_config, on=["config_id", "table_name"], how="inner")

        breach_cols = [
            "config_id", "table_name", "partition_date", "dq_score",
            "min_quality_pct", "breach_type", "metric_value", "threshold", "sla_owner",
        ]

        df_quality = (
            df_joined
            .filter(F.col("min_quality_pct").isNotNull())
            .filter(F.col("dq_score") < F.col("min_quality_pct"))
            .withColumn("breach_type",  F.lit("QUALITY_BELOW_THRESHOLD"))
            .withColumn("metric_value", F.col("dq_score"))
            .withColumn("threshold",    F.col("min_quality_pct"))
            .select(*breach_cols)
        )

        return (
            df_quality
            .withColumn("notified",  F.lit(False))
            .withColumn("breach_ts", F.current_timestamp())
        )

    def save_breaches(self, partition_date: str | None = None) -> bool:
        """
        Detects breaches and appends them to ``dq_sla_breach_audit``.

        Returns:
            True if write succeeded or no breaches found, False on write error.
        """
        df_breaches = self.check_breaches(partition_date=partition_date)
        count = df_breaches.count()
        if count == 0:
            logger.info("No SLA breaches detected.")
            print("[DQSLAChecker] No SLA breaches detected.")
            return True

        logger.warning(f"{count} SLA breach(es) detected.")
        print(f"[DQSLAChecker] {count} SLA breach(es) detected.")
        try:
            (
                df_breaches
                .write
                .partitionBy("partition_date")
                .mode("append")
                .saveAsTable(self.breach_table)
            )
            logger.info(f"Breaches written to '{self.breach_table}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to write SLA breaches: {e}")
            return False

    def get_breach_history(self, table_name: str | None = None) -> DataFrame:
        """Returns the full breach history, optionally filtered by table name."""
        df = self.spark.table(self.breach_table)
        if table_name:
            df = df.filter(F.lower(F.col("table_name")) == table_name.lower())
        return df.orderBy(F.col("breach_ts").desc())


__all__ = ["DQSLAChecker"]
