"""
dq_framework.alerts
~~~~~~~~~~~~~~~~~~~~
:class:`DQAlertSystem` — drift detection for metric quality, row count, and schema.

Concerns resolved vs the notebook version:
  - ``spark`` is injected via ``__init__``, never a global.
  - All three checks return pure Spark DataFrames (no side effects).
  - :meth:`DQAlertSystem.save_alerts` is the only write path.
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class DQAlertSystem:
    """
    Monitors DQ audit output and raises alerts for:
        1. Drastic drop/rise in good_quality % per column  (check_metric_drift)
        2. Drastic change in table row count               (check_count_drift)
        3. Column additions or removals between runs       (check_column_drift)

    Args:
        spark          : Active SparkSession (required — never uses a global).
        simplified_vw  : Fully qualified name of dq_simplified_vw.
        column_vw      : Fully qualified name of dq_column_vw.
        alert_catalog  : catalog.schema prefix for alert audit tables.
                         Required when save_alerts() is called.

    Alert audit tables written by save_alerts():
        <alert_catalog>.dq_metric_drift_audit
        <alert_catalog>.dq_count_drift_audit
        <alert_catalog>.dq_column_drift_audit
    """

    _METRIC_DRIFT_TBL = "dq_metric_drift_audit"
    _COUNT_DRIFT_TBL  = "dq_count_drift_audit"
    _COLUMN_DRIFT_TBL = "dq_column_drift_audit"

    def __init__(
        self,
        spark:         SparkSession,
        simplified_vw: str,
        column_vw:     str,
        alert_catalog: str | None = None,
        notifier=None,
    ):
        self.spark         = spark
        self.simplified_vw = simplified_vw
        self.column_vw     = column_vw
        self.alert_catalog = alert_catalog
        self.notifier      = notifier  # optional DQNotifier
        logger.info(
            f"DQ_AlertSystem initialised | simplified_vw='{simplified_vw}' "
            f"| column_vw='{column_vw}' | alert_catalog='{alert_catalog}'."
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_table(self, table_name: str) -> str | None:
        """Prepends alert_catalog to a short table name. Returns None if not set."""
        if not self.alert_catalog:
            logger.warning(f"alert_catalog not set — skipping write for '{table_name}'.")
            return None
        return f"{self.alert_catalog}.{table_name}"

    def _write_alert(self, df, table_name: str) -> bool:
        """
        Appends an alert DataFrame to its Delta audit table, partitioned by alert_date.
        Returns True on success, False on failure.
        """
        fqn = self._resolve_table(table_name)
        if not fqn:
            return False
        try:
            (
                df
                .withColumn("alert_ts",   F.current_timestamp())
                .withColumn("alert_date", F.current_date())
                .write
                .partitionBy("alert_date")
                .mode("append")
                .saveAsTable(fqn)
            )
            logger.info(f"Alert audit written to '{fqn}'.")
            print(f"[DQAlertSystem] Alert audit written to '{fqn}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to write alert audit to '{fqn}': {e}")
            return False

    # ------------------------------------------------------------------
    # 1. Metric quality drift
    # ------------------------------------------------------------------

    def check_metric_drift(self, threshold_pct: float = 10.0):
        """
        Compares good_quality % per column between the latest and previous DQ run.
        Flags columns where quality has changed beyond threshold_pct.

        Returns Spark DataFrame with columns:
            config_id, table_name, columns,
            previous_date, current_date,
            previous_quality, current_quality,
            quality_drift_pct, drift_direction,
            is_drastic, severity
        """
        window_spec = (
            Window.partitionBy("config_id", "table_name", "columns")
                  .orderBy(F.col("partition_date").desc())
        )
        df_ranked = (
            self.spark.table(self.column_vw)
            .withColumn("run_rank", F.rank().over(window_spec))
            .filter(F.col("run_rank") <= 2)
        )
        df_current = (
            df_ranked.filter(F.col("run_rank") == 1)
            .select(
                "config_id", "table_name", "columns",
                F.col("good_quality").alias("current_quality"),
                F.col("partition_date").alias("current_date"),
            )
        )
        df_previous = (
            df_ranked.filter(F.col("run_rank") == 2)
            .select(
                "config_id", "table_name", "columns",
                F.col("good_quality").alias("previous_quality"),
                F.col("partition_date").alias("previous_date"),
            )
        )
        return (
            df_current
            .join(df_previous, on=["config_id", "table_name", "columns"], how="left")
            .withColumn("quality_drift_pct",
                F.round(F.col("current_quality") - F.col("previous_quality"), 2))
            .withColumn("drift_direction",
                F.when(F.col("quality_drift_pct") < 0, "DEGRADED")
                 .when(F.col("quality_drift_pct") > 0, "IMPROVED")
                 .otherwise("STABLE"))
            .withColumn("is_drastic",
                F.abs(F.col("quality_drift_pct")) >= F.lit(threshold_pct))
            .withColumn("severity",
                F.when(F.abs(F.col("quality_drift_pct")) >= 30, "CRITICAL")
                 .when(F.abs(F.col("quality_drift_pct")) >= 20, "HIGH")
                 .when(F.abs(F.col("quality_drift_pct")) >= threshold_pct, "MEDIUM")
                 .otherwise("LOW"))
            .select(
                "config_id", "table_name", "columns",
                "previous_date", "current_date",
                "previous_quality", "current_quality",
                "quality_drift_pct", "drift_direction",
                "is_drastic", "severity",
            )
            .orderBy(F.col("is_drastic").desc(), F.col("quality_drift_pct").asc())
        )

    # ------------------------------------------------------------------
    # 2. Row count drift
    # ------------------------------------------------------------------

    def check_count_drift(self, threshold_pct: float = 20.0):
        """
        Compares table_count between the latest and previous DQ run.
        Flags tables where row count changed beyond threshold_pct.

        Returns Spark DataFrame with columns:
            config_id, table_name,
            previous_date, current_date,
            previous_count, current_count,
            count_change_pct, change_direction,
            is_drastic, severity
        """
        window_spec = (
            Window.partitionBy("config_id", "table_name")
                  .orderBy(F.col("partition_date").desc())
        )
        df_ranked = (
            self.spark.table(self.simplified_vw)
            .select("config_id", "table_name", "table_count", "partition_date")
            .distinct()
            .withColumn("run_rank", F.rank().over(window_spec))
            .filter(F.col("run_rank") <= 2)
        )
        df_current = (
            df_ranked.filter(F.col("run_rank") == 1)
            .select(
                "config_id", "table_name",
                F.col("table_count").alias("current_count"),
                F.col("partition_date").alias("current_date"),
            )
        )
        df_previous = (
            df_ranked.filter(F.col("run_rank") == 2)
            .select(
                "config_id", "table_name",
                F.col("table_count").alias("previous_count"),
                F.col("partition_date").alias("previous_date"),
            )
        )
        return (
            df_current
            .join(df_previous, on=["config_id", "table_name"], how="left")
            .withColumn("count_change_pct",
                F.round(
                    ((F.col("current_count") - F.col("previous_count")) / F.col("previous_count")) * 100,
                    2,
                ))
            .withColumn("change_direction",
                F.when(F.col("count_change_pct") < 0, "DECREASED")
                 .when(F.col("count_change_pct") > 0, "INCREASED")
                 .otherwise("STABLE"))
            .withColumn("is_drastic",
                F.abs(F.col("count_change_pct")) >= F.lit(threshold_pct))
            .withColumn("severity",
                F.when(F.abs(F.col("count_change_pct")) >= 50, "CRITICAL")
                 .when(F.abs(F.col("count_change_pct")) >= 30, "HIGH")
                 .when(F.abs(F.col("count_change_pct")) >= threshold_pct, "MEDIUM")
                 .otherwise("LOW"))
            .select(
                "config_id", "table_name",
                "previous_date", "current_date",
                "previous_count", "current_count",
                "count_change_pct", "change_direction",
                "is_drastic", "severity",
            )
            .orderBy(F.col("is_drastic").desc(), F.col("count_change_pct").asc())
        )

    # ------------------------------------------------------------------
    # 3. Column schema drift
    # ------------------------------------------------------------------

    def check_column_drift(self):
        """
        Compares the set of columns between the latest and previous DQ run.
        Flags columns added or removed.

        Returns Spark DataFrame with columns:
            config_id, table_name,
            previous_date, current_date,
            column_name, change_type  (ADDED | REMOVED)
        """
        window_spec = (
            Window.partitionBy("config_id", "table_name")
                  .orderBy(F.col("partition_date").desc())
        )
        df_ranked = (
            self.spark.table(self.simplified_vw)
            .select("config_id", "table_name", "table_cols", "partition_date")
            .distinct()
            .withColumn("run_rank", F.rank().over(window_spec))
            .filter(F.col("run_rank") <= 2)
        )
        df_current = (
            df_ranked.filter(F.col("run_rank") == 1)
            .withColumn("column_name", F.explode(F.split(F.col("table_cols"), ",")))
            .withColumn("column_name", F.trim(F.col("column_name")))
            .select("config_id", "table_name",
                    F.col("partition_date").alias("current_date"), "column_name")
        )
        df_previous = (
            df_ranked.filter(F.col("run_rank") == 2)
            .withColumn("column_name", F.explode(F.split(F.col("table_cols"), ",")))
            .withColumn("column_name", F.trim(F.col("column_name")))
            .select("config_id", "table_name",
                    F.col("partition_date").alias("previous_date"), "column_name")
        )
        df_added = (
            df_current
            .join(df_previous, on=["config_id", "table_name", "column_name"], how="left_anti")
            .join(
                df_previous.select("config_id", "table_name", "previous_date").distinct(),
                on=["config_id", "table_name"], how="left",
            )
            .withColumn("change_type", F.lit("ADDED"))
        )
        df_removed = (
            df_previous
            .join(df_current, on=["config_id", "table_name", "column_name"], how="left_anti")
            .join(
                df_current.select("config_id", "table_name", "current_date").distinct(),
                on=["config_id", "table_name"], how="left",
            )
            .withColumn("change_type", F.lit("REMOVED"))
        )
        return (
            df_added.unionByName(df_removed)
            .select("config_id", "table_name",
                    "previous_date", "current_date",
                    "column_name", "change_type")
            .orderBy("table_name", "change_type", "column_name")
        )

    # ------------------------------------------------------------------
    # 4. Save all alerts to Delta
    # ------------------------------------------------------------------

    def save_alerts(
        self,
        metric_threshold_pct: float = 10.0,
        count_threshold_pct:  float = 20.0,
        drastic_only:         bool  = False,
    ) -> dict:
        """
        Runs all three checks and appends results to Delta audit tables.

        Each table is partitioned by alert_date and accumulates history across
        runs, giving a full timeline of alert trends.

        Args:
            metric_threshold_pct : Quality drift threshold. Default 10.0.
            count_threshold_pct  : Count drift threshold.   Default 20.0.
            drastic_only         : If True, only write rows where is_drastic=True.

        Returns:
            dict of { check_name: True|False } indicating write success per check.
        """
        df_metric = self.check_metric_drift(threshold_pct=metric_threshold_pct)
        df_count  = self.check_count_drift(threshold_pct=count_threshold_pct)
        df_column = self.check_column_drift()

        if drastic_only:
            df_metric = df_metric.filter(F.col("is_drastic"))
            df_count  = df_count.filter(F.col("is_drastic"))

        results = {
            "metric_drift": self._write_alert(df_metric, self._METRIC_DRIFT_TBL),
            "count_drift":  self._write_alert(df_count,  self._COUNT_DRIFT_TBL),
            "column_drift": self._write_alert(df_column, self._COLUMN_DRIFT_TBL),
        }

        if self.notifier is not None:
            self.notifier.send_alert_summary(results)

        return results

    # ------------------------------------------------------------------
    # 5. Inline inspection (no Delta write)
    # ------------------------------------------------------------------

    def run_all_checks(
        self,
        metric_threshold_pct: float = 10.0,
        count_threshold_pct:  float = 20.0,
    ) -> dict:
        """
        Runs all three checks and returns a dict of DataFrames.
        Does NOT write to Delta — use save_alerts() to persist.
        """
        return {
            "metric_drift": self.check_metric_drift(threshold_pct=metric_threshold_pct),
            "count_drift":  self.check_count_drift(threshold_pct=count_threshold_pct),
            "column_drift": self.check_column_drift(),
        }


__all__ = ["DQAlertSystem"]
