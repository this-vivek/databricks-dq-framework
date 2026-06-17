"""
dq_framework.metrics
~~~~~~~~~~~~~~~~~~~~~~
Pure Spark transform that aggregates DQX error output into the audit payload.

This is intentionally a free function (not a method): it depends only on its inputs,
which keeps the fragile coupling to the DQX ``_errors`` struct schema isolated in one
documented place and makes it independently testable.

.. note::
   Only the ``_errors`` column is aggregated — warning-level violations
   (``_warnings``) are deliberately *not* folded into the audit payload, so the
   downstream ``dq_simplified_vw`` / ``dq_column_vw`` schema stays stable. Change
   this only alongside those views.
"""

from __future__ import annotations

from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from ..exceptions import MetricComputationError


def compute_metric_stats(
    df_dq_output: DataFrame,
    json_output: bool = False,
    start_ts: str | None = None,
) -> DataFrame | str | None:
    """
    Aggregate DQ error metrics from a status DataFrame.

    Args:
        df_dq_output : DataFrame containing the DQX ``_errors`` column.
        json_output  : If True, return a JSON string; otherwise a Spark DataFrame.
        start_ts     : Run start timestamp (str). Defaults to ``now()``.

    Returns:
        A Spark DataFrame (``json_output=False``), a JSON string
        (``json_output=True``), or ``None`` if a JSON result has no rows.

    Raises:
        MetricComputationError: if the aggregation fails (e.g. the expected DQX
            ``_errors`` schema is absent).
    """
    start_ts = start_ts or str(datetime.now())
    try:
        df_output = (
            df_dq_output
            .select(F.explode_outer("_errors").alias("errors"))
            .filter("errors IS NOT NULL")
            .select("errors.*")
            .withColumn("columns", F.explode_outer("columns"))
            .withColumn(
                "rule_execution_time",
                (F.unix_timestamp("run_time") - F.unix_timestamp(F.lit(start_ts).cast("timestamp"))) / 60,
            )
            .groupBy("columns", "function")
            .agg(
                F.count("name").alias("bad_count"),
                F.sum("rule_execution_time").alias("total_execution_time"),
                F.first("name").alias("rule_name"),
                F.first("message").alias("message"),
                F.first("run_id").alias("run_id"),
            )
            .groupBy("function", "run_id")
            .agg(
                F.collect_set(
                    F.struct("columns", "bad_count", "total_execution_time", "rule_name", "message")
                ).alias("sub_payload")
            )
            .select(F.collect_set(F.struct("function", "run_id", "sub_payload")).alias("final_payload"))
            .withColumn("final_end_timestamp", F.current_timestamp())
        )

        if json_output:
            row = df_output.select(F.to_json("final_payload").alias("final_payload")).head()
            return row[0] if row else None
        return df_output
    except Exception as e:
        raise MetricComputationError(f"Failed to compute metric stats: {e}") from e


__all__ = ["compute_metric_stats"]
