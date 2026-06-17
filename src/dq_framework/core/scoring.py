"""
dq_framework.scoring
~~~~~~~~~~~~~~~~~~~~~~
Computes a composite DQ quality score (0–100) from a status DataFrame.

Score definition:
    score = (rows with no errors / total rows) * 100

100 = every row passed all rules.
0   = every row has at least one error.
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def compute_dq_score(df_status: DataFrame, table_count: int | None = None) -> float:
    """
    Computes a composite 0–100 DQ quality score.

    Args:
        df_status   : Status DataFrame with at least an ``_errors`` column —
                      the output of ``apply_dq_rules(dq_flag=True)``.
        table_count : Total row count of the source table. If ``None``, derived
                      from ``df_status`` at the cost of one extra Spark action.

    Returns:
        Float score in [0.0, 100.0].  Returns 100.0 when ``table_count`` is 0.
    """
    try:
        total = table_count if table_count is not None else df_status.count()
        if not total:
            return 100.0
        error_rows = (
            df_status
            .filter(
                F.col("_errors").isNotNull() &
                (F.size(F.col("_errors")) > 0)
            )
            .count()
        )
        score = max(0.0, ((total - error_rows) / total) * 100.0)
        logger.debug(f"DQ score: {score:.2f} ({total - error_rows}/{total} error-free rows).")
        return round(score, 2)
    except Exception as e:
        logger.exception(f"Failed to compute DQ score: {e}")
        return 0.0


__all__ = ["compute_dq_score"]
