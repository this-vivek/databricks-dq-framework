"""
dq_framework.core
~~~~~~~~~~~~~~~~~~
Pipeline execution — config management, DQ runner, metrics, results, and scoring.
"""

from ._explain import suppress_explain
from .config import DQConfig
from .metrics import compute_metric_stats
from .results import DQBatchResult, summarize_batch
from .runner import DQRunner
from .scoring import compute_dq_score

__all__ = [
    "DQConfig",
    "DQRunner",
    "DQBatchResult",
    "summarize_batch",
    "compute_metric_stats",
    "compute_dq_score",
    "suppress_explain",
]
