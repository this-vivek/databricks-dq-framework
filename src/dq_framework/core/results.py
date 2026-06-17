"""
dq_framework.results
~~~~~~~~~~~~~~~~~~~~~~
Result containers and summaries for multi-table batch runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DQBatchResult:
    """Holds the outcome of a single table's DQ run in a multi-table execution."""

    table_name:  str
    success:     bool
    result:      object              = field(default=None)
    error:       Exception | None = field(default=None)
    duration_s:  float               = field(default=0.0)
    thread_name: str                 = field(default="")

    def to_dict(self) -> dict:
        """Serialisable summary — safe to log, display, or pass downstream."""
        return {
            "table_name":  self.table_name,
            "success":     self.success,
            "duration_s":  round(self.duration_s, 2),
            "thread_name": self.thread_name,
            "error":       str(self.error) if self.error else None,
        }


def summarize_batch(results: list[DQBatchResult]) -> dict:
    """Build a structured summary from a completed batch run."""
    return {
        "total":            len(results),
        "succeeded":        sum(1 for r in results if r.success),
        "failed":           sum(1 for r in results if not r.success),
        "total_duration_s": round(sum(r.duration_s for r in results), 2),
        "table_details":    [r.to_dict() for r in results],
    }


__all__ = ["DQBatchResult", "summarize_batch"]
