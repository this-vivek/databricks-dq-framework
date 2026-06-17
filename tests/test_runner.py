"""Tests for dq_framework.core.runner.DQRunner (mocked, no Databricks)."""

from unittest.mock import MagicMock

import pytest

from dq_framework import DQBatchResult, DQRunner
from dq_framework.exceptions import DQDependencyError

# DQRunner.__init__ lazily imports databricks-labs-dqx; detect whether it's available
# so the dependency-error path is only asserted when the dep is genuinely absent.
try:
    import databricks.labs.dqx  # noqa: F401
    _DQX_INSTALLED = True
except Exception:
    _DQX_INSTALLED = False


def test_get_batch_summary_delegates():
    """get_batch_summary is pure Python — invoke it unbound with a mock self."""
    results = [
        DQBatchResult(table_name="t1", success=True,  duration_s=1.0),
        DQBatchResult(table_name="t2", success=False, duration_s=0.5, error=Exception("fail")),
    ]
    summary = DQRunner.get_batch_summary(MagicMock(), results)
    assert summary["total"]            == 2
    assert summary["succeeded"]        == 1
    assert summary["failed"]           == 1
    assert summary["total_duration_s"] == 1.5


@pytest.mark.skipif(_DQX_INSTALLED, reason="databricks-labs-dqx installed; dependency path not exercised")
def test_runner_raises_dependency_error_without_dqx(mock_spark):
    with pytest.raises(DQDependencyError):
        DQRunner(spark=mock_spark, config_table="cat.sc.dq_config", table_name="t")
