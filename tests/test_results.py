"""Tests for dq_framework.core.results (DQBatchResult, summarize_batch)."""

from dq_framework import DQBatchResult
from dq_framework.core.results import summarize_batch


class TestDQBatchResult:

    def test_to_dict_success(self):
        r = DQBatchResult(table_name="t", success=True, duration_s=1.234, thread_name="w0")
        d = r.to_dict()
        assert d["success"] is True
        assert d["duration_s"] == 1.23
        assert d["thread_name"] == "w0"
        assert d["error"] is None

    def test_to_dict_failure(self):
        err = ValueError("bad config")
        r = DQBatchResult(table_name="t", success=False, error=err, duration_s=0.5)
        d = r.to_dict()
        assert d["success"] is False
        assert "bad config" in d["error"]


class TestSummarizeBatch:

    def test_summarize_batch(self):
        results = [
            DQBatchResult(table_name="t1", success=True,  duration_s=1.0),
            DQBatchResult(table_name="t2", success=False, duration_s=0.5, error=Exception("fail")),
        ]
        summary = summarize_batch(results)
        assert summary["total"]            == 2
        assert summary["succeeded"]        == 1
        assert summary["failed"]           == 1
        assert summary["total_duration_s"] == 1.5
        assert len(summary["table_details"]) == 2

    def test_summarize_empty_batch(self):
        summary = summarize_batch([])
        assert summary["total"]            == 0
        assert summary["succeeded"]        == 0
        assert summary["failed"]           == 0
        assert summary["total_duration_s"] == 0.0
        assert summary["table_details"]    == []
