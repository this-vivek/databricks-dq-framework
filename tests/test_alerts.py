"""Tests for dq_framework.monitoring.alerts.DQAlertSystem (mocked, no Databricks)."""

from unittest.mock import MagicMock, patch

from dq_framework import DQAlertSystem


class TestDQAlertSystem:

    def test_resolve_table_with_catalog(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw", alert_catalog="cat.sc")
        assert a._resolve_table("dq_metric_drift_audit") == "cat.sc.dq_metric_drift_audit"

    def test_resolve_table_without_catalog(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw")
        assert a._resolve_table("any_table") is None

    def test_write_alert_returns_false_without_catalog(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw")
        df = MagicMock()
        assert a._write_alert(df, "dq_metric_drift_audit") is False
        df.withColumn.assert_not_called()

    def test_save_alerts_calls_all_three_checks(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw", alert_catalog="cat.sc")
        a.check_metric_drift = MagicMock(return_value=MagicMock())
        a.check_count_drift  = MagicMock(return_value=MagicMock())
        a.check_column_drift = MagicMock(return_value=MagicMock())
        a._write_alert       = MagicMock(return_value=True)

        a.save_alerts()
        a.check_metric_drift.assert_called_once()
        a.check_count_drift.assert_called_once()
        a.check_column_drift.assert_called_once()
        assert a._write_alert.call_count == 3

    def test_save_alerts_drastic_only_filters(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw", alert_catalog="cat.sc")
        df_metric = MagicMock()
        df_count  = MagicMock()
        a.check_metric_drift = MagicMock(return_value=df_metric)
        a.check_count_drift  = MagicMock(return_value=df_count)
        a.check_column_drift = MagicMock(return_value=MagicMock())
        a._write_alert       = MagicMock(return_value=True)

        with patch("dq_framework.monitoring.alerts.F") as mock_F:
            mock_F.col.return_value = "is_drastic"
            a.save_alerts(drastic_only=True)

        df_metric.filter.assert_called_once()
        df_count.filter.assert_called_once()

    def test_run_all_checks_returns_dict_no_write(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw")
        a.check_metric_drift = MagicMock(return_value="df_m")
        a.check_count_drift  = MagicMock(return_value="df_c")
        a.check_column_drift = MagicMock(return_value="df_col")
        a._write_alert       = MagicMock()

        result = a.run_all_checks()
        assert result == {"metric_drift": "df_m", "count_drift": "df_c", "column_drift": "df_col"}
        a._write_alert.assert_not_called()

    def test_save_alerts_returns_status_dict(self, mock_spark):
        a = DQAlertSystem(mock_spark, "s_vw", "c_vw", alert_catalog="cat.sc")
        a.check_metric_drift = MagicMock(return_value=MagicMock())
        a.check_count_drift  = MagicMock(return_value=MagicMock())
        a.check_column_drift = MagicMock(return_value=MagicMock())
        a._write_alert       = MagicMock(side_effect=[True, True, False])

        result = a.save_alerts()
        assert result == {"metric_drift": True, "count_drift": True, "column_drift": False}
