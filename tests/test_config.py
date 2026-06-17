"""Tests for dq_framework.core.config.DQConfig."""

import json
from unittest.mock import patch

from dq_framework import DQConfig


class TestDQConfig:

    def test_init_loads_config_table(self, mock_spark):
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        mock_spark.table.assert_called_once_with("cat.sc.dq_config")
        assert cfg.config_table == "cat.sc.dq_config"

    def test_reload_config_re_queries_spark(self, mock_spark):
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        cfg.reload_config()
        assert mock_spark.table.call_count == 2  # __init__ + reload_config

    def test_get_config_returns_none_when_missing(self, mock_spark, mock_df_chain):
        mock_df_chain(mock_spark, [])
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.F"):
            assert cfg.get_config_from_delta("nonexistent.table") is None

    def test_get_config_returns_none_when_ambiguous(self, mock_spark, mock_df_chain, sample_config_row):
        mock_df_chain(mock_spark, [sample_config_row, sample_config_row])
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.F"):
            assert cfg.get_config_from_delta("catalog.schema.my_table") is None

    def test_get_config_returns_dict_with_parsed_payload(self, mock_spark, mock_df_chain, sample_config_row):
        mock_df_chain(mock_spark, [sample_config_row])
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.F"):
            result = cfg.get_config_from_delta("catalog.schema.my_table")
        assert result is not None
        assert result["config_id"] == 1
        assert isinstance(result["dq_rule_payload"], dict)

    def test_validate_config_valid(self, mock_spark, mock_df_chain, sample_config_row):
        mock_df_chain(mock_spark, [sample_config_row])
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.F"):
            result = cfg.validate_config("catalog.schema.my_table")
        assert result["valid"] is True
        assert result["issues"] == []

    def test_validate_config_missing_payload(self, mock_spark, mock_df_chain):
        from unittest.mock import MagicMock
        row = MagicMock()
        row.asDict.return_value = {
            "config_id": 2, "table_name": "t", "business_rules": "check nulls",
            "dq_rule_payload": None, "change_flag": False,
        }
        mock_df_chain(mock_spark, [row])
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.F"):
            result = cfg.validate_config("t")
        assert result["valid"] is False
        assert any("dq_rule_payload" in i for i in result["issues"])

    def test_update_dq_rule_payload_rejects_invalid_json(self, mock_spark):
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.DeltaTable"):
            assert cfg.update_dq_rule_payload(1, "not valid json{{") is False

    def test_update_dq_rule_payload_accepts_valid_json(self, mock_spark):
        cfg = DQConfig(spark=mock_spark, config_table="cat.sc.dq_config")
        with patch("dq_framework.core.config.DeltaTable") as mock_dt, \
             patch("dq_framework.core.config.F"):
            mock_dt.forName.return_value.update.return_value = None
            assert cfg.update_dq_rule_payload(1, json.dumps({"checks": []})) is True
