"""
dq_framework.config
~~~~~~~~~~~~~~~~~~~~~
:class:`DQConfig` — manages and validates the DQ configuration Delta table.

The query helpers here return ``Optional`` / ``bool``: a missing config row is a
*normal* result, not an error, so callers such as :meth:`DQConfig.validate_config`
can reason about it directly. Fatal, abort-the-run decisions are made one level up
in :class:`dq_framework.runner.DQRunner`, which raises typed
:mod:`dq_framework.exceptions` instead.
"""

from __future__ import annotations

import json
import logging

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class DQConfig:
    """
    Manages the DQ configuration Delta table.

    Args:
        spark        : Active SparkSession. Must be passed explicitly —
                       the package never relies on a global ``spark`` variable.
        config_table : Fully qualified Delta config table name.

    Expected columns: config_id, table_name, business_rules, dq_rule_payload, change_flag.
    """

    def __init__(self, spark: SparkSession, config_table: str):
        self.spark        = spark
        self.config_table = config_table
        self.df_config    = spark.table(config_table)
        logger.info(f"DQConfig initialised | config_table='{config_table}'.")

    def reload_config(self) -> None:
        """Refreshes the in-memory config snapshot from the Delta table."""
        self.df_config = self.spark.table(self.config_table)
        logger.info(f"Config snapshot reloaded from '{self.config_table}'.")

    def update_dq_rule_payload(self, config_id: int, payload: str) -> bool:
        """
        Validates the JSON payload and persists it to the config Delta table
        via DeltaTable API (avoids SQL injection from single quotes in payload).
        Resets change_flag to False on success. Returns True/False.
        """
        logger.info(f"Updating DQ rule payload for config_id={config_id}.")
        try:
            try:
                json.loads(payload)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON payload for config_id={config_id}: {e}")
                return False

            DeltaTable.forName(self.spark, self.config_table).update(
                condition=F.col("config_id") == F.lit(config_id),
                set={
                    "dq_rule_payload": F.lit(payload),
                    "change_flag":     F.lit(False),
                },
            )
            self.reload_config()
            logger.info(f"Payload updated successfully for config_id={config_id}.")
            return True
        except Exception as e:
            logger.exception(f"Failed to update payload for config_id={config_id}: {e}")
            return False

    def set_change_flag(self, table_name: str, flag: bool = True) -> bool:
        """
        Manually sets change_flag for a table. Use flag=True to force rule
        regeneration on the next run, or flag=False to mark as stable.
        """
        logger.info(f"Setting change_flag={flag} for table='{table_name}'.")
        try:
            DeltaTable.forName(self.spark, self.config_table).update(
                condition=F.lower(F.col("table_name")) == F.lit(table_name.lower()),
                set={"change_flag": F.lit(flag)},
            )
            self.reload_config()
            logger.info(f"change_flag={flag} set for table='{table_name}'.")
            return True
        except Exception as e:
            logger.exception(f"Failed to set change_flag for table='{table_name}': {e}")
            return False

    def get_config_from_delta(self, table_name: str) -> dict | None:
        """
        Fetches the single DQ config record for the given table.

        Uses ``.limit(2).collect()`` — one Spark job instead of ``.count()`` + ``.head()``.
        Returns dict, or None if config is missing or ambiguous.
        """
        logger.info(f"Fetching config for table='{table_name}'.")
        try:
            # SLA columns may not exist in older config tables — coalesce to None if absent.
            sla_cols = [
                c for c in ("min_quality_pct", "max_null_rate", "sla_owner")
                if c in self.df_config.columns
            ]
            select_cols = ["config_id", "table_name", "business_rules", "dq_rule_payload", "change_flag"] + sla_cols
            rows = (
                self.df_config
                .filter(F.lower(F.col("table_name")) == table_name.lower())
                .select(*select_cols)
                .limit(2)
                .collect()
            )
            if len(rows) == 0:
                logger.error(f"No config found for table='{table_name}'.")
                return None
            if len(rows) > 1:
                logger.error(f"Multiple config entries found for table='{table_name}'.")
                return None

            result = dict(rows[0].asDict())
            result["dq_rule_payload"] = (
                json.loads(result["dq_rule_payload"]) if result.get("dq_rule_payload") else None
            )
            return result
        except Exception as e:
            logger.exception(f"Failed to fetch config for table='{table_name}': {e}")
            return None

    def get_dq_rule_payload(self, table_name: str) -> dict | None:
        """Returns the parsed dq_rule_payload for a table, or None."""
        config = self.get_config_from_delta(table_name)
        return config.get("dq_rule_payload") if config else None

    def validate_config(self, table_name: str) -> dict[str, object]:
        """
        Pre-flight check on the config entry for a table without running DQ.
        Returns dict with keys: valid, config_id, has_payload, change_flag, issues.
        """
        issues = []
        config = self.get_config_from_delta(table_name)

        if not config:
            return {
                "valid": False, "config_id": None, "has_payload": False,
                "change_flag": None, "issues": [f"No config entry found for '{table_name}'"],
            }

        if not config.get("business_rules"):
            issues.append("business_rules is empty — AI rule generation will fail.")
        if not config.get("dq_rule_payload"):
            issues.append("dq_rule_payload is missing — rules will be regenerated on next run.")
        if config.get("change_flag"):
            issues.append("change_flag=True — rules will be regenerated on next run.")

        return {
            "valid":       len(issues) == 0,
            "config_id":   config.get("config_id"),
            "has_payload": bool(config.get("dq_rule_payload")),
            "change_flag": config.get("change_flag"),
            "issues":      issues,
        }


__all__ = ["DQConfig"]
