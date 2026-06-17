"""
dq_framework.rules
~~~~~~~~~~~~~~~~~~~~
Pre-built DQ rule templates + :class:`RuleRegistry` for versioned rule management.

Templates generate dqx-compatible rule dicts (the format expected by
``DQEngine.apply_checks_by_metadata``).  Chain them with ``+``::

    rules = (
        RuleTemplate.not_null(["patient_id", "admit_date"])
        + RuleTemplate.date_range("admit_date", "2000-01-01", "2030-12-31")
        + RuleTemplate.email_format(["contact_email"])
    )

:class:`RuleRegistry` stores versioned payloads in ``dq_rule_history`` and supports
rollback to any prior version.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Rule templates
# ------------------------------------------------------------------

class RuleTemplate:
    """
    Factory methods for dqx-compatible rule dicts.

    Each method returns ``list[dict]``.  Lists can be combined with ``+`` or via
    :meth:`combine`.
    """

    @staticmethod
    def not_null(columns: list[str]) -> list[dict]:
        """Flags rows where any of the given columns is NULL."""
        return [
            {"name": f"{col}_not_null", "function": "is_not_null",
             "arguments": {"columns": [col]}}
            for col in columns
        ]

    @staticmethod
    def not_empty(columns: list[str]) -> list[dict]:
        """Flags rows where any of the given columns is NULL or an empty string."""
        return [
            {"name": f"{col}_not_empty", "function": "is_not_null_and_not_empty",
             "arguments": {"columns": [col]}}
            for col in columns
        ]

    @staticmethod
    def date_range(column: str, min_date: str, max_date: str) -> list[dict]:
        """Flags rows where ``column`` falls outside [min_date, max_date]."""
        return [{
            "name":      f"{column}_date_range",
            "function":  "is_in_range",
            "arguments": {"column": column, "min_value": min_date, "max_value": max_date},
        }]

    @staticmethod
    def numeric_range(column: str, min_val: float, max_val: float) -> list[dict]:
        """Flags rows where a numeric column falls outside [min_val, max_val]."""
        return [{
            "name":      f"{column}_range",
            "function":  "is_in_range",
            "arguments": {"column": column, "min_value": min_val, "max_value": max_val},
        }]

    @staticmethod
    def positive_value(columns: list[str]) -> list[dict]:
        """Flags rows where any of the given numeric columns is <= 0."""
        return [
            {"name": f"{col}_positive", "function": "is_positive",
             "arguments": {"columns": [col]}}
            for col in columns
        ]

    @staticmethod
    def value_in_set(column: str, allowed: list) -> list[dict]:
        """Flags rows where ``column`` is not in ``allowed``."""
        return [{
            "name":      f"{column}_in_set",
            "function":  "value_is_in_list",
            "arguments": {"column": column, "allowed_values": [str(v) for v in allowed]},
        }]

    @staticmethod
    def regex_match(column: str, pattern: str, name: str | None = None) -> list[dict]:
        """Flags rows where ``column`` does not match ``pattern``."""
        return [{
            "name":      name or f"{column}_regex",
            "function":  "regex_match",
            "arguments": {"column": column, "regex": pattern},
        }]

    @staticmethod
    def email_format(columns: list[str]) -> list[dict]:
        """Flags rows where any of the given columns is not a valid email address."""
        pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
        return [
            {"name": f"{col}_email_format", "function": "regex_match",
             "arguments": {"column": col, "regex": pattern}}
            for col in columns
        ]

    @staticmethod
    def unique(columns: list[str]) -> list[dict]:
        """Flags duplicate values in any of the given columns."""
        return [
            {"name": f"{col}_unique", "function": "is_unique",
             "arguments": {"columns": [col]}}
            for col in columns
        ]

    @staticmethod
    def referential_integrity(
        column:        str,
        lookup_table:  str,
        lookup_column: str,
    ) -> list[dict]:
        """Flags rows where ``column`` has a value absent from ``lookup_table.lookup_column``."""
        return [{
            "name":      f"{column}_ref_integrity",
            "function":  "is_in_table",
            "arguments": {
                "column":     column,
                "ref_table":  lookup_table,
                "ref_column": lookup_column,
            },
        }]

    @staticmethod
    def combine(*rule_lists: list[dict]) -> list[dict]:
        """Merges multiple rule lists into one."""
        result: list[dict] = []
        for lst in rule_lists:
            result.extend(lst)
        return result


# ------------------------------------------------------------------
# Rule registry
# ------------------------------------------------------------------

class RuleRegistry:
    """
    Versioned storage for DQ rule payloads in ``dq_rule_history`` Delta table.

    Each :meth:`save_version` call creates a new immutable record and marks
    the previous version as non-current. :meth:`rollback` restores any prior version.

    Args:
        spark         : Active SparkSession.
        history_table : Fully qualified ``dq_rule_history`` table name.
        created_by    : Identity stamped on every version record
                        (e.g. notebook path or user email).

    Example::

        registry = RuleRegistry(spark, "prod.dq.dq_rule_history", "pipeline/run_dq")
        rules    = RuleTemplate.not_null(["id", "date"]) + RuleTemplate.positive_value(["amount"])
        version  = registry.save_version(config_id=1, table_name="prod.sales.orders",
                                         payload=rules, notes="Initial ruleset")
    """

    def __init__(
        self,
        spark:         SparkSession,
        history_table: str,
        created_by:    str = "dq_framework",
    ):
        self.spark         = spark
        self.history_table = history_table
        self.created_by    = created_by

    def save_version(
        self,
        config_id:  int,
        table_name: str,
        payload:    list[dict] | str,
        notes:      str = "",
    ) -> int:
        """
        Saves a new rule version and marks it as current.

        Args:
            config_id  : config_id from dq_config.
            table_name : Fully qualified table name.
            payload    : Rule list or JSON string.
            notes      : Optional description of the change.

        Returns:
            New version number (integer).
        """
        payload_str = json.dumps(payload) if isinstance(payload, list) else payload

        existing = (
            self.spark.table(self.history_table)
            .filter(F.col("config_id") == F.lit(config_id))
            .agg(F.max("version").alias("max_version"))
            .collect()
        )
        next_version = ((existing[0]["max_version"] or 0) + 1) if existing else 1

        DeltaTable.forName(self.spark, self.history_table).update(
            condition=F.col("config_id") == F.lit(config_id),
            set={"is_current": F.lit(False)},
        )

        new_row = self.spark.createDataFrame([{
            "config_id":       config_id,
            "table_name":      table_name,
            "version":         next_version,
            "dq_rule_payload": payload_str,
            "created_at":      datetime.now().isoformat(),
            "created_by":      self.created_by,
            "is_current":      True,
            "notes":           notes,
        }])
        new_row.write.format("delta").mode("append").saveAsTable(self.history_table)
        logger.info(f"Rule version {next_version} saved for config_id={config_id}.")
        return next_version

    def get_current(self, table_name: str) -> dict | None:
        """Returns the current rule version record for a table, or None."""
        rows = (
            self.spark.table(self.history_table)
            .filter(
                (F.lower(F.col("table_name")) == table_name.lower()) &
                F.col("is_current")
            )
            .orderBy(F.col("version").desc())
            .limit(1)
            .collect()
        )
        return rows[0].asDict() if rows else None

    def get_history(self, table_name: str) -> list[dict]:
        """Returns all rule versions for a table, newest first."""
        return [
            row.asDict()
            for row in (
                self.spark.table(self.history_table)
                .filter(F.lower(F.col("table_name")) == table_name.lower())
                .orderBy(F.col("version").desc())
                .collect()
            )
        ]

    def rollback(self, config_id: int, version: int) -> bool:
        """
        Rolls back to a specific version by marking it as current.

        Args:
            config_id : config_id from dq_config.
            version   : Target version number.

        Returns:
            True on success, False if the version was not found.
        """
        target = (
            self.spark.table(self.history_table)
            .filter(
                (F.col("config_id") == F.lit(config_id)) &
                (F.col("version") == F.lit(version))
            )
            .limit(1)
            .collect()
        )
        if not target:
            logger.error(f"Version {version} not found for config_id={config_id}.")
            return False

        dt = DeltaTable.forName(self.spark, self.history_table)
        dt.update(
            condition=F.col("config_id") == F.lit(config_id),
            set={"is_current": F.lit(False)},
        )
        dt.update(
            condition=(
                (F.col("config_id") == F.lit(config_id)) &
                (F.col("version") == F.lit(version))
            ),
            set={"is_current": F.lit(True)},
        )
        logger.info(f"Rolled back to version {version} for config_id={config_id}.")
        return True


__all__ = ["RuleTemplate", "RuleRegistry"]
