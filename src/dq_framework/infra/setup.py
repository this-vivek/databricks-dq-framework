"""
dq_framework.setup
~~~~~~~~~~~~~~~~~~~
:class:`DQSetup` — one-call bootstrap that creates all required Delta tables and views.

Intended for first-time setup on a new catalog/schema. All DDL is idempotent
(``CREATE TABLE IF NOT EXISTS``, ``CREATE OR REPLACE VIEW``), so re-running is safe.

Example::

    setup = DQSetup(spark, catalog="prod_catalog", schema="dq")
    report = setup.bootstrap()
    # {'tables': {'dq_config': True, ...}, 'views': {'dq_simplified_vw': True, ...}, 'success': True}
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .views import DQViews

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Table DDL
# ------------------------------------------------------------------

_CONFIG_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dq_config (
    config_id       BIGINT  COMMENT 'Unique identifier for the config entry',
    table_name      STRING  COMMENT 'Fully qualified table name (catalog.schema.table)',
    business_rules  STRING  COMMENT 'Plain-language rules used for AI rule generation',
    dq_rule_payload STRING  COMMENT 'Generated JSON rule payload (populated after first run)',
    change_flag     BOOLEAN COMMENT 'Set TRUE to force rule regeneration on next run',
    min_quality_pct DOUBLE  COMMENT 'SLA: minimum acceptable DQ score (0-100). NULL = no SLA',
    max_null_rate   DOUBLE  COMMENT 'SLA: maximum acceptable null rate (0-100). NULL = no SLA',
    sla_owner       STRING  COMMENT 'Email or Slack handle for SLA breach notifications'
) USING DELTA
COMMENT 'DQ Framework configuration — one row per monitored table'
"""

_AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dq_audit (
    config_id       BIGINT,
    table_name      STRING,
    table_count     BIGINT,
    table_cols      STRING,
    final_payload   ARRAY<STRUCT<
                        function:    STRING,
                        run_id:      STRING,
                        sub_payload: ARRAY<STRUCT<
                            columns:              STRING,
                            bad_count:            BIGINT,
                            total_execution_time: DOUBLE,
                            rule_name:            STRING,
                            message:              STRING
                        >>
                    >>,
    start_ts        TIMESTAMP,
    end_ts          TIMESTAMP,
    total_time      DOUBLE,
    dq_score        DOUBLE    COMMENT 'Composite quality score 0-100',
    audit_ts        TIMESTAMP,
    partition_date  DATE,
    active_flag     INT,
    audit_update_ts TIMESTAMP
) USING DELTA
PARTITIONED BY (partition_date)
COMMENT 'DQ Framework audit log — one row per table per run'
"""

_RULE_HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dq_rule_history (
    history_id      BIGINT    GENERATED ALWAYS AS IDENTITY,
    config_id       BIGINT,
    table_name      STRING,
    version         INT,
    dq_rule_payload STRING,
    created_at      TIMESTAMP,
    created_by      STRING,
    is_current      BOOLEAN,
    notes           STRING
) USING DELTA
COMMENT 'DQ rule version history — tracks every payload change per table'
"""

_SLA_BREACH_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dq_sla_breach_audit (
    config_id       BIGINT,
    table_name      STRING,
    partition_date  DATE,
    dq_score        DOUBLE,
    min_quality_pct DOUBLE,
    breach_type     STRING    COMMENT 'QUALITY_BELOW_THRESHOLD or NULL_RATE_EXCEEDED',
    metric_value    DOUBLE,
    threshold       DOUBLE,
    sla_owner       STRING,
    notified        BOOLEAN,
    breach_ts       TIMESTAMP
) USING DELTA
PARTITIONED BY (partition_date)
COMMENT 'SLA breach log — populated by DQSLAChecker'
"""

_LINEAGE_IMPACT_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dq_lineage_impact_audit (
    failed_table       STRING    COMMENT 'Table where DQ failed',
    downstream_table   STRING    COMMENT 'Downstream dependent table',
    hop_depth          INT       COMMENT 'Number of hops from failed table (1 = direct)',
    failed_dq_score    STRING    COMMENT 'DQ score of the failed source table',
    impact_severity    STRING    COMMENT 'HIGH / MEDIUM / LOW based on hop depth and score',
    downstream_owner   STRING    COMMENT 'sla_owner of the downstream table from dq_config',
    partition_date     DATE      COMMENT 'Date of the impact report',
    created_at         TIMESTAMP COMMENT 'When this record was written'
) USING DELTA
PARTITIONED BY (partition_date)
COMMENT 'Lineage impact audit — downstream tables affected by DQ failures'
"""

_CORE_TABLES: dict[str, str] = {
    "dq_config":                _CONFIG_DDL,
    "dq_audit":                 _AUDIT_DDL,
    "dq_rule_history":          _RULE_HISTORY_DDL,
    "dq_sla_breach_audit":      _SLA_BREACH_DDL,
    "dq_lineage_impact_audit":  _LINEAGE_IMPACT_DDL,
}


class DQSetup:
    """
    One-call bootstrap for a new DQ Framework deployment.

    Creates all required Delta tables and views in the target catalog.schema.
    All DDL is idempotent — safe to re-run without dropping existing data.

    Args:
        spark   : Active SparkSession.
        catalog : Unity Catalog name (e.g. ``"my_catalog"``).
        schema  : Schema name (e.g. ``"dq"``).

    Example::

        setup = DQSetup(spark, catalog="prod_catalog", schema="dq")
        report = setup.bootstrap(create_example_config=True)
    """

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark   = spark
        self.catalog = catalog
        self.schema  = schema
        self._prefix = f"{catalog}.{schema}"

    def _fmt(self, sql: str) -> str:
        return sql.format(catalog=self.catalog, schema=self.schema)

    def _create_table(self, name: str, ddl: str) -> bool:
        try:
            self.spark.sql(self._fmt(ddl))
            logger.info(f"Table '{self._prefix}.{name}' created (if not exists).")
            return True
        except Exception as e:
            logger.exception(f"Failed to create table '{name}': {e}")
            return False

    def bootstrap(self, create_example_config: bool = False) -> dict:
        """
        Creates all core tables and all five standard views.

        Args:
            create_example_config : If True, inserts a placeholder row into dq_config.

        Returns:
            ``{"tables": {name: bool}, "views": {name: bool}, "success": bool}``
        """
        logger.info(f"DQSetup.bootstrap starting for '{self._prefix}'.")
        print(f"[DQSetup] Bootstrapping '{self._prefix}' ...")

        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self._prefix}")
            print(f"[DQSetup] Schema '{self._prefix}' ready.")
        except Exception as e:
            logger.warning(f"Could not create schema '{self._prefix}': {e}")

        table_results: dict[str, bool] = {}
        for name, ddl in _CORE_TABLES.items():
            ok = self._create_table(name, ddl)
            table_results[name] = ok
            print(f"[DQSetup]   {'✓' if ok else '✗'} {self._prefix}.{name}")

        vw_results = DQViews(self.spark, self.catalog, self.schema).create_all()
        for name, ok in vw_results.items():
            print(f"[DQSetup]   {'✓' if ok else '✗'} {self._prefix}.{name}")

        if create_example_config:
            self._insert_example_config()

        all_ok = all(table_results.values()) and all(vw_results.values())
        print(f"[DQSetup] Bootstrap {'complete' if all_ok else 'completed with errors'}.")
        logger.info(f"DQSetup.bootstrap {'complete' if all_ok else 'with errors'} for '{self._prefix}'.")
        return {"tables": table_results, "views": vw_results, "success": all_ok}

    def _insert_example_config(self) -> None:
        try:
            self.spark.sql(f"""
                INSERT INTO {self._prefix}.dq_config
                (config_id, table_name, business_rules, dq_rule_payload,
                 change_flag, min_quality_pct, max_null_rate, sla_owner)
                VALUES
                (1, 'your_catalog.your_schema.your_table',
                    'Replace with plain-language data quality rules for your table',
                    NULL, TRUE, 95.0, 5.0, NULL)
            """)
            print(f"[DQSetup]   ✓ Example config row inserted.")
        except Exception as e:
            logger.warning(f"Could not insert example config: {e}")

    def drop_all(self, confirm: bool = False) -> bool:
        """
        Drops all DQ tables and views (destructive — dev/test teardown only).
        Requires ``confirm=True`` to prevent accidental deletion.
        """
        if not confirm:
            print("[DQSetup] Pass confirm=True to drop all DQ objects.")
            return False
        objects = [
            f"VIEW IF EXISTS {self._prefix}.dq_function_vw",
            f"VIEW IF EXISTS {self._prefix}.dq_column_vw",
            f"VIEW IF EXISTS {self._prefix}.dq_table_vw",
            f"VIEW IF EXISTS {self._prefix}.dq_table_stats_vw",
            f"VIEW IF EXISTS {self._prefix}.dq_simplified_vw",
            f"TABLE IF EXISTS {self._prefix}.dq_sla_breach_audit",
            f"TABLE IF EXISTS {self._prefix}.dq_rule_history",
            f"TABLE IF EXISTS {self._prefix}.dq_audit",
            f"TABLE IF EXISTS {self._prefix}.dq_config",
        ]
        errors = []
        for obj in objects:
            try:
                self.spark.sql(f"DROP {obj}")
                logger.info(f"Dropped {obj}.")
            except Exception as e:
                logger.exception(f"Failed to drop {obj}: {e}")
                errors.append(obj)
        return len(errors) == 0


__all__ = ["DQSetup"]
