"""
dq_framework.views
~~~~~~~~~~~~~~~~~~~
DDL helpers that create the standard DQ views on top of ``dq_audit``.

These are the Python port of the original Databricks notebook DDL. Call
:meth:`DQViews.create_all` during setup or whenever the audit table schema changes.
All statements are ``CREATE OR REPLACE VIEW`` — safe to re-run at any time.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# View DDL (parameterised by {catalog} and {schema})
# ------------------------------------------------------------------

_SIMPLIFIED_SQL = """
CREATE OR REPLACE VIEW {catalog}.{schema}.dq_simplified_vw AS
SELECT * EXCEPT (sub_payload), sub_payload.*
FROM (
  SELECT * EXCEPT (payload), payload.function,
         EXPLODE(payload.sub_payload) AS sub_payload
  FROM (
    SELECT config_id, table_name, table_count, table_cols,
           EXPLODE(final_payload) AS payload, audit_ts, partition_date
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY config_id, table_name, partition_date
               ORDER BY audit_ts DESC
             ) AS rn
      FROM {catalog}.{schema}.dq_audit
      WHERE active_flag = 1
    )
    WHERE rn = 1
  )
)
"""

_TABLE_STATS_SQL = """
CREATE OR REPLACE VIEW {catalog}.{schema}.dq_table_stats_vw AS
SELECT config_id, table_name, partition_date,
       SPLIT(table_cols, ',') AS table_cols, table_count
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY config_id, table_name
           ORDER BY audit_ts DESC
         ) AS rn
  FROM {catalog}.{schema}.dq_audit
  WHERE active_flag = 1
)
WHERE rn = 1
"""

_TABLE_VW_SQL = """
CREATE OR REPLACE VIEW {catalog}.{schema}.dq_table_vw AS
SELECT CONCAT(config_id, table_name, partition_date) AS dq_table_uid,
       config_id, table_name, partition_date,
       AVG(good_quality) AS good_quality
FROM (
  SELECT *,
         (((table_count - bad_count) / table_count) * 100) AS good_quality
  FROM {catalog}.{schema}.dq_simplified_vw
)
GROUP BY config_id, table_name, partition_date
"""

_COLUMN_VW_SQL = """
CREATE OR REPLACE VIEW {catalog}.{schema}.dq_column_vw AS
SELECT CONCAT(config_id, table_name, partition_date) AS dq_column_uid,
       config_id, table_name, columns, partition_date,
       AVG(good_quality) AS good_quality
FROM (
  SELECT *,
         (((table_count - bad_count) / table_count) * 100) AS good_quality
  FROM {catalog}.{schema}.dq_simplified_vw
)
GROUP BY config_id, table_name, columns, partition_date
"""

_FUNCTION_VW_SQL = """
CREATE OR REPLACE VIEW {catalog}.{schema}.dq_function_vw AS
SELECT CONCAT(config_id, table_name, partition_date) AS dq_function_uid,
       config_id, table_name, function, partition_date,
       AVG(good_quality) AS good_quality
FROM (
  SELECT *,
         (((table_count - bad_count) / table_count) * 100) AS good_quality
  FROM {catalog}.{schema}.dq_simplified_vw
)
GROUP BY config_id, table_name, function, partition_date
"""


class DQViews:
    """
    Creates and refreshes the five standard DQ metric views on top of ``dq_audit``.

    View dependency order (dq_simplified_vw must be created first):
        1. dq_simplified_vw    — flattened audit rows (base for all others)
        2. dq_table_stats_vw   — latest table-level stats
        3. dq_table_vw         — avg good_quality per table / partition_date
        4. dq_column_vw        — avg good_quality per column / partition_date
        5. dq_function_vw      — avg good_quality per DQ function / partition_date

    Args:
        spark   : Active SparkSession.
        catalog : Unity Catalog name.
        schema  : Schema/database name within the catalog.

    Example::

        DQViews(spark, "prod_catalog", "dq").create_all()
    """

    # Insertion order matters — simplified_vw must come before the three that depend on it.
    _VIEWS: dict[str, str] = {
        "dq_simplified_vw":  _SIMPLIFIED_SQL,
        "dq_table_stats_vw": _TABLE_STATS_SQL,
        "dq_table_vw":       _TABLE_VW_SQL,
        "dq_column_vw":      _COLUMN_VW_SQL,
        "dq_function_vw":    _FUNCTION_VW_SQL,
    }

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark   = spark
        self.catalog = catalog
        self.schema  = schema

    def _fmt(self, sql: str) -> str:
        return sql.format(catalog=self.catalog, schema=self.schema)

    def create(self, view_name: str) -> bool:
        """Creates (or replaces) a single named view. Returns True on success."""
        sql = self._VIEWS.get(view_name)
        if not sql:
            logger.error(f"Unknown view '{view_name}'. Available: {list(self._VIEWS)}.")
            return False
        try:
            self.spark.sql(self._fmt(sql))
            logger.info(f"View '{self.catalog}.{self.schema}.{view_name}' created/replaced.")
            return True
        except Exception as e:
            logger.exception(f"Failed to create view '{view_name}': {e}")
            return False

    def create_all(self) -> dict[str, bool]:
        """
        Creates all five standard DQ views in dependency order.

        Returns:
            ``{view_name: success}`` for each view.
        """
        results = {name: self.create(name) for name in self._VIEWS}
        ok = sum(v for v in results.values())
        logger.info(f"DQViews.create_all: {ok}/{len(results)} views created.")
        return results


__all__ = ["DQViews"]
