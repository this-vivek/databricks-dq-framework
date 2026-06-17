# Databricks notebook source
# One-time bootstrap driver — creates all DQ tables, views, and initial config.
# Run manually ONCE per environment before deploying the pipeline job.
# MAGIC %md
# MAGIC # DQ Framework — One-Time Bootstrap
# MAGIC Run this notebook **once per environment** to create all required Delta tables and views.
# MAGIC Parameters are passed as job parameters or set via the widgets below.

# COMMAND ----------
# MAGIC %md ## 0 · Install wheel

# COMMAND ----------
# MAGIC %pip install /Shared/dq-framework/${bundle.target}/artifacts/dq_framework-*.whl --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 1 · Parameters

# COMMAND ----------
dbutils.widgets.text("catalog", "dev_catalog", "Unity Catalog")
dbutils.widgets.text("schema",  "dq",          "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

print(f"Bootstrapping: {CATALOG}.{SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 2 · Bootstrap Delta tables

# COMMAND ----------
from dq_framework.infra.setup import DQSetup

setup = DQSetup(spark, catalog=CATALOG, schema=SCHEMA)
setup.bootstrap()

print("Tables created:")
print("  dq_config, dq_audit, dq_rule_history, dq_sla_breach_audit")

# COMMAND ----------
# MAGIC %md ## 3 · Create analytical views

# COMMAND ----------
from dq_framework.infra.views import DQViews

views = DQViews(spark, catalog=CATALOG, schema=SCHEMA)
views.create_all()

print("Views created:")
for v in ["dq_simplified_vw", "dq_column_vw", "dq_metric_drift_vw",
          "dq_count_drift_vw", "dq_column_drift_vw"]:
    print(f"  {CATALOG}.{SCHEMA}.{v}")

# COMMAND ----------
# MAGIC %md ## 4 · Verify

# COMMAND ----------
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").toPandas()
print(tables[["tableName", "isTemporary"]].to_string(index=False))
