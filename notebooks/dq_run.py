# Databricks notebook source
# Task 1 of the DQ pipeline job.
# Reads all active configs from dq_config, applies DQ rules to each table,
# writes results to dq_audit, and routes error rows to quarantine tables.

# COMMAND ----------
# MAGIC %md # DQ Framework — Run DQ Rules

# COMMAND ----------
# MAGIC %md ## 0 · Parameters (injected by DAB job or set manually)

# COMMAND ----------
dbutils.widgets.text("catalog", "dev_catalog", "Unity Catalog")
dbutils.widgets.text("schema",  "dq",          "Schema")

dbutils.library.restartPython()
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

CONFIG_TABLE    = f"{CATALOG}.{SCHEMA}.dq_config"
CATALOG_SCHEMA  = f"{CATALOG}.{SCHEMA}"

print(f"Catalog       : {CATALOG}")
print(f"Schema        : {SCHEMA}")
print(f"Config table  : {CONFIG_TABLE}")
print(f"Audit target  : {CATALOG_SCHEMA}.dq_audit")

# COMMAND ----------
# MAGIC %md ## 1 · Load active table names from config

# COMMAND ----------
df_config_raw = spark.table(CONFIG_TABLE)
active_tables = [r["table_name"] for r in df_config_raw.select("table_name").collect()]

if not active_tables:
    dbutils.notebook.exit("NO_ACTIVE_CONFIGS — nothing to process")

print(f"Active tables ({len(active_tables)}): {active_tables}")

# COMMAND ----------
# MAGIC %md ## 2 · Run DQ rules (batch)

# COMMAND ----------
from dq_framework.core.runner import DQRunner
from dq_framework.quality.quarantine import DQQuarantine

quarantine = DQQuarantine(spark, catalog_schema=CATALOG_SCHEMA)

runner = DQRunner(
    spark          = spark,
    config_table   = CONFIG_TABLE,
    table_name     = active_tables,
    catalog_schema = CATALOG_SCHEMA,
    quarantine     = quarantine,
)

results = runner.run_dq()

# COMMAND ----------
# MAGIC %md ## 3 · Batch summary

# COMMAND ----------
summary = runner.get_batch_summary(results)

print(f"Total   : {summary['total']}")
print(f"Success : {summary['succeeded']}")
print(f"Failed  : {summary['failed']}")
print(f"Duration: {summary['total_duration_s']:.1f}s")

if summary["failed"] > 0:
    print("\nFailed tables:")
    for r in results:
        if not r.success:
            print(f"  {r.table_name}: {r.error}")

# COMMAND ----------
# MAGIC %md ## 4 · Exit status for downstream tasks

# COMMAND ----------
exit_val = "FAILED" if summary["failed"] > 0 else "SUCCESS"
print(f"Exit: {exit_val}")
dbutils.notebook.exit(exit_val)
