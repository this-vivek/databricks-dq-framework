# Databricks notebook source
# Task 1.5 of the DQ pipeline job.
# Reads batch results from dq_audit, identifies failed tables,
# traverses downstream lineage via system.access.table_lineage,
# and writes an impact report to dq_lineage_impact_audit.

# COMMAND ----------
# MAGIC %md # DQ Framework — Lineage Impact

# COMMAND ----------
# MAGIC %md ## 0 · Parameters

# COMMAND ----------
dbutils.widgets.text("catalog", "dev_catalog", "Unity Catalog")
dbutils.widgets.text("schema",  "dq",          "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")

CATALOG_SCHEMA = f"{CATALOG}.{SCHEMA}"
CONFIG_TABLE   = f"{CATALOG_SCHEMA}.dq_config"
AUDIT_TABLE    = f"{CATALOG_SCHEMA}.dq_audit"
IMPACT_TABLE   = f"{CATALOG_SCHEMA}.dq_lineage_impact_audit"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Impact  : {IMPACT_TABLE}")

# COMMAND ----------
# MAGIC %md ## 1 · Find tables that failed DQ in today's run

# COMMAND ----------
from pyspark.sql import functions as F

df_today = (
    spark.table(AUDIT_TABLE)
    .filter(F.col("partition_date") == F.current_date())
    .select("table_name", "dq_score")
)

# A table "failed" if its dq_score is below 100 or below its SLA threshold.
# Here we flag anything below 100 as a candidate for lineage impact.
df_config = spark.table(CONFIG_TABLE).select("table_name", "min_quality_pct")

df_failed = (
    df_today
    .join(df_config, on="table_name", how="left")
    .filter(
        (F.col("dq_score") < 100) |
        (F.col("min_quality_pct").isNotNull() & (F.col("dq_score") < F.col("min_quality_pct")))
    )
    .select("table_name", "dq_score")
)

failed_list = [(r["table_name"], r["dq_score"]) for r in df_failed.collect()]
print(f"Tables with DQ issues today: {len(failed_list)}")
for t, s in failed_list:
    print(f"  {t} — score: {s}")

# COMMAND ----------
# MAGIC %md ## 2 · Run lineage impact analysis

# COMMAND ----------
from dq_framework.monitoring.lineage import DQLineage

lineage = DQLineage(
    spark        = spark,
    impact_table = IMPACT_TABLE,
    config_table = CONFIG_TABLE,
    max_depth    = 3,
)

if failed_list:
    df_impact = lineage.build_impact_report(failed_list)
    impact_count = df_impact.count()
    print(f"Downstream tables impacted: {impact_count}")
    if impact_count > 0:
        df_impact.show(truncate=False)
        lineage.save_impact(df_impact)
        print(f"Impact report written to {IMPACT_TABLE}")
else:
    print("No DQ failures today — lineage impact skipped.")
    impact_count = 0

# COMMAND ----------
# MAGIC %md ## 3 · Summary

# COMMAND ----------
print(f"Failed tables  : {len(failed_list)}")
print(f"Impacted tables: {impact_count}")
dbutils.notebook.exit("SUCCESS")
