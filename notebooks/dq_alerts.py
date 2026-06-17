# Databricks notebook source
# Tasks 2 and 3 of the DQ pipeline job (shared notebook, mode-switched via widget).
# mode=alerts → DQAlertSystem (metric/count/column drift)
# mode=sla    → DQSLAChecker  (SLA breach audit)

# COMMAND ----------
# MAGIC %md # DQ Framework — Alerts & SLA

# COMMAND ----------
# MAGIC %md ## 0 · Parameters

# COMMAND ----------
dbutils.widgets.text("catalog", "dev_catalog", "Unity Catalog")
dbutils.widgets.text("schema",  "dq",          "Schema")
dbutils.widgets.dropdown("mode", "alerts", ["alerts", "sla"], "Run mode")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
MODE    = dbutils.widgets.get("mode")

CATALOG_SCHEMA  = f"{CATALOG}.{SCHEMA}"
CONFIG_TABLE    = f"{CATALOG_SCHEMA}.dq_config"
AUDIT_TABLE     = f"{CATALOG_SCHEMA}.dq_audit"
BREACH_TABLE    = f"{CATALOG_SCHEMA}.dq_sla_breach_audit"

SIMPLIFIED_VW   = f"{CATALOG_SCHEMA}.dq_simplified_vw"
COLUMN_VW       = f"{CATALOG_SCHEMA}.dq_column_vw"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Mode    : {MODE}")

# COMMAND ----------
# MAGIC %md ## 1 · Alerts mode

# COMMAND ----------
if MODE == "alerts":
    from dq_framework.monitoring.alerts import DQAlertSystem

    alert_system = DQAlertSystem(
        spark         = spark,
        simplified_vw = SIMPLIFIED_VW,
        column_vw     = COLUMN_VW,
        alert_catalog = CATALOG_SCHEMA,
    )

    status = alert_system.save_alerts(drastic_only=False)
    print("Alert write status:")
    for check, written in status.items():
        print(f"  {check}: {'written' if written else 'skipped/empty'}")

# COMMAND ----------
# MAGIC %md ## 2 · SLA mode

# COMMAND ----------
if MODE == "sla":
    from dq_framework.monitoring.sla import DQSLAChecker

    sla_checker = DQSLAChecker(
        spark         = spark,
        audit_table   = AUDIT_TABLE,
        config_table  = CONFIG_TABLE,
        breach_table  = BREACH_TABLE,
    )

    breach_count = sla_checker.save_breaches()
    if breach_count:
        print(f"SLA breaches written to {BREACH_TABLE}")
    else:
        print("No SLA breaches detected or no thresholds configured")

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")
