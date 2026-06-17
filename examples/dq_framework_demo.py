# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # DQ Framework — Setup & Demo Notebook
# MAGIC
# MAGIC This notebook walks through the complete DQ Framework lifecycle on Databricks:
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |------|-------------|
# MAGIC | **1. Install** | Wheel uploaded to a Volume; `%pip install` on the cluster |
# MAGIC | **2. Bootstrap** | `DQSetup.bootstrap()` creates all tables + views once |
# MAGIC | **3. Config** | Insert rows into `dq_config` (one per monitored table) |
# MAGIC | **4. Run** | `DQRunner.run_dq()` applies rules, writes `dq_audit`, computes DQ score |
# MAGIC | **5. Inspect** | Browse audit results, valid/invalid split, quarantine |
# MAGIC | **6. Rules** | Use `RuleTemplate` to build rules without AI |
# MAGIC | **7. Alerts** | `DQAlertSystem.save_alerts()` detects drift, notifies via `DQNotifier` |
# MAGIC | **8. SLA** | `DQSLAChecker` flags tables below quality threshold |
# MAGIC | **9. Schedule** | `DQScheduler` creates a Workflows job for daily automation |
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Databricks Runtime 13.3 LTS or 14.x
# MAGIC - Unity Catalog enabled
# MAGIC - Foundation Model API access (for AI rule generation)

# COMMAND ----------
# MAGIC %md ## 1 · Install dq-framework

# COMMAND ----------

# ── Option A: install from a UC Volume (recommended for production) ──────────
# Upload dist/dq_framework-1.1.0-py3-none-any.whl to a Volume first, then:
#
# %pip install /Volumes/<catalog>/<schema>/<volume>/dq_framework-1.1.0-py3-none-any.whl[llm,notifications]

# ── Option B: install from local path (dev/test) ─────────────────────────────
# %pip install /Workspace/Users/you@org.com/dq-framework/dist/dq_framework-1.1.0-py3-none-any.whl[llm]

# ── After installing, restart Python to activate the new library ─────────────
# dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ## 2 · Imports & cluster-level Spark session

# COMMAND ----------

# Standard library
import json
import logging

# Databricks / Spark (provided by the runtime — do NOT pip-install these)
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# dq_framework top-level public API
from dq_framework import (
    # Infrastructure
    DQSetup,
    DQViews,
    DQScheduler,
    # Core pipeline
    DQConfig,
    DQRunner,
    DQBatchResult,
    compute_dq_score,
    # Rules
    RuleTemplate,
    RuleRegistry,
    # Quality
    DQQuarantine,
    # Monitoring
    DQAlertSystem,
    DQNotifier,
    DQSLAChecker,
    # Exceptions
    DQError,
    ConfigNotFoundError,
    RuleGenerationError,
)

# Optional: fine-grained subpackage imports (same objects, different path)
# from dq_framework.core   import DQConfig, DQRunner
# from dq_framework.quality import RuleTemplate, DQQuarantine
# from dq_framework.monitoring import DQAlertSystem, DQNotifier, DQSLAChecker
# from dq_framework.infra  import DQSetup, DQViews, DQScheduler

logging.basicConfig(level=logging.INFO)
print(f"dq_framework imported. SparkSession: {spark}")

# COMMAND ----------
# MAGIC %md ## 3 · Configuration — set your catalog, schema, and table

# COMMAND ----------

# ── EDIT THESE VALUES ────────────────────────────────────────────────────────
CATALOG      = "my_catalog"          # Unity Catalog name
SCHEMA       = "dq"                  # Schema for all DQ tables / views
SOURCE_TABLE = f"{CATALOG}.sales.orders"   # The table you want to monitor

# Derived fully-qualified names (do not edit)
DQ_PREFIX        = f"{CATALOG}.{SCHEMA}"
CONFIG_TABLE     = f"{DQ_PREFIX}.dq_config"
AUDIT_TABLE      = f"{DQ_PREFIX}.dq_audit"
RULE_HIST_TABLE  = f"{DQ_PREFIX}.dq_rule_history"
SLA_BREACH_TABLE = f"{DQ_PREFIX}.dq_sla_breach_audit"

# Views (created by DQSetup.bootstrap)
SIMPLIFIED_VW    = f"{DQ_PREFIX}.dq_simplified_vw"
COLUMN_VW        = f"{DQ_PREFIX}.dq_column_vw"

# Slack webhook for notifications (optional — leave None to skip)
SLACK_WEBHOOK    = None   # "https://hooks.slack.com/services/..."

print(f"Target catalog.schema : {DQ_PREFIX}")
print(f"Source table          : {SOURCE_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4 · One-time bootstrap — creates all tables and views

# COMMAND ----------

# DQSetup.bootstrap() is idempotent — safe to re-run at any time.
# It creates: dq_config, dq_audit, dq_rule_history, dq_sla_breach_audit
# and the 5 views: dq_simplified_vw, dq_table_vw, dq_column_vw, dq_function_vw, dq_table_stats_vw

setup  = DQSetup(spark, catalog=CATALOG, schema=SCHEMA)
report = setup.bootstrap(create_example_config=False)

print("\nBootstrap report:")
print(json.dumps(report, indent=2))

assert report["success"], "Bootstrap failed — check logs above before continuing."

# COMMAND ----------
# MAGIC %md ## 5 · Insert DQ config for your table

# COMMAND ----------
# MAGIC %md
# MAGIC Each row in `dq_config` tells the framework:
# MAGIC - **what table** to check
# MAGIC - **business_rules** — plain English used to generate AI rules
# MAGIC - **min_quality_pct** — optional SLA (breach if score drops below this)
# MAGIC - **sla_owner** — who to notify on breach

# COMMAND ----------

# Run once per table. Skip if the row already exists.
spark.sql(f"""
    INSERT INTO {CONFIG_TABLE}
        (config_id, table_name, business_rules, dq_rule_payload,
         change_flag, min_quality_pct, max_null_rate, sla_owner)
    VALUES
        (1, '{SOURCE_TABLE}',
            'order_id must not be null and must be unique.
             order_date must be between 2020-01-01 and today.
             amount must be positive.
             customer_id must reference customers table.',
            NULL, TRUE,
            95.0, 5.0, NULL)
""")

print(f"Config inserted for {SOURCE_TABLE}")

# COMMAND ----------
# MAGIC %md ## 6 · Pre-flight config validation (no DQ run yet)

# COMMAND ----------

cfg    = DQConfig(spark, config_table=CONFIG_TABLE)
result = cfg.validate_config(SOURCE_TABLE)

print(json.dumps(result, indent=2))

# COMMAND ----------
# MAGIC %md ## 7 · Run DQ — single table with AI rule generation

# COMMAND ----------
# MAGIC %md
# MAGIC First run: `change_flag=TRUE` and `dq_rule_payload IS NULL` → rules are generated via AI,
# MAGIC stored back in `dq_config`, and applied. Subsequent runs use the stored payload.

# COMMAND ----------

runner = DQRunner(
    spark               = spark,
    config_table        = CONFIG_TABLE,
    table_name          = SOURCE_TABLE,
    catalog_schema      = DQ_PREFIX,
    collect_table_stats = True,
    max_retries         = 2,
)

try:
    df_result = runner.run_dq(debug_flag=False)   # debug_flag=True skips audit write
    display(df_result)
except ConfigNotFoundError as e:
    print(f"Config missing: {e}")
except RuleGenerationError as e:
    print(f"AI rule generation failed: {e}")
except DQError as e:
    print(f"DQ run failed ({type(e).__name__}): {e}")

# COMMAND ----------
# MAGIC %md ## 8 · Inspect audit results

# COMMAND ----------

# Latest audit row — flattened via the dq_simplified_vw view
display(spark.table(SIMPLIFIED_VW).filter(
    F.col("table_name") == SOURCE_TABLE
).orderBy(F.col("partition_date").desc()))

# COMMAND ----------

# DQ score per table per day (from dq_table_vw)
display(spark.sql(f"""
    SELECT table_name, partition_date,
           ROUND(good_quality, 2) AS dq_score
    FROM {DQ_PREFIX}.dq_table_vw
    WHERE table_name = '{SOURCE_TABLE}'
    ORDER BY partition_date DESC
"""))

# COMMAND ----------
# MAGIC %md ## 9 · Force-regenerate rules (skip AI — use RuleTemplate instead)

# COMMAND ----------
# MAGIC %md
# MAGIC Use `RuleTemplate` when you want deterministic, version-controlled rules
# MAGIC without calling the Foundation Model API on every run.

# COMMAND ----------

manual_rules = (
    RuleTemplate.not_null(["order_id", "order_date", "amount", "customer_id"])
    + RuleTemplate.unique(["order_id"])
    + RuleTemplate.positive_value(["amount"])
    + RuleTemplate.date_range("order_date", "2020-01-01", "2030-12-31")
)

print(f"Built {len(manual_rules)} rules manually:")
for r in manual_rules:
    print(f"  [{r['function']}] {r['name']}")

# Save to rule history for rollback capability
registry = RuleRegistry(spark, history_table=RULE_HIST_TABLE, created_by="demo_notebook")
version  = registry.save_version(
    config_id  = 1,
    table_name = SOURCE_TABLE,
    payload    = manual_rules,
    notes      = "Manually authored rules v1",
)
print(f"\nSaved as rule version {version}")

# Now run DQ with the manual rules (force_regenerate=False since payload is now set)
cfg.update_dq_rule_payload(1, json.dumps(manual_rules))

runner2 = DQRunner(
    spark          = spark,
    config_table   = CONFIG_TABLE,
    table_name     = SOURCE_TABLE,
    catalog_schema = DQ_PREFIX,
)
df_result2 = runner2.run_dq(force_regenerate=False)
display(df_result2)

# COMMAND ----------
# MAGIC %md ## 10 · Quarantine error rows

# COMMAND ----------
# MAGIC %md
# MAGIC `DQQuarantine` routes rows with DQ errors to a separate Delta table
# MAGIC so the clean dataset can flow downstream without tainted rows.

# COMMAND ----------

# Get the full DF (valid + invalid rows) for the table
runner3 = DQRunner(
    spark          = spark,
    config_table   = CONFIG_TABLE,
    table_name     = SOURCE_TABLE,
    catalog_schema = DQ_PREFIX,
)
df_full = runner3.get_valid_invalid_df(SOURCE_TABLE)

quarantine = DQQuarantine(spark, catalog_schema=DQ_PREFIX)
clean_df, qok = quarantine.route(df_full, source_table=SOURCE_TABLE)

print(f"\nQuarantine write: {'OK' if qok else 'FAILED'}")
print(f"Clean rows  : {clean_df.count():,}")

# Inspect quarantined rows
quarantine_tbl = f"{DQ_PREFIX}.{SOURCE_TABLE.split('.')[-1]}_quarantine"
try:
    display(spark.table(quarantine_tbl).select("_source_table", "_quarantine_ts", "_errors"))
except Exception:
    print("No quarantine table yet (no error rows found).")

# COMMAND ----------
# MAGIC %md ## 11 · Multi-table batch run

# COMMAND ----------

TABLE_LIST = [
    f"{CATALOG}.sales.orders",
    f"{CATALOG}.sales.customers",
    f"{CATALOG}.sales.products",
]

batch_runner = DQRunner(
    spark          = spark,
    config_table   = CONFIG_TABLE,
    table_name     = TABLE_LIST,
    catalog_schema = DQ_PREFIX,
    max_workers    = 4,
    max_retries    = 2,
)

batch_results = batch_runner.run_dq()   # returns List[DQBatchResult]
summary       = batch_runner.get_batch_summary(batch_results)

print("\nBatch summary:")
print(json.dumps({k: v for k, v in summary.items() if k != "table_details"}, indent=2))

for r in batch_results:
    status = "✓" if r.success else f"✗ {type(r.error).__name__}"
    print(f"  {r.table_name:<50} {status}  ({r.duration_s:.1f}s)")

# COMMAND ----------
# MAGIC %md ## 12 · Drift alerts

# COMMAND ----------
# MAGIC %md
# MAGIC `DQAlertSystem` reads from the two metric views and flags:
# MAGIC - **Metric drift** — quality score dropped/rose beyond threshold
# MAGIC - **Count drift** — row count changed drastically
# MAGIC - **Column drift** — columns added or removed between runs

# COMMAND ----------

notifier = DQNotifier(
    slack_webhook = SLACK_WEBHOOK,          # None = skip Slack
    # teams_webhook = "https://...",        # uncomment for Teams
    # email_config  = {                     # uncomment for email
    #     "smtp_host": "smtp.gmail.com",
    #     "sender": "dq@org.com",
    #     "password": "...",
    #     "recipients": ["team@org.com"],
    # },
)

alerts = DQAlertSystem(
    spark          = spark,
    simplified_vw  = SIMPLIFIED_VW,
    column_vw      = COLUMN_VW,
    alert_catalog  = DQ_PREFIX,
    notifier       = notifier,             # auto-dispatches after save_alerts()
)

# Inline inspection (no write)
checks = alerts.run_all_checks(metric_threshold_pct=10.0, count_threshold_pct=20.0)
display(checks["metric_drift"].filter(F.col("is_drastic")))

# COMMAND ----------

# Write alerts to Delta + dispatch notifications
alert_results = alerts.save_alerts(
    metric_threshold_pct = 10.0,
    count_threshold_pct  = 20.0,
    drastic_only         = False,     # True = only write is_drastic=True rows
)
print("Alert write results:", alert_results)

# COMMAND ----------
# MAGIC %md ## 13 · SLA breach detection

# COMMAND ----------
# MAGIC %md
# MAGIC `DQSLAChecker` joins the audit table with `min_quality_pct` from `dq_config`.
# MAGIC Any table scoring below its threshold generates a breach record.

# COMMAND ----------

sla = DQSLAChecker(
    spark        = spark,
    audit_table  = AUDIT_TABLE,
    config_table = CONFIG_TABLE,
    breach_table = SLA_BREACH_TABLE,
)

df_breaches = sla.check_breaches()
display(df_breaches)

# Persist breaches
sla.save_breaches()

# History
display(sla.get_breach_history(table_name=SOURCE_TABLE))

# COMMAND ----------
# MAGIC %md ## 14 · Rule version history & rollback

# COMMAND ----------

history = registry.get_history(SOURCE_TABLE)
print(f"Rule versions for {SOURCE_TABLE}:")
for v in history:
    print(f"  v{v['version']}  is_current={v['is_current']}  notes={v['notes']}")

# Roll back to version 1 if needed
# registry.rollback(config_id=1, version=1)

# COMMAND ----------
# MAGIC %md ## 15 · Schedule daily DQ runs via Databricks Workflows

# COMMAND ----------
# MAGIC %md
# MAGIC `DQScheduler` creates a Workflows job that runs this notebook on a cron schedule.
# MAGIC The job runs on an existing cluster (cheaper) or a new job cluster.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

ws        = WorkspaceClient()
scheduler = DQScheduler(ws)

# List existing DQ jobs
existing = scheduler.list_jobs(prefix="dq_")
print("Existing DQ jobs:", existing)

# Create a new job (uncomment to run)
# job_id = scheduler.create_job(
#     job_name            = "dq_daily_orders",
#     notebook_path       = "/Workspace/Users/you@org.com/dq_framework_demo",
#     cron_expression     = "0 0 6 * * ?",       # 6 AM UTC daily
#     existing_cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId"),
#     notebook_params     = {"SOURCE_TABLE": SOURCE_TABLE},
# )
# print(f"Job created: {job_id}")

# Trigger an immediate run of an existing job
# run_id = scheduler.trigger_run(job_id=job_id)
# print(f"Run started: run_id={run_id}")

# COMMAND ----------
# MAGIC %md ## 16 · Rebuild & distribute the wheel

# COMMAND ----------
# MAGIC %md
# MAGIC After editing the source code locally, rebuild and re-upload the wheel:
# MAGIC
# MAGIC ```bash
# MAGIC # On your local machine (needs Python 3.10–3.12 for pyspark wheels)
# MAGIC pip install build
# MAGIC python -m build      # → dist/dq_framework-1.1.0-py3-none-any.whl
# MAGIC
# MAGIC # Upload to UC Volume
# MAGIC databricks fs cp dist/dq_framework-1.1.0-py3-none-any.whl \
# MAGIC     dbfs:/Volumes/<catalog>/<schema>/<volume>/dq_framework-1.1.0-py3-none-any.whl
# MAGIC
# MAGIC # Then re-install on the cluster (run in a notebook cell):
# MAGIC # %pip install /Volumes/.../dq_framework-1.1.0-py3-none-any.whl[llm,notifications] --force-reinstall
# MAGIC # dbutils.library.restartPython()
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | What you have now | |
# MAGIC |---|---|
# MAGIC | `DQSetup.bootstrap()` | One-time DDL for all 4 tables + 5 views |
# MAGIC | `DQRunner.run_dq()` | Runs rules, writes audit, computes `dq_score` |
# MAGIC | `RuleTemplate` | Hand-build rules without AI |
# MAGIC | `RuleRegistry` | Version + rollback rules |
# MAGIC | `DQQuarantine` | Isolate error rows for downstream safety |
# MAGIC | `DQAlertSystem` | Detect metric/count/column drift |
# MAGIC | `DQNotifier` | Push alerts to Slack / Teams / email |
# MAGIC | `DQSLAChecker` | Enforce per-table quality thresholds |
# MAGIC | `DQScheduler` | Automate via Databricks Workflows |
# MAGIC | `dq-framework` CLI | Control-plane commands from terminal |
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Add more tables to `dq_config` (one row per table)
# MAGIC - Set `min_quality_pct` / `sla_owner` for SLA monitoring
# MAGIC - Configure `SLACK_WEBHOOK` for real-time alerts
# MAGIC - Schedule daily runs via `DQScheduler.create_job()`
