# DQ Framework — Complete Documentation

**Version:** 1.1.0
**Platform:** Databricks (Unity Catalog)
**Author:** Vivek Rawat

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [Prerequisites](#3-prerequisites)
4. [Project Structure](#4-project-structure)
5. [Setup Guide](#5-setup-guide)
6. [Configuration Reference](#6-configuration-reference)
7. [Running the Pipeline](#7-running-the-pipeline)
8. [Monitoring & Alerts](#8-monitoring--alerts)
9. [SLA Enforcement & Notifications](#9-sla-enforcement--notifications)
10. [Quarantine](#10-quarantine)
11. [Rule Registry](#11-rule-registry)
12. [CLI Reference](#12-cli-reference)
13. [CI/CD — Azure DevOps](#13-cicd--azure-devops)
14. [Troubleshooting](#14-troubleshooting)
15. [Versioning & Upgrade Guide](#15-versioning--upgrade-guide)

---

## 1. Executive Summary

### What it is

DQ Framework is a **production-ready, config-driven Data Quality platform** built for Databricks. It wraps `databricks-labs-dqx` and adds a complete operational layer on top — from rule management and audit trails to drift alerting, SLA enforcement, quarantine, and CI/CD deployment.

### Problem it solves

Data engineering teams typically build DQ checks per pipeline in an ad-hoc way. As the number of tables grows, this creates:
- No central visibility into which tables have DQ checks and when they last ran
- No consistent audit trail to prove quality over time
- No alerting when quality degrades or row counts shift unexpectedly
- No enforcement of SLA thresholds agreed with data consumers

DQ Framework solves all of this from a single config table.

### Who it is for

| Persona | How they use it |
|---|---|
| **Data Engineer** | Registers tables, sets business rules, deploys the pipeline via DAB |
| **Data Analyst / Consumer** | Queries `dq_simplified_vw` and `dq_column_vw` to assess table quality |
| **Data Owner / Manager** | Sets SLA thresholds; receives Slack/email breach notifications |
| **Platform Team** | Manages deployment via Azure DevOps; controls prod promotion |

### Key capabilities at a glance

- AI-generated DQ rules from plain-language business descriptions
- Parallel multi-table runs with per-table error capture
- DQ score (`0–100`) written to every audit row
- Drift detection: metric quality, row count, schema changes
- Bad rows routed automatically to quarantine Delta tables
- SLA thresholds per table; breach audit with notifications
- Full DAB deployment — one command deploys wheel + notebooks + job
- Azure DevOps CI/CD with approval gate before production

---

## 2. Architecture Overview

### Data flow

```
┌─────────────────────────────────────────────────────────────────┐
│  dq_config (Delta)                                              │
│  table_name | business_rules | dq_rule_payload | change_flag    │
│             | min_quality_pct | max_null_rate  | sla_owner      │
└──────────────────────────┬──────────────────────────────────────┘
                           │ DQConfig.get_config_from_delta()
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  DQRunner                                                        │
│                                                                  │
│  1. Load config for each table                                   │
│  2. Generate DQ rules (AI-assisted via dqx, if change_flag=True) │
│  3. Apply rules to source table                                  │
│  4. Compute DQ score = (total - error_rows) / total × 100       │
│  5. Write metrics → dq_audit                                     │
│  6. Route error rows → <table>_quarantine  (if enabled)          │
└──────────────────────────┬───────────────────────────────────────┘
                           │ dq_audit (Delta, partitioned by date)
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Analytical Views (auto-created)                                 │
│  dq_simplified_vw   — flattened per-column quality per run       │
│  dq_column_vw       — column quality % trend over time           │
│  dq_metric_drift_vw — quality % change between runs              │
│  dq_count_drift_vw  — row count change between runs              │
│  dq_column_drift_vw — schema additions / removals                │
└──────────────────────────┬───────────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
     DQAlertSystem   DQSLAChecker    DQNotifier
     (drift audit)   (breach audit)  (Slack/Teams/Email)
```

### Deployment flow

```
Developer pushes to main branch
          │
          ▼
  Azure DevOps CI
  ├── ruff lint
  ├── pytest (mocked)
  └── bundle validate
          │  (on merge)
          ▼
  bundle deploy --target dev
          │  (manual approval)
          ▼
  bundle deploy --target prod
          │
          ▼
  Databricks Workflow (scheduled daily 06:00 UTC)
  ├── Task 1: dq_run      (apply rules, write audit + quarantine)
  ├── Task 2: dq_alerts   (drift detection)
  └── Task 3: dq_sla      (SLA check + notifications)
```

### Component map

```
src/dq_framework/
│
├── core/           ← Pipeline engine
│   ├── config.py      DQConfig       — reads/updates dq_config Delta table
│   ├── runner.py      DQRunner       — orchestrates the full DQ pipeline
│   ├── results.py     DQBatchResult  — per-table run result dataclass
│   ├── metrics.py     compute_metric_stats — Spark aggregation (pure)
│   ├── scoring.py     compute_dq_score     — DQ score calculation
│   └── _explain.py    suppress_explain     — thread-safe stdout suppressor
│
├── quality/        ← Data quality controls
│   ├── rules.py       RuleTemplate   — pre-built rule factory
│   │                  RuleRegistry   — versioned rule storage + rollback
│   └── quarantine.py  DQQuarantine   — routes bad rows to quarantine tables
│
├── monitoring/     ← Observability
│   ├── alerts.py      DQAlertSystem  — drift detection + alert writes
│   ├── sla.py         DQSLAChecker   — threshold evaluation + breach audit
│   └── notifications.py DQNotifier  — Slack / Teams / SMTP dispatch
│
└── infra/          ← Setup & lifecycle
    ├── setup.py       DQSetup        — bootstrap DDL for all Delta tables
    ├── views.py       DQViews        — creates the 5 analytical views
    └── scheduling.py  DQScheduler    — Databricks SDK job CRUD (dev helper)
```

---

## 3. Prerequisites

### Databricks workspace
- Databricks Runtime **13.3 LTS** or later (tested on 15.4 LTS)
- **Unity Catalog** enabled
- A catalog and schema where DQ objects will be created
- Workspace has internet access (for AI rule generation)

### Python environment (local / CI)
- Python **3.10+**
- Databricks CLI **>= 0.221** (`pip install databricks-cli`)
- `python -m build` available (`pip install build`)

### Python dependencies (installed as part of the wheel)

| Package | Version | Purpose |
|---|---|---|
| `pyspark` | >=3.5 | Spark DataFrame operations |
| `delta-spark` | >=3.0 | Delta table reads/writes |
| `databricks-labs-dqx` | ==0.13.0 | DQ rule engine and AI generator |
| `databricks-sdk` | >=0.20 | WorkspaceClient, job CRUD |

Optional extras:

| Extra | Packages | Install with |
|---|---|---|
| `[llm]` | `databricks-labs-dqx[llm]` | AI rule generation |
| `[notifications]` | `requests>=2.28` | Slack / Teams webhooks |
| `[cli]` | `typer>=0.12` | `dq-framework` CLI |
| `[all]` | All of the above | Recommended for production |
| `[dev]` | `pytest`, `ruff`, `build` | Local development |

### Azure DevOps (for CI/CD)
- ADO project with a pipeline connected to the repository
- Two environments configured: `dq-framework-dev`, `dq-framework-prod`
- Manual approval check on the prod environment
- Variable group `dq-framework` with the tokens and host URLs (see [Section 13](#13-cicd--azure-devops))

---

## 4. Project Structure

```
dq-framework/
│
├── databricks.yml              # DAB bundle: targets (dev/staging/prod), variables, artifact
├── azure-pipelines.yml         # Azure DevOps 3-stage pipeline
├── Makefile                    # make test | lint | build | deploy-dev | deploy-prod
├── pyproject.toml              # Package metadata, dependencies, entry points
├── README.md                   # Quick-reference
├── DOCUMENTATION.md            # This file
├── LICENSE                     # MIT
├── .gitignore
│
├── resources/
│   └── jobs/
│       └── dq_pipeline_job.yml # Databricks Workflow: 3 sequential tasks
│
├── notebooks/
│   ├── dq_setup.py             # One-time bootstrap driver
│   ├── dq_run.py               # Task 1 driver — run DQ rules
│   └── dq_alerts.py            # Task 2 (alerts) + Task 3 (SLA) shared driver
│
├── src/
│   └── dq_framework/           # Installable Python package
│       └── ...                 # (see component map in Section 2)
│
├── examples/
│   └── dq_framework_demo.py    # 16-cell Databricks notebook — full walkthrough
│
└── tests/
    ├── conftest.py             # Shared mocks: mock_spark, sample_config_row
    ├── test_config.py
    ├── test_runner.py
    ├── test_results.py
    └── test_alerts.py
```

---

## 5. Setup Guide

### Step 1 — Clone and install locally (for development)

```bash
git clone https://github.com/your-org/dq-framework.git
cd dq-framework
pip install -e ".[all,dev]"
```

### Step 2 — Configure workspace targets

Edit `databricks.yml` and fill in the workspace URLs for each environment:

```yaml
targets:
  dev:
    workspace:
      host: https://<your-dev-workspace>.azuredatabricks.net
      profile: <your-databricks-cli-profile>
    variables:
      catalog: <your_dev_catalog>
      schema: dq

  prod:
    workspace:
      host: https://<your-prod-workspace>.azuredatabricks.net
    variables:
      catalog: <your_prod_catalog>
      schema: dq
```

Also update `notification_email` in the variables section.

### Step 3 — Build and deploy the bundle

```bash
# Validate YAML and auth
databricks bundle validate --target dev

# Build wheel + upload notebooks + create job
databricks bundle deploy --target dev
```

### Step 4 — Bootstrap Delta tables (run once per environment)

Open `notebooks/dq_setup.py` in the Databricks workspace and run it with:
- `catalog` widget = your catalog name
- `schema` widget = `dq` (or your chosen schema)

This creates:
- `dq_config`
- `dq_audit`
- `dq_rule_history`
- `dq_sla_breach_audit`
- All 5 analytical views

> Run this notebook manually once. It is idempotent (`CREATE TABLE IF NOT EXISTS`).

### Step 5 — Insert your first config entry

```sql
INSERT INTO <catalog>.dq.dq_config
  (table_name, business_rules, dq_rule_payload, change_flag,
   min_quality_pct, max_null_rate, sla_owner)
VALUES
  ('catalog.bronze.orders',
   'order_id must not be null. order_date must be a valid date. amount must be positive.',
   NULL,      -- leave NULL; runner generates and caches the payload on first run
   TRUE,      -- TRUE forces rule (re)generation on next run
   95.0,      -- SLA: at least 95% quality required
   0.05,      -- SLA: at most 5% nulls allowed
   'data-team@company.com');
```

### Step 6 — Validate the config entry

```python
from dq_framework import DQConfig

cfg = DQConfig(spark, config_table="catalog.dq.dq_config")
result = cfg.validate_config("catalog.bronze.orders")
print(result)
# {'valid': True, 'config_id': 1, 'has_payload': False, 'change_flag': True, 'issues': [...]}
```

### Step 7 — Run the pipeline

**Via DAB (recommended for production):**
```bash
databricks bundle run dq_pipeline --target dev
```

**Via Databricks UI:**
Workflows → `[dev] dq-framework-pipeline` → Run now

**Via notebook directly:**
Open `notebooks/dq_run.py` and run all cells with the catalog/schema widgets set.

---

## 6. Configuration Reference

### `dq_config` table — column definitions

| Column | Type | Required | Description |
|---|---|---|---|
| `config_id` | BIGINT (IDENTITY) | Auto | Auto-incrementing primary key |
| `table_name` | STRING | Yes | Fully qualified table name: `catalog.schema.table` |
| `business_rules` | STRING | Yes | Plain-language rules for AI rule generation. Be specific — each rule on a new sentence. Example: `"order_id must not be null. amount must be greater than 0."` |
| `dq_rule_payload` | STRING (JSON) | No | Cached JSON rule payload generated by dqx. Leave `NULL` on first insert; the runner generates and stores it automatically. Set to `NULL` and `change_flag=TRUE` to force regeneration. |
| `change_flag` | BOOLEAN | Yes | `TRUE` = regenerate rules on next run. Automatically reset to `FALSE` after successful rule generation. |
| `min_quality_pct` | DOUBLE | No | SLA threshold: minimum acceptable DQ score (0–100). If the run score falls below this, a breach is recorded. |
| `max_null_rate` | DOUBLE | No | SLA threshold: maximum acceptable null rate (0.0–1.0). Currently stored; planned for column-level SLA checks. |
| `sla_owner` | STRING | No | Email or team identifier notified on SLA breach. |

### Example config entries

```sql
-- Minimal entry (no SLA, no cached payload)
INSERT INTO catalog.dq.dq_config
  (table_name, business_rules, dq_rule_payload, change_flag)
VALUES
  ('catalog.bronze.customers',
   'customer_id must not be null. email must be a valid email format. signup_date must not be in the future.',
   NULL, TRUE);

-- Entry with SLA thresholds
INSERT INTO catalog.dq.dq_config
  (table_name, business_rules, dq_rule_payload, change_flag, min_quality_pct, max_null_rate, sla_owner)
VALUES
  ('catalog.silver.transactions',
   'transaction_id must be unique and not null. amount must be positive. currency must be a 3-letter ISO code.',
   NULL, TRUE, 98.0, 0.02, 'finance-team@company.com');
```

### Force rule regeneration for a table

```python
cfg = DQConfig(spark, config_table="catalog.dq.dq_config")
cfg.set_change_flag("catalog.bronze.orders", flag=True)
```

### Writing rules manually (bypassing AI generation)

Use `RuleTemplate` to build dqx-compatible rules without AI:

```python
from dq_framework.quality.rules import RuleTemplate
import json

rules = [
    RuleTemplate.not_null("order_id"),
    RuleTemplate.not_null("customer_id"),
    RuleTemplate.positive_value("amount"),
    RuleTemplate.date_range("order_date", min_date="2020-01-01"),
    RuleTemplate.email_format("customer_email"),
]

cfg.update_dq_rule_payload(config_id=1, payload=json.dumps(rules))
```

---

## 7. Running the Pipeline

### What each task does

#### Task 1 — `dq_run` (`notebooks/dq_run.py`)

1. Reads all rows from `dq_config`
2. For each table:
   - Loads the config row
   - If `change_flag=True` or `dq_rule_payload` is NULL: calls `DQGenerator` to create rules via AI, stores the payload back to `dq_config`
   - Applies the rules to the source table via `DQEngine`
   - Computes DQ score: `(total_rows - error_rows) / total_rows × 100`
   - Writes metric stats to `dq_audit`
   - Routes rows where `_errors IS NOT NULL AND size(_errors) > 0` to `<table>_quarantine`
3. Prints a batch summary (total / succeeded / failed / duration)

#### Task 2 — `dq_alerts` (`notebooks/dq_alerts.py`, mode=alerts)

Reads from the views and detects three types of drift between the latest and previous run:
- **Metric drift** — column-level quality % changed beyond 10% (configurable)
- **Count drift** — table row count changed beyond 20% (configurable)
- **Column drift** — columns were added or removed

Writes results to:
- `dq_metric_drift_audit`
- `dq_count_drift_audit`
- `dq_column_drift_audit`

#### Task 3 — `dq_sla` (`notebooks/dq_alerts.py`, mode=sla)

Joins `dq_audit` with `dq_config` SLA thresholds. Any table where `dq_score < min_quality_pct` is written to `dq_sla_breach_audit`.

### DQ Score formula

```
dq_score = max(0, (total_rows - error_rows) / total_rows × 100)
```

Where `error_rows` = rows where `_errors IS NOT NULL AND size(_errors) > 0`.

Score range: **0** (all rows failed) to **100** (all rows passed).

### Running a single table manually (Python)

```python
from dq_framework import DQRunner

runner = DQRunner(
    spark          = spark,
    config_table   = "catalog.dq.dq_config",
    table_name     = "catalog.bronze.orders",   # single table
    catalog_schema = "catalog.dq",
)

results = runner.run_dq()
print(runner.get_batch_summary(results))
```

### Running multiple tables in parallel

```python
runner = DQRunner(
    spark          = spark,
    config_table   = "catalog.dq.dq_config",
    table_name     = [                          # list → threaded batch run
        "catalog.bronze.orders",
        "catalog.bronze.customers",
        "catalog.bronze.products",
    ],
    catalog_schema = "catalog.dq",
    max_workers    = 4,    # default: min(len(tables), 8)
    max_retries    = 2,    # retry failed tables once
)

results = runner.run_dq()  # always returns List[DQBatchResult]
summary = runner.get_batch_summary(results)
# {'total': 3, 'succeeded': 3, 'failed': 0, 'total_duration_s': 42.1}
```

### `DQBatchResult` fields

| Field | Type | Description |
|---|---|---|
| `table_name` | str | Table that was processed |
| `success` | bool | True if run completed without error |
| `duration_s` | float | Wall-clock time in seconds |
| `result` | DataFrame | Metric stats DataFrame (if success) |
| `error` | Exception | Captured exception (if failed) |
| `thread_name` | str | Worker thread name (multi-table mode) |

---

## 8. Monitoring & Alerts

### Analytical views

All views are created by `DQViews.create_all()` (run as part of the bootstrap notebook).

#### `dq_simplified_vw`
Flattens `dq_audit` into one row per table / column / run. This is the base view used by most downstream queries.

Key columns: `config_id`, `table_name`, `column_name`, `good_quality_pct`, `error_count`, `dq_score`, `partition_date`

#### `dq_column_vw`
Selects column-level quality % per run, used by `DQAlertSystem` for metric drift detection.

#### `dq_metric_drift_vw`
Pre-computed view showing the quality % change between the two most recent runs per column.

#### `dq_count_drift_vw`
Pre-computed view showing the row count change between the two most recent runs per table.

#### `dq_column_drift_vw`
Pre-computed view showing columns added or removed between the two most recent runs.

### Querying quality inline

```sql
-- Current quality score per table
SELECT table_name, dq_score, partition_date
FROM catalog.dq.dq_simplified_vw
WHERE partition_date = current_date()
GROUP BY table_name, dq_score, partition_date
ORDER BY dq_score ASC;

-- Tables that dropped in quality today
SELECT table_name, column_name, quality_pct_change, is_drastic
FROM catalog.dq.dq_metric_drift_vw
WHERE partition_date = current_date()
  AND is_drastic = TRUE;

-- Row count changes
SELECT table_name, prev_count, curr_count, count_change_pct
FROM catalog.dq.dq_count_drift_vw
WHERE partition_date = current_date();
```

### Running alerts from Python

```python
from dq_framework import DQAlertSystem

alerts = DQAlertSystem(
    spark         = spark,
    simplified_vw = "catalog.dq.dq_simplified_vw",
    column_vw     = "catalog.dq.dq_column_vw",
    alert_catalog = "catalog.dq",
)

# Run all checks and write to Delta
status = alerts.save_alerts(
    metric_threshold_pct = 10.0,   # flag if quality drops/rises by >10%
    count_threshold_pct  = 20.0,   # flag if row count changes by >20%
    drastic_only         = False,   # True = only write flagged rows
)
print(status)
# {'metric_drift': True, 'count_drift': True, 'column_drift': True}

# Inspect without writing
report = alerts.run_all_checks()
report["metric_drift"].filter("is_drastic").show()
```

---

## 9. SLA Enforcement & Notifications

### Setting SLA thresholds

SLA thresholds are set per table in `dq_config`:

| Column | Meaning | Example |
|---|---|---|
| `min_quality_pct` | Run fails SLA if `dq_score < this` | `95.0` = require 95% quality |
| `max_null_rate` | Informational; stored for future column-level SLA | `0.05` = allow 5% nulls |
| `sla_owner` | Who is notified on breach | `finance-team@company.com` |

### How SLA checking works

1. `DQSLAChecker` reads the latest `dq_audit` partition
2. Joins with `dq_config` on `table_name`
3. Any row where `dq_score < min_quality_pct` is a breach
4. Breaches are written to `dq_sla_breach_audit`

```python
from dq_framework import DQSLAChecker

sla = DQSLAChecker(
    spark        = spark,
    audit_table  = "catalog.dq.dq_audit",
    config_table = "catalog.dq.dq_config",
    breach_table = "catalog.dq.dq_sla_breach_audit",
)
sla.save_breaches()
```

### `dq_sla_breach_audit` schema

| Column | Description |
|---|---|
| `config_id` | Config entry that breached |
| `table_name` | Table that failed SLA |
| `dq_score` | Actual score achieved |
| `min_quality_pct` | Threshold that was set |
| `sla_owner` | Notified owner |
| `partition_date` | Date of the breach |

### Notifications

Wire `DQNotifier` to `DQAlertSystem` so alerts are dispatched automatically after every save:

```python
from dq_framework import DQNotifier, DQAlertSystem

notifier = DQNotifier(
    slack_webhook = "https://hooks.slack.com/services/...",   # Slack
    # teams_webhook = "https://...",                          # Teams
    # smtp_host="smtp.company.com", smtp_port=587,            # Email
    # smtp_user="dq@company.com", smtp_password="...",
    # email_recipients=["team@company.com"],
)

alerts = DQAlertSystem(
    spark         = spark,
    simplified_vw = "catalog.dq.dq_simplified_vw",
    column_vw     = "catalog.dq.dq_column_vw",
    alert_catalog = "catalog.dq",
    notifier      = notifier,           # ← attach here
)

alerts.save_alerts()
# Alerts written to Delta AND dispatched via Slack/email automatically
```

> Notification credentials should be stored as Databricks Secrets and fetched with `dbutils.secrets.get()` — never hardcode them in notebooks or config.

```python
slack_webhook = dbutils.secrets.get(scope="dq-secrets", key="slack-webhook")
```

---

## 10. Quarantine

### What it does

When quarantine is enabled, DQ Framework splits the output of each table run into:
- **Clean rows** → written to `dq_audit` (metric stats only)
- **Error rows** → written to `<catalog>.<schema>.<table_name>_quarantine` Delta table

Error rows are rows where `_errors IS NOT NULL AND size(_errors) > 0`.

### How to enable it

Pass a `DQQuarantine` instance to `DQRunner`:

```python
from dq_framework import DQRunner, DQQuarantine

quarantine = DQQuarantine(spark, catalog_schema="catalog.dq")

runner = DQRunner(
    spark          = spark,
    config_table   = "catalog.dq.dq_config",
    table_name     = ["catalog.bronze.orders"],
    catalog_schema = "catalog.dq",
    quarantine     = quarantine,          # ← attach here
)

runner.run_dq()
```

### Quarantine table schema

Each quarantine table has all original columns from the source table, plus:

| Added column | Description |
|---|---|
| `_errors` | Array of dqx error objects from the failing rules |
| `_source_table` | Fully qualified name of the source table |
| `_quarantine_ts` | Timestamp when the row was quarantined |
| `_partition_date` | Date of the quarantine run (partition column) |

### Remediating quarantined rows

```sql
-- See what errors caused rows to be quarantined
SELECT _source_table, _errors, COUNT(*) as error_count
FROM catalog.dq.orders_quarantine
WHERE _partition_date = current_date()
GROUP BY _source_table, _errors
ORDER BY error_count DESC;

-- Fix and re-insert after remediation
-- 1. Fix the data in the source
-- 2. Set change_flag=TRUE to re-run rules
-- 3. Delete the quarantine partition after confirming the re-run is clean
```

---

## 11. Rule Registry

`RuleRegistry` stores versioned snapshots of DQ rules in `dq_rule_history`. This lets you track what rules were in effect at any point and roll back if needed.

### Save a version

```python
from dq_framework.quality.rules import RuleRegistry, RuleTemplate

registry = RuleRegistry(spark, rule_history_table="catalog.dq.dq_rule_history")

rules = [
    RuleTemplate.not_null("order_id"),
    RuleTemplate.positive_value("amount"),
]

registry.save_version(
    config_id   = 1,
    table_name  = "catalog.bronze.orders",
    rules       = rules,
    version     = 2,
    description = "Added positive amount check",
)
```

### Retrieve current rules

```python
current = registry.get_current(config_id=1)
```

### View history

```python
history = registry.get_history(config_id=1)
history.show()
```

### Roll back to a previous version

```python
registry.rollback(config_id=1, version=1)
# Restores version 1 rules to dq_config.dq_rule_payload
# and sets change_flag=FALSE
```

---

## 12. CLI Reference

Install the CLI extra:
```bash
pip install "dq_framework[cli]"
```

Set authentication:
```bash
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<your-pat>
```

### Commands

```bash
# Bootstrap all Delta tables and views
dq-framework setup --catalog my_catalog --schema dq

# Validate config for a table (pre-flight check without running)
dq-framework validate catalog.bronze.orders \
  --config-table catalog.dq.dq_config

# Run DQ for all tables in config
dq-framework run \
  --config-table catalog.dq.dq_config \
  --catalog-schema catalog.dq

# Get DQ score for a table from audit
dq-framework score catalog.bronze.orders \
  --audit-table catalog.dq.dq_audit

# Scheduling (dev/testing use — production uses DAB)
dq-framework schedule create --name my-dq-job --notebook /path/to/notebook
dq-framework schedule list
dq-framework schedule pause --job-id 12345
dq-framework schedule delete --job-id 12345

# Show version
dq-framework version
```

---

## 13. CI/CD — Azure DevOps

### Pipeline overview (`azure-pipelines.yml`)

```
PR to main ──► CI stage
                ├── pip install -e ".[dev]"
                ├── ruff check src/ tests/
                ├── pytest tests/ -v
                └── databricks bundle validate --target dev
                        │
               (merge to main)
                        ▼
              DeployDev stage (automatic)
                └── databricks bundle deploy --target dev
                        │
              (manual approval gate)
                        ▼
              DeployProd stage
                └── databricks bundle deploy --target prod
```

### Required ADO setup

#### 1. Variable group: `dq-framework`

Create in ADO → Pipelines → Library → Variable groups.

| Variable | Value |
|---|---|
| `DATABRICKS_HOST_DEV` | `https://<dev-workspace>.azuredatabricks.net` |
| `DATABRICKS_TOKEN_DEV` | Service principal token for dev workspace |
| `DATABRICKS_HOST_PROD` | `https://<prod-workspace>.azuredatabricks.net` |
| `DATABRICKS_TOKEN_PROD` | Service principal token for prod workspace |
| `CATALOG_DEV` | `dev_catalog` |
| `CATALOG_PROD` | `prod_catalog` |

Mark `DATABRICKS_TOKEN_DEV` and `DATABRICKS_TOKEN_PROD` as **secret**.

#### 2. Environments

In ADO → Pipelines → Environments, create:
- `dq-framework-dev` — no approval required (auto-deploys on merge)
- `dq-framework-prod` — add **Approval** check; specify approvers

#### 3. Pipeline file

Point your ADO pipeline at `azure-pipelines.yml` in the repository root.

### Makefile shortcuts (local)

```bash
make install       # pip install -e ".[all,dev]"
make lint          # ruff check src/ tests/
make test          # pytest tests/ -v
make build         # python -m build --wheel
make validate      # databricks bundle validate --target dev
make deploy-dev    # databricks bundle deploy --target dev
make deploy-prod   # databricks bundle deploy --target prod  (confirms before running)
make clean         # remove dist/, build/, __pycache__
```

---

## 14. Troubleshooting

### `DQConfig.__init__() got an unexpected keyword argument 'table_name'`

`DQConfig` only accepts `spark` and `config_table`. Remove `table_name` from the constructor — pass it to individual methods like `get_config_from_delta(table_name)`.

```python
# Wrong
cfg = DQConfig(spark, config_table="...", table_name="...")

# Correct
cfg = DQConfig(spark, config_table="...")
result = cfg.get_config_from_delta("catalog.bronze.orders")
```

### `TypeError: object of type 'DataFrame' has no len()`

`run_dq()` always returns `List[DQBatchResult]` in v1.1. If you see this error it means you are on an older build. Rebuild and redeploy the wheel.

### `DELTA_FAILED_TO_MERGE_FIELDS: Failed to merge fields 'config_id'`

Type mismatch between the DataFrame being written and the `dq_audit` table schema. Fixed in v1.1 by casting `config_id` to `BIGINT` explicitly. Rebuild and redeploy.

### `DQAlertSystem.__init__() got an unexpected keyword argument 'count_vw'`

The second view parameter is `column_vw`, not `count_vw`. The correct view name is `dq_column_vw`.

```python
# Correct
DQAlertSystem(spark, simplified_vw="...dq_simplified_vw", column_vw="...dq_column_vw", ...)
```

### `bundle validate` fails: `root_path must start with '~/'`

In `development` mode, DAB requires the root path to include `~` or the username. Use:
```yaml
workspace:
  root_path: ~/.bundle/dq-framework/${bundle.target}
```

### `cannot resolve bundle auth: multiple profiles matched`

Multiple Databricks CLI profiles share the same host URL. Pin a specific profile in `databricks.yml`:
```yaml
targets:
  dev:
    workspace:
      host: https://<workspace>.azuredatabricks.net
      profile: <your-profile-name>
```

### `file doesn't exist resources/jobs/dist/...`

Wheel paths in job YAML are relative to the YAML file's location (`resources/jobs/`), not the bundle root. Use `../../dist/` to navigate back:
```yaml
libraries:
  - whl: ../../dist/dq_framework-1.1.0-py3-none-any.whl
```

### `No config found for table='...'`

The table name in `dq_config` must exactly match (case-insensitive) the fully qualified table name passed to `DQRunner`. Check for extra spaces or schema mismatches.

### AI rule generation fails / returns empty

- Ensure the workspace has internet access and `databricks-labs-dqx[llm]` is installed
- Check that `business_rules` is descriptive enough (at least one rule sentence per column to check)
- Set `force_regenerate=True` to retry: `runner.run_dq(force_regenerate=True)`
- Check Databricks cluster logs for dqx-specific errors

---

## 15. Versioning & Upgrade Guide

### Version numbering

`dq-framework` follows semantic versioning (`MAJOR.MINOR.PATCH`):

| Change | Version bump |
|---|---|
| Breaking API change | MAJOR |
| New module / feature, backward-compatible | MINOR |
| Bug fix, performance improvement | PATCH |

Current version: **1.1.0**

### Upgrading `databricks-labs-dqx`

dqx minor releases can change method signatures. When upgrading:

1. Update the pinned version in `pyproject.toml`:
   ```toml
   "databricks-labs-dqx==0.14.0"
   ```
2. Run `pytest tests/` to verify mocked tests still pass
3. Run `runner.run_dq(debug_flag=True)` on a single table in dev (skips audit write) to verify the dqx call works end-to-end
4. If `apply_checks_by_metadata` or `generate_dq_rules_ai_assisted` signatures changed, update `runner.py` accordingly

### Release checklist

```
[ ] Bump version in pyproject.toml
[ ] Bump version in src/dq_framework/__init__.py (__version__)
[ ] Update DOCUMENTATION.md version header
[ ] Run: make lint && make test
[ ] Run: make build   (verify wheel builds cleanly)
[ ] Run: databricks bundle validate --target dev
[ ] Run: databricks bundle validate --target prod
[ ] Commit, push, open PR
[ ] CI passes → merge → auto-deploys to dev
[ ] Smoke test on dev: trigger job, verify dq_audit has rows
[ ] Approve production deployment in ADO
```

### Adding a new table to monitor

1. Insert a row into `dq_config` (see [Section 6](#6-configuration-reference))
2. No code change or redeployment needed
3. The next scheduled run or manual trigger will pick it up automatically

### Removing a table

```sql
DELETE FROM catalog.dq.dq_config WHERE table_name = 'catalog.bronze.old_table';
```

The quarantine and audit history remain for audit purposes. Drop them manually if no longer needed.

---

*Document maintained by Vivek Rawat. For issues, raise a ticket in the project repository.*
