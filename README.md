# dq-framework

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Built on dqx](https://img.shields.io/badge/built%20on-databricks--labs--dqx-orange.svg)](https://github.com/databrickslabs/dqx)
[![Version](https://img.shields.io/badge/version-1.1.0-green.svg)]()

A production-ready, **config-driven** Databricks Data Quality framework built on
[`databricks-labs-dqx`](https://github.com/databrickslabs/dqx). It generates DQ rules
from plain-language business descriptions, runs them across one or many tables, persists
metric results to Delta audit tables, detects drift, enforces SLAs, routes bad rows to
quarantine, and deploys end-to-end via **Databricks Asset Bundles (DAB)**.

---

## Features

| Area | Capability |
|---|---|
| **Rule execution** | AI-assisted rule generation from business rules; single & multi-table parallel runs |
| **Audit trail** | Partitioned Delta audit table with DQ scores per run |
| **Drift alerting** | Metric-quality, row-count, and schema drift detection |
| **SLA enforcement** | Per-table quality thresholds; breach audit table |
| **Quarantine** | Error rows routed to `<table>_quarantine` Delta tables |
| **Notifications** | Slack, Teams webhook, and SMTP email |
| **Rule registry** | Versioned rule history with rollback |
| **CLI** | `dq-framework` command for setup, validate, run, score, schedule |
| **DAB deployment** | Full Databricks Asset Bundle with CI/CD via Azure DevOps |
| **Typed errors** | `DQError` hierarchy — no silent failures |

---

## Architecture

```
dq_config (Delta)
      │
      ▼
  DQRunner ──► AI rule gen ──► apply rules ──► DQ score ──► dq_audit (Delta)
      │                                              │
      └──► DQQuarantine ──► <table>_quarantine       │
                                                     ▼
                                            dq_simplified_vw
                                            dq_column_vw
                                                     │
                                            DQAlertSystem
                                                     │
                                            DQSLAChecker ──► dq_sla_breach_audit
                                                     │
                                            DQNotifier (Slack / Teams / Email)
```

---

## Package structure

```
dq-framework/
├── databricks.yml              # DAB bundle root (dev / staging / prod targets)
├── azure-pipelines.yml         # Azure DevOps CI/CD pipeline
├── Makefile                    # make test | lint | build | deploy-dev | deploy-prod
├── pyproject.toml
├── README.md
├── LICENSE
├── .gitignore
│
├── resources/
│   └── jobs/
│       └── dq_pipeline_job.yml # 3-task DAB job: dq_run → dq_alerts → dq_sla
│
├── notebooks/
│   ├── dq_setup.py             # One-time bootstrap (tables + views)
│   ├── dq_run.py               # Task 1 — run DQ rules, write audit + quarantine
│   └── dq_alerts.py            # Task 2 (alerts) + Task 3 (SLA)
│
├── src/
│   └── dq_framework/
│       ├── __init__.py         # Public API + deprecated aliases
│       ├── exceptions.py       # DQError hierarchy
│       │
│       ├── core/               # Pipeline engine
│       │   ├── config.py       # DQConfig
│       │   ├── runner.py       # DQRunner
│       │   ├── results.py      # DQBatchResult + summarize_batch
│       │   ├── metrics.py      # compute_metric_stats
│       │   ├── scoring.py      # compute_dq_score
│       │   └── _explain.py     # Thread-safe explain suppressor
│       │
│       ├── quality/            # Data quality controls
│       │   ├── rules.py        # RuleTemplate + RuleRegistry
│       │   └── quarantine.py   # DQQuarantine
│       │
│       ├── monitoring/         # Observability
│       │   ├── alerts.py       # DQAlertSystem
│       │   ├── sla.py          # DQSLAChecker
│       │   └── notifications.py# DQNotifier (Slack / Teams / Email)
│       │
│       └── infra/              # Setup & lifecycle
│           ├── setup.py        # DQSetup (bootstrap DDL)
│           ├── views.py        # DQViews (5 analytical views)
│           └── scheduling.py   # DQScheduler (Databricks SDK job CRUD)
│
├── examples/
│   └── dq_framework_demo.py    # 16-cell Databricks notebook walkthrough
│
└── tests/                      # Mocked unit tests (run anywhere, no Databricks needed)
    ├── conftest.py
    ├── test_config.py
    ├── test_runner.py
    ├── test_results.py
    └── test_alerts.py
```

---

## Delta tables & views

### Tables (created by `DQSetup.bootstrap()`)

| Table | Purpose |
|---|---|
| `dq_config` | Per-table business rules, DQ rule payload, SLA thresholds, change flag |
| `dq_audit` | DQ metric results per run — partitioned by `partition_date`, includes `dq_score` |
| `dq_rule_history` | Versioned rule snapshots with rollback support |
| `dq_sla_breach_audit` | SLA threshold violations per table per run |

### Views (created by `DQViews.create_all()`)

| View | Purpose |
|---|---|
| `dq_simplified_vw` | Flattened audit — one row per table/column/run |
| `dq_column_vw` | Column-level quality % over time |
| `dq_metric_drift_vw` | Metric-quality drift between consecutive runs |
| `dq_count_drift_vw` | Row-count drift between consecutive runs |
| `dq_column_drift_vw` | Schema (column add/remove) drift between runs |

---

## Deployment with DAB

### Prerequisites
- Databricks CLI >= 0.221 installed and authenticated
- Unity Catalog enabled on the workspace
- `dq_config` table bootstrapped (run `notebooks/dq_setup.py` once per environment)

### Deploy to dev
```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

### Run the pipeline manually
```bash
databricks bundle run dq_pipeline --target dev
```

### Makefile shortcuts
```bash
make install        # pip install -e ".[dev]"
make test           # pytest tests/
make lint           # ruff check src/ tests/
make build          # python -m build --wheel
make deploy-dev     # databricks bundle deploy --target dev
make deploy-prod    # databricks bundle deploy --target prod  (prompts for confirmation)
```

### CI/CD (Azure DevOps)
The `azure-pipelines.yml` defines three stages:

| Stage | Trigger | What it does |
|---|---|---|
| **CI** | Every PR to `main` | Lint (ruff) + pytest + `bundle validate` |
| **DeployDev** | Merge to `main` | `bundle deploy --target dev` (automatic) |
| **DeployProd** | After DeployDev | `bundle deploy --target prod` (requires manual approval gate) |

Required ADO variable group `dq-framework`:
- `DATABRICKS_HOST_DEV`, `DATABRICKS_TOKEN_DEV`
- `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD`
- `CATALOG_DEV`, `CATALOG_PROD`

---

## Quick start (notebook / interactive)

```python
from dq_framework import DQRunner, DQAlertSystem, DQSetup, DQViews

# One-time bootstrap
setup = DQSetup(spark, catalog="my_catalog", schema="dq")
setup.bootstrap()
DQViews(spark, catalog="my_catalog", schema="dq").create_all()

# Run DQ — always returns List[DQBatchResult] (1 table or 50)
runner = DQRunner(
    spark          = spark,
    config_table   = "my_catalog.dq.dq_config",
    table_name     = ["my_catalog.bronze.orders", "my_catalog.bronze.customers"],
    catalog_schema = "my_catalog.dq",
)
results = runner.run_dq()
print(runner.get_batch_summary(results))

# Alerts
from dq_framework import DQAlertSystem
alerts = DQAlertSystem(
    spark         = spark,
    simplified_vw = "my_catalog.dq.dq_simplified_vw",
    column_vw     = "my_catalog.dq.dq_column_vw",
    alert_catalog = "my_catalog.dq",
)
alerts.save_alerts()

# SLA
from dq_framework import DQSLAChecker
sla = DQSLAChecker(
    spark        = spark,
    audit_table  = "my_catalog.dq.dq_audit",
    config_table = "my_catalog.dq.dq_config",
    breach_table = "my_catalog.dq.dq_sla_breach_audit",
)
sla.save_breaches()

# Quarantine
from dq_framework import DQQuarantine
DQQuarantine(spark, catalog_schema="my_catalog.dq")
# quarantine is passed to DQRunner; route() is called automatically

# Notifications
from dq_framework import DQNotifier
notifier = DQNotifier(slack_webhook="https://hooks.slack.com/services/...")
notifier.send_alert_summary(results)
```

---

## CLI

```bash
# Install CLI extra
pip install "dq_framework[cli]"

# Bootstrap tables and views
dq-framework setup --catalog my_catalog --schema dq

# Validate config for a table
dq-framework validate my_catalog.bronze.orders --config-table my_catalog.dq.dq_config

# Run DQ for all tables
dq-framework run --config-table my_catalog.dq.dq_config --catalog-schema my_catalog.dq
```

---

## Public API

### Core
| Class / Function | Description |
|---|---|
| `DQConfig` | Reads and updates the DQ config Delta table |
| `DQRunner` | Runs DQ rules — single or multi-table |
| `DQBatchResult` | Per-table result dataclass (success, duration, error) |
| `compute_dq_score` | `(total - error_rows) / total × 100` |

### Quality
| Class | Description |
|---|---|
| `RuleTemplate` | Pre-built rule factory (not_null, date_range, email_format, …) |
| `RuleRegistry` | Versioned rule storage with rollback |
| `DQQuarantine` | Routes error rows to `<table>_quarantine` Delta |

### Monitoring
| Class | Description |
|---|---|
| `DQAlertSystem` | Detects metric / count / column drift; writes alert tables |
| `DQSLAChecker` | Evaluates SLA thresholds; writes breach audit |
| `DQNotifier` | Dispatches Slack, Teams, and email notifications |

### Infra
| Class | Description |
|---|---|
| `DQSetup` | Bootstrap DDL for all Delta tables |
| `DQViews` | Creates the 5 analytical views |
| `DQScheduler` | Databricks SDK wrapper for job CRUD (dev/testing use) |

---

## Error handling

All fatal errors raise a subclass of `DQError`:

| Exception | Raised when |
|---|---|
| `DQDependencyError` | `databricks-labs-dqx` / `databricks-sdk` not installed |
| `ConfigNotFoundError` | No config row found for a table |
| `RuleGenerationError` | AI rule generation returned nothing |
| `RuleApplicationError` | Applying rules failed / produced no status columns |
| `MetricComputationError` | Metric aggregation failed |
| `AuditWriteError` | Writing audit / alert results failed |

In multi-table mode errors are captured per table in `DQBatchResult.error` — the batch never stops. In single-table mode they propagate to the caller.

The pre-1.0 names `DQ_Config` / `DQ_Runner` / `DQ_BatchResult` / `DQ_AlertSystem` still import but emit a `DeprecationWarning`.

---

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v          # mocked unit tests — no Databricks needed
ruff check src/ tests/    # lint
python -m build           # build wheel
```

> The package targets the Databricks runtime (Unity Catalog, Delta, `WorkspaceClient`).
> The test suite mocks Spark/Delta and runs anywhere. Integration tests run on Databricks.

---

## License

[MIT](LICENSE) © Celebal Technologies
