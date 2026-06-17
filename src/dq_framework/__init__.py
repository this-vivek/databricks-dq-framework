"""
dq_framework
============
Reusable, config-driven Databricks Data Quality framework.

Core API
--------
DQSetup        — one-call bootstrap (creates all tables + views)
DQConfig       — manage and validate the DQ config Delta table
DQRunner       — orchestrate the end-to-end DQ pipeline (single or multi-table)
DQBatchResult  — result container for multi-table batch runs
DQAlertSystem  — metric, count, and schema drift detection with Delta persistence
DQNotifier     — Slack / Teams / email alert dispatch
DQQuarantine   — route error rows to an isolated Delta table
DQSLAChecker   — detect SLA breaches per table
DQScheduler    — create and manage Databricks Workflows jobs
RuleTemplate   — pre-built DQ rule factory (not_null, date_range, email, …)
RuleRegistry   — versioned rule storage with rollback
DQViews        — create/refresh the five standard DQ metric views
DQError, …     — typed exception hierarchy (see ``dq_framework.exceptions``)

.. note::
   The pre-1.0 names ``DQ_Config`` / ``DQ_Runner`` / ``DQ_BatchResult`` /
   ``DQ_AlertSystem`` still work as deprecated aliases (they emit a
   ``DeprecationWarning``). Migrate to the PEP 8 names above.

Quick start (inside a Databricks notebook)
------------------------------------------
    from dq_framework import DQSetup, DQRunner, DQAlertSystem, DQNotifier

    # 1. One-time setup
    DQSetup(spark, catalog="prod", schema="dq").bootstrap()

    # 2. Run DQ
    runner = DQRunner(
        spark          = spark,
        config_table   = "prod.dq.dq_config",
        table_name     = ["prod.sales.orders", "prod.sales.customers"],
        catalog_schema = "prod.dq",
        max_retries    = 2,
    )
    results = runner.run_dq()

    # 3. Alerts + notifications
    notifier = DQNotifier(slack_webhook="https://hooks.slack.com/services/...")
    alerts   = DQAlertSystem(
        spark          = spark,
        simplified_vw  = "prod.dq.dq_simplified_vw",
        column_vw      = "prod.dq.dq_column_vw",
        alert_catalog  = "prod.dq",
        notifier       = notifier,
    )
    alerts.save_alerts(metric_threshold_pct=10.0, count_threshold_pct=20.0)
"""

from __future__ import annotations

import logging
import warnings

from .core.config import DQConfig
from .core.results import DQBatchResult
from .core.runner import DQRunner
from .core.scoring import compute_dq_score
from .exceptions import (
    AuditWriteError,
    ConfigNotFoundError,
    DQDependencyError,
    DQError,
    MetricComputationError,
    RuleApplicationError,
    RuleGenerationError,
)
from .infra.scheduling import DQScheduler
from .infra.setup import DQSetup
from .infra.views import DQViews
from .monitoring.alerts import DQAlertSystem
from .monitoring.notifications import DQNotifier
from .monitoring.sla import DQSLAChecker
from .quality.quarantine import DQQuarantine
from .quality.rules import RuleRegistry, RuleTemplate

__version__ = "1.1.0"

__all__ = [
    # Setup & infrastructure
    "DQSetup",
    "DQViews",
    # Core pipeline
    "DQConfig",
    "DQRunner",
    "DQBatchResult",
    # Alerting & notifications
    "DQAlertSystem",
    "DQNotifier",
    # Rules
    "RuleTemplate",
    "RuleRegistry",
    # Quality scoring
    "compute_dq_score",
    # Quarantine
    "DQQuarantine",
    # SLA
    "DQSLAChecker",
    # Scheduling
    "DQScheduler",
    # Exceptions
    "DQError",
    "DQDependencyError",
    "ConfigNotFoundError",
    "RuleGenerationError",
    "RuleApplicationError",
    "MetricComputationError",
    "AuditWriteError",
]

# Pre-1.0 names kept as deprecated aliases (PEP 562 module-level __getattr__).
_DEPRECATED_ALIASES = {
    "DQ_Config":      "DQConfig",
    "DQ_Runner":      "DQRunner",
    "DQ_BatchResult": "DQBatchResult",
    "DQ_AlertSystem": "DQAlertSystem",
}


def __getattr__(name: str):
    """Resolve deprecated class aliases, emitting a ``DeprecationWarning``."""
    new_name = _DEPRECATED_ALIASES.get(name)
    if new_name is not None:
        warnings.warn(
            f"'{name}' is deprecated and will be removed in a future release; "
            f"use '{new_name}' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[new_name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted([*__all__, *_DEPRECATED_ALIASES])


# Set a NullHandler so library users control log output.
logging.getLogger(__name__).addHandler(logging.NullHandler())
