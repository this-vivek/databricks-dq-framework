"""
dq_framework.exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~
Typed exception hierarchy for the DQ framework.

Design
------
``dq_framework`` separates two kinds of "failure":

* **Normal absence** (no config row for a table, an empty stats result) is *not*
  exceptional — query helpers keep returning ``None`` / ``False`` for those.
* **Fatal pipeline failures** (config missing where one is required, rule
  generation/application failing, metric computation failing) raise a subclass of
  :class:`DQError`, so callers cannot silently ignore them.

All exceptions derive from :class:`DQError`, so downstream code can catch the whole
family with a single ``except DQError``.
"""

from __future__ import annotations


class DQError(Exception):
    """Base class for every error raised by ``dq_framework``."""


class DQDependencyError(DQError, ImportError):
    """
    A required optional dependency (``databricks-labs-dqx`` / ``databricks-sdk``)
    is not installed.

    Subclasses :class:`ImportError` as well so existing ``except ImportError``
    handlers keep working.
    """


class ConfigNotFoundError(DQError):
    """No config row — or an ambiguous set of rows — was found for a table."""


class RuleGenerationError(DQError):
    """AI-assisted DQ rule generation failed or produced no rules."""


class RuleApplicationError(DQError):
    """Applying DQ rules to a table failed, or produced no status columns."""


class MetricComputationError(DQError):
    """Aggregating DQ error metrics into the audit payload failed."""


class AuditWriteError(DQError):
    """Persisting audit / alert results to a Delta table failed."""


__all__ = [
    "DQError",
    "DQDependencyError",
    "ConfigNotFoundError",
    "RuleGenerationError",
    "RuleApplicationError",
    "MetricComputationError",
    "AuditWriteError",
]
