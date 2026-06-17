"""
dq_framework.quality
~~~~~~~~~~~~~~~~~~~~~
Rule management and quarantine — pre-built rule templates, versioned registry,
and error-row routing.
"""

from .quarantine import DQQuarantine
from .rules import RuleRegistry, RuleTemplate

__all__ = [
    "RuleTemplate",
    "RuleRegistry",
    "DQQuarantine",
]
