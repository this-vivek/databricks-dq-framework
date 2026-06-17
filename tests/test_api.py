"""Tests for the public package surface: PEP8 names, deprecated aliases, exceptions."""

import pytest

import dq_framework
from dq_framework import (
    ConfigNotFoundError,
    DQAlertSystem,
    DQBatchResult,
    DQConfig,
    DQDependencyError,
    DQError,
    MetricComputationError,
    RuleApplicationError,
    RuleGenerationError,
)


def test_public_names_exist():
    assert dq_framework.__version__ == "1.1.0"
    for cls in (DQConfig, DQBatchResult, DQAlertSystem):
        assert isinstance(cls, type)


@pytest.mark.parametrize(
    "old_name, new_name",
    [
        ("DQ_Config", "DQConfig"),
        ("DQ_Runner", "DQRunner"),
        ("DQ_BatchResult", "DQBatchResult"),
        ("DQ_AlertSystem", "DQAlertSystem"),
    ],
)
def test_deprecated_alias_warns_and_resolves(old_name, new_name):
    with pytest.warns(DeprecationWarning):
        aliased = getattr(dq_framework, old_name)
    assert aliased is getattr(dq_framework, new_name)


def test_unknown_attribute_raises_attributeerror():
    with pytest.raises(AttributeError):
        _ = dq_framework.NoSuchThing


def test_exceptions_subclass_dqerror():
    for exc in (
        ConfigNotFoundError,
        RuleGenerationError,
        RuleApplicationError,
        MetricComputationError,
        DQDependencyError,
    ):
        assert issubclass(exc, DQError)


def test_dependency_error_is_also_importerror():
    assert issubclass(DQDependencyError, ImportError)
