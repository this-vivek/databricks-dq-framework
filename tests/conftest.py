"""
Shared pytest fixtures for the dq_framework test suite.

These tests mock Spark/Delta so they run anywhere (no Databricks, no SparkContext).
Where a method calls ``pyspark.sql.functions`` directly, the relevant ``F`` symbol is
patched per test so the framework logic — not the JVM — is what gets exercised.
"""

import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_spark():
    """A MagicMock SparkSession whose .table()/.read.table() return mock DataFrames."""
    spark = MagicMock()
    spark.table.return_value = MagicMock()
    spark.read.table.return_value = MagicMock()
    return spark


@pytest.fixture
def sample_config_row():
    """A mock Spark Row mimicking one dq_config record."""
    row = MagicMock()
    row.asDict.return_value = {
        "config_id":       1,
        "table_name":      "catalog.schema.my_table",
        "business_rules":  "check nulls on patient_id",
        "dq_rule_payload": json.dumps({"checks": []}),
        "change_flag":     False,
    }
    return row


@pytest.fixture
def mock_df_chain():
    """
    Returns a helper that wires the filter/select/limit/collect chain on
    ``mock_spark.table.return_value`` to yield a given list of rows.
    """
    def _make(mock_spark, rows):
        chain = mock_spark.table.return_value
        chain.filter.return_value = chain
        chain.select.return_value = chain
        chain.limit.return_value  = chain
        chain.collect.return_value = rows
        return chain

    return _make
