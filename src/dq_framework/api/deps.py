"""
Databricks SDK dependency for the DQ Framework API.

In Databricks Apps the SDK auto-configures from the runtime environment
(no token/host env vars needed). The SQL warehouse ID is the only
required environment variable: WAREHOUSE_ID.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any, Generator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import HTTPException


def _client() -> WorkspaceClient:
    return WorkspaceClient()


@lru_cache(maxsize=1)
def get_warehouse_id() -> str:
    wid = os.environ.get("WAREHOUSE_ID", "")
    if not wid:
        raise RuntimeError(
            "WAREHOUSE_ID environment variable is not set. "
            "Set it in the Databricks App configuration."
        )
    return wid


@lru_cache(maxsize=1)
def get_catalog() -> str:
    return os.environ.get("DQ_CATALOG", "dev_catalog")


@lru_cache(maxsize=1)
def get_schema() -> str:
    return os.environ.get("DQ_SCHEMA", "dq")


def run_query(sql: str, parameters: list | None = None) -> list[dict[str, Any]]:
    """
    Execute a SQL statement via the Databricks statement execution API
    and return results as a list of dicts.

    Runs synchronously (waits for completion) — suitable for interactive
    API requests where the result set is small (< 10k rows).
    """
    client       = _client()
    warehouse_id = get_warehouse_id()

    try:
        response = client.statement_execution.execute_statement(
            warehouse_id = warehouse_id,
            statement    = sql,
            catalog      = get_catalog(),
            schema       = get_schema(),
            wait_timeout = "30s",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}")

    if response.status.state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
        error_msg = getattr(response.status, "error", {})
        raise HTTPException(status_code=500, detail=f"Query failed: {error_msg}")

    if not response.result or not response.manifest:
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows    = response.result.data_array or []
    return [dict(zip(columns, row)) for row in rows]
