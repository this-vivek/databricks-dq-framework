"""
DQ Framework REST API — FastAPI application.

Deployed as a Databricks App. All routes query Delta tables via the
Databricks SQL statement execution API (no PySpark runtime needed).

Start locally (for testing):
    WAREHOUSE_ID=<id> DQ_CATALOG=<cat> DQ_SCHEMA=dq uvicorn dq_framework.api.app:app --reload
"""

from __future__ import annotations

from datetime import date

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from dq_framework import __version__
from .deps import get_catalog, get_schema, run_query
from .models import (
    AlertRow,
    AlertsResponse,
    HealthResponse,
    LineageNode,
    LineageResponse,
    ScoreHistoryPoint,
    ScoreHistoryResponse,
    SLAResponse,
    SLAStatus,
    TableScoreResponse,
    TableSummary,
    TablesListResponse,
    ValidationResponse,
)

app = FastAPI(
    title       = "DQ Framework API",
    description = "REST API for Databricks Data Quality Framework — scores, alerts, lineage, SLA.",
    version     = __version__,
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["GET", "POST"],
    allow_headers  = ["*"],
)


def _tbl(name: str) -> str:
    return f"{get_catalog()}.{get_schema()}.{name}"


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    """Returns API health and configured catalog/schema."""
    return HealthResponse(
        status      = "ok",
        version     = __version__,
        catalog     = get_catalog(),
        schema_name = get_schema(),
    )


# ── Tables list ────────────────────────────────────────────────────────────────

@app.get("/tables", response_model=TablesListResponse, tags=["Tables"])
def list_tables():
    """List all monitored tables with their latest DQ score and SLA status."""
    rows = run_query(f"""
        SELECT
            c.table_name,
            a.dq_score                                              AS latest_score,
            a.partition_date                                        AS last_run_date,
            c.min_quality_pct                                       AS sla_threshold,
            CASE
                WHEN c.min_quality_pct IS NULL           THEN 'NO_SLA'
                WHEN a.dq_score >= c.min_quality_pct     THEN 'PASS'
                ELSE 'FAIL'
            END                                                     AS sla_status
        FROM {_tbl("dq_config")} c
        LEFT JOIN (
            SELECT table_name, dq_score, partition_date,
                   ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY partition_date DESC) AS rn
            FROM {_tbl("dq_audit")}
        ) a ON c.table_name = a.table_name AND a.rn = 1
        ORDER BY c.table_name
    """)

    tables = [
        TableSummary(
            table_name    = r["table_name"],
            latest_score  = float(r["latest_score"]) if r["latest_score"] is not None else None,
            last_run_date = date.fromisoformat(r["last_run_date"]) if r["last_run_date"] else None,
            sla_threshold = float(r["sla_threshold"]) if r["sla_threshold"] is not None else None,
            sla_status    = r["sla_status"] or "NO_SLA",
        )
        for r in rows
    ]
    return TablesListResponse(total=len(tables), tables=tables)


# ── Score ──────────────────────────────────────────────────────────────────────

@app.get("/tables/{table_name:path}/score", response_model=TableScoreResponse, tags=["Tables"])
def get_score(table_name: str):
    """Latest DQ score for a specific table."""
    rows = run_query(f"""
        SELECT dq_score, table_count, partition_date, audit_ts
        FROM {_tbl("dq_audit")}
        WHERE table_name = '{table_name}'
        ORDER BY partition_date DESC, audit_ts DESC
        LIMIT 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No audit data found for table '{table_name}'")
    r = rows[0]
    return TableScoreResponse(
        table_name     = table_name,
        dq_score       = float(r["dq_score"])       if r["dq_score"]       is not None else None,
        table_count    = int(r["table_count"])       if r["table_count"]    is not None else None,
        partition_date = date.fromisoformat(r["partition_date"]) if r["partition_date"] else None,
        run_timestamp  = r["audit_ts"],
    )


# ── Score history ──────────────────────────────────────────────────────────────

@app.get("/tables/{table_name:path}/history", response_model=ScoreHistoryResponse, tags=["Tables"])
def get_score_history(
    table_name: str,
    days: int = Query(default=30, ge=1, le=365, description="Number of days of history"),
):
    """DQ score trend over the last N days."""
    rows = run_query(f"""
        SELECT partition_date, dq_score, table_count
        FROM {_tbl("dq_audit")}
        WHERE table_name = '{table_name}'
          AND partition_date >= current_date() - INTERVAL {days} DAYS
        ORDER BY partition_date ASC
    """)
    history = [
        ScoreHistoryPoint(
            partition_date = date.fromisoformat(r["partition_date"]),
            dq_score       = float(r["dq_score"]),
            table_count    = int(r["table_count"]) if r["table_count"] is not None else None,
        )
        for r in rows
        if r["dq_score"] is not None
    ]
    return ScoreHistoryResponse(table_name=table_name, history=history)


# ── Alerts ─────────────────────────────────────────────────────────────────────

@app.get("/tables/{table_name:path}/alerts", response_model=AlertsResponse, tags=["Alerts"])
def get_alerts(
    table_name: str,
    days: int = Query(default=7, ge=1, le=90),
):
    """Open drift alerts for a specific table (metric, count, schema)."""
    rows = run_query(f"""
        SELECT
            table_name,
            'metric_drift'          AS alert_type,
            column_name,
            CAST(curr_pct AS DOUBLE) AS metric_value,
            CAST(prev_pct AS DOUBLE) AS previous_value,
            CAST(pct_change AS DOUBLE) AS change_pct,
            is_drastic,
            partition_date
        FROM {_tbl("dq_metric_drift_audit")}
        WHERE table_name = '{table_name}'
          AND partition_date >= current_date() - INTERVAL {days} DAYS

        UNION ALL

        SELECT
            table_name,
            'count_drift'           AS alert_type,
            NULL                    AS column_name,
            CAST(curr_count AS DOUBLE) AS metric_value,
            CAST(prev_count AS DOUBLE) AS previous_value,
            CAST(count_change_pct AS DOUBLE) AS change_pct,
            is_drastic,
            partition_date
        FROM {_tbl("dq_count_drift_audit")}
        WHERE table_name = '{table_name}'
          AND partition_date >= current_date() - INTERVAL {days} DAYS

        UNION ALL

        SELECT
            table_name,
            'column_drift'          AS alert_type,
            column_name,
            NULL                    AS metric_value,
            NULL                    AS previous_value,
            NULL                    AS change_pct,
            TRUE                    AS is_drastic,
            partition_date
        FROM {_tbl("dq_column_drift_audit")}
        WHERE table_name = '{table_name}'
          AND partition_date >= current_date() - INTERVAL {days} DAYS

        ORDER BY partition_date DESC
    """)

    alerts = [
        AlertRow(
            table_name     = r["table_name"],
            alert_type     = r["alert_type"],
            column_name    = r.get("column_name"),
            metric_value   = float(r["metric_value"])   if r.get("metric_value")   is not None else None,
            previous_value = float(r["previous_value"]) if r.get("previous_value") is not None else None,
            change_pct     = float(r["change_pct"])     if r.get("change_pct")     is not None else None,
            is_drastic     = bool(r["is_drastic"])      if r.get("is_drastic")     is not None else None,
            partition_date = date.fromisoformat(r["partition_date"]),
        )
        for r in rows
    ]
    return AlertsResponse(table_name=table_name, alerts=alerts)


# ── Lineage ────────────────────────────────────────────────────────────────────

@app.get("/tables/{table_name:path}/lineage", response_model=LineageResponse, tags=["Lineage"])
def get_lineage(
    table_name: str,
    days: int = Query(default=7, ge=1, le=30, description="Lookback window for impact records"),
):
    """Downstream lineage impact for a table — sourced from dq_lineage_impact_audit."""
    rows = run_query(f"""
        SELECT DISTINCT
            downstream_table,
            hop_depth,
            impact_severity,
            downstream_owner
        FROM {_tbl("dq_lineage_impact_audit")}
        WHERE failed_table = '{table_name}'
          AND partition_date >= current_date() - INTERVAL {days} DAYS
        ORDER BY hop_depth ASC, downstream_table ASC
    """)

    nodes = [
        LineageNode(
            downstream_table = r["downstream_table"],
            hop_depth        = int(r["hop_depth"]),
            impact_severity  = r["impact_severity"],
            downstream_owner = r.get("downstream_owner"),
        )
        for r in rows
    ]
    return LineageResponse(
        table_name     = table_name,
        downstream     = nodes,
        total_impacted = len(nodes),
    )


# ── SLA ────────────────────────────────────────────────────────────────────────

@app.get("/tables/{table_name:path}/sla", response_model=SLAResponse, tags=["SLA"])
def get_sla(table_name: str):
    """SLA status and recent breaches for a table."""
    score_rows = run_query(f"""
        SELECT a.dq_score, c.min_quality_pct
        FROM {_tbl("dq_audit")} a
        JOIN {_tbl("dq_config")} c ON a.table_name = c.table_name
        WHERE a.table_name = '{table_name}'
        ORDER BY a.partition_date DESC
        LIMIT 1
    """)

    if not score_rows:
        raise HTTPException(status_code=404, detail=f"No data found for table '{table_name}'")

    r           = score_rows[0]
    dq_score    = float(r["dq_score"])       if r["dq_score"]       is not None else None
    threshold   = float(r["min_quality_pct"]) if r["min_quality_pct"] is not None else None

    if threshold is None:
        status = "NO_SLA"
    elif dq_score is not None and dq_score >= threshold:
        status = "PASS"
    else:
        status = "FAIL"

    breach_rows = run_query(f"""
        SELECT partition_date, dq_score, min_quality_pct, breach_type, sla_owner
        FROM {_tbl("dq_sla_breach_audit")}
        WHERE table_name = '{table_name}'
        ORDER BY partition_date DESC
        LIMIT 10
    """)

    last_breach = date.fromisoformat(breach_rows[0]["partition_date"]) if breach_rows else None

    return SLAResponse(
        table_name = table_name,
        sla = SLAStatus(
            table_name      = table_name,
            dq_score        = dq_score,
            min_quality_pct = threshold,
            status          = status,
            last_breach     = last_breach,
        ),
        recent_breaches = breach_rows,
    )


# ── Validate ───────────────────────────────────────────────────────────────────

@app.post("/tables/{table_name:path}/validate", response_model=ValidationResponse, tags=["Tables"])
def validate_table(table_name: str):
    """Pre-flight config validation for a table (does not run DQ)."""
    rows = run_query(f"""
        SELECT config_id, business_rules, dq_rule_payload, change_flag
        FROM {_tbl("dq_config")}
        WHERE LOWER(table_name) = LOWER('{table_name}')
        LIMIT 1
    """)

    if not rows:
        return ValidationResponse(
            table_name  = table_name,
            valid       = False,
            config_id   = None,
            has_payload = False,
            change_flag = None,
            issues      = [f"No config entry found for '{table_name}'"],
        )

    r       = rows[0]
    issues  = []
    payload = r.get("dq_rule_payload")
    if not r.get("business_rules"):
        issues.append("business_rules is empty — AI rule generation will fail.")
    if not payload:
        issues.append("dq_rule_payload is missing — rules will be regenerated on next run.")
    if r.get("change_flag"):
        issues.append("change_flag=True — rules will be regenerated on next run.")

    return ValidationResponse(
        table_name  = table_name,
        valid       = len(issues) == 0,
        config_id   = int(r["config_id"]) if r.get("config_id") is not None else None,
        has_payload = bool(payload),
        change_flag = bool(r["change_flag"]) if r.get("change_flag") is not None else None,
        issues      = issues,
    )
