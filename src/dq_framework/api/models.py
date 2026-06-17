"""Pydantic response models for the DQ Framework REST API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    version: str
    catalog: str
    schema_name: str


class TableSummary(BaseModel):
    table_name:     str
    latest_score:   float | None
    last_run_date:  date  | None
    sla_threshold:  float | None
    sla_status:     str          # PASS / FAIL / NO_SLA


class TableScoreResponse(BaseModel):
    table_name:    str
    dq_score:      float | None
    table_count:   int   | None
    partition_date: date | None
    run_timestamp: datetime | None


class ScoreHistoryPoint(BaseModel):
    partition_date: date
    dq_score:       float
    table_count:    int | None


class ScoreHistoryResponse(BaseModel):
    table_name: str
    history:    list[ScoreHistoryPoint]


class AlertRow(BaseModel):
    table_name:       str
    alert_type:       str
    column_name:      str | None
    metric_value:     float | None
    previous_value:   float | None
    change_pct:       float | None
    is_drastic:       bool | None
    partition_date:   date


class AlertsResponse(BaseModel):
    table_name: str
    alerts:     list[AlertRow]


class LineageNode(BaseModel):
    downstream_table: str
    hop_depth:        int
    impact_severity:  str
    downstream_owner: str | None


class LineageResponse(BaseModel):
    table_name:  str
    downstream:  list[LineageNode]
    total_impacted: int


class SLAStatus(BaseModel):
    table_name:     str
    dq_score:       float | None
    min_quality_pct: float | None
    status:         str       # PASS / FAIL / NO_SLA
    last_breach:    date | None


class SLAResponse(BaseModel):
    table_name: str
    sla:        SLAStatus
    recent_breaches: list[dict[str, Any]]


class ValidationResponse(BaseModel):
    table_name:  str
    valid:       bool
    config_id:   int  | None
    has_payload: bool
    change_flag: bool | None
    issues:      list[str]


class TablesListResponse(BaseModel):
    total:  int
    tables: list[TableSummary]
