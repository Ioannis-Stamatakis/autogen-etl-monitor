"""Core data model — pipeline state dataclasses."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    REMEDIATED = "remediated"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class LogEntry:
    timestamp: str
    level: Severity
    stage: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def now(
        cls,
        level: Severity,
        stage: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> "LogEntry":
        return cls(
            timestamp=datetime.utcnow().isoformat(),
            level=level,
            stage=stage,
            message=message,
            details=details or {},
        )


@dataclass
class DataQualityMetrics:
    total_rows: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    duplicate_count: int = 0
    out_of_range_count: int = 0
    schema_violations: list[str] = field(default_factory=list)
    type_mismatches: dict[str, str] = field(default_factory=dict)  # col → found_type


@dataclass
class StageResult:
    stage_name: str
    status: StageStatus = StageStatus.PENDING
    started_at: str | None = None
    finished_at: str | None = None
    rows_in: int = 0
    rows_out: int = 0
    error_message: str | None = None
    quality_metrics: DataQualityMetrics = field(default_factory=DataQualityMetrics)
    metadata: dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        self.status = StageStatus.RUNNING
        self.started_at = datetime.utcnow().isoformat()

    def succeed(self, rows_out: int) -> None:
        self.status = StageStatus.SUCCESS
        self.rows_out = rows_out
        self.finished_at = datetime.utcnow().isoformat()

    def fail(self, error: str) -> None:
        self.status = StageStatus.FAILED
        self.error_message = error
        self.finished_at = datetime.utcnow().isoformat()


@dataclass
class PipelineRun:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    pipeline_name: str = "sales_etl"
    scenario_name: str = "unknown"
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    finished_at: str | None = None
    overall_status: StageStatus = StageStatus.PENDING
    failure_injected: bool = False
    failure_type: str | None = None


@dataclass
class PipelineState:
    """Single source of truth for one ETL pipeline execution."""

    run: PipelineRun = field(default_factory=PipelineRun)
    stages: dict[str, StageResult] = field(default_factory=dict)
    logs: list[LogEntry] = field(default_factory=list)
    source_data: dict[str, Any] = field(default_factory=dict)      # raw DataFrames/dicts
    processed_data: dict[str, Any] = field(default_factory=dict)   # transformed data
    remediation_applied: list[str] = field(default_factory=list)
    incident_report: str | None = None

    def add_log(self, level: Severity, stage: str, message: str, **details: Any) -> None:
        self.logs.append(LogEntry.now(level, stage, message, details))

    def get_stage(self, name: str) -> StageResult:
        if name not in self.stages:
            self.stages[name] = StageResult(stage_name=name)
        return self.stages[name]

    @property
    def has_failures(self) -> bool:
        return any(s.status == StageStatus.FAILED for s in self.stages.values())

    @property
    def error_logs(self) -> list[LogEntry]:
        return [
            log for log in self.logs
            if log.level in (Severity.ERROR, Severity.CRITICAL)
        ]
