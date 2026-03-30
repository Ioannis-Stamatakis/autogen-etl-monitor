"""History tools — Watchdog Agent queries and saves run history."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from ..pipeline.history import HistoryStore
from ..pipeline.state import PipelineState, StageStatus


def make_history_tools(state: PipelineState, store: HistoryStore) -> list[Any]:
    """Factory: bind history tools to a specific PipelineState and HistoryStore."""

    def save_run_to_history() -> str:
        """
        Persist the completed pipeline run to the history store.
        Call this after the agent team finishes to record the outcome.
        """
        ingest = state.stages.get("ingestion")
        total_rows = ingest.quality_metrics.total_rows if ingest else 0
        total_nulls = (
            sum(ingest.quality_metrics.null_counts.values()) if ingest else 0
        )
        duplicate_count = ingest.quality_metrics.duplicate_count if ingest else 0

        # Compute duration
        duration: float | None = None
        if state.run.started_at and state.run.finished_at:
            try:
                start = datetime.fromisoformat(state.run.started_at)
                end = datetime.fromisoformat(state.run.finished_at)
                duration = (end - start).total_seconds()
            except ValueError:
                pass

        failed_stages = [
            name for name, s in state.stages.items()
            if s.status == StageStatus.FAILED
        ]

        outcome = "HEALTHY"
        if state.remediation_applied:
            outcome = "UNRESOLVED" if state.has_failures else "RESOLVED"
        elif state.has_failures:
            outcome = "FAILED"

        record: dict[str, Any] = {
            "run_id": state.run.run_id,
            "scenario_name": state.run.scenario_name,
            "failure_type": state.run.failure_type,
            "timestamp": state.run.started_at,
            "outcome": outcome,
            "duration_seconds": duration,
            "remediation_applied": state.remediation_applied,
            "failed_stages": failed_stages,
            "data_quality": {
                "total_rows": total_rows,
                "total_nulls": total_nulls,
                "duplicate_count": duplicate_count,
                "schema_violations": (
                    ingest.quality_metrics.schema_violations if ingest else []
                ),
            },
        }
        store.append(record)
        return json.dumps({"saved": True, "run_id": state.run.run_id, "outcome": outcome})

    def get_run_history(scenario_name: str = "", limit: int = 10) -> str:
        """
        Return recent pipeline run history.
        Optionally filter by scenario_name; limit controls how many records to return.
        """
        records = store.get_recent(
            scenario_name=scenario_name or None,
            limit=limit,
        )
        return json.dumps({
            "total_returned": len(records),
            "scenario_filter": scenario_name or "all",
            "records": records,
        }, indent=2)

    def get_failure_trends(failure_type: str = "", window_hours: int = 24) -> str:
        """
        Analyze failure recurrence and MTTR within the last window_hours.
        Optionally filter by failure_type (e.g. 'schema_drift', 'data_quality').
        """
        trend = store.get_failure_trend(
            failure_type=failure_type or None,
            window_hours=window_hours,
        )
        return json.dumps(trend, indent=2)

    def compare_metrics_to_baseline(scenario_name: str) -> str:
        """
        Compare the current run's data quality metrics against the historical
        average for this scenario. Returns deviation from baseline.
        """
        baseline = store.get_baseline_metrics(scenario_name)

        ingest = state.stages.get("ingestion")
        if not ingest or not baseline.get("sample_size"):
            return json.dumps({
                "scenario_name": scenario_name,
                "baseline_available": False,
                "message": "No historical baseline available yet.",
            })

        total_rows = ingest.quality_metrics.total_rows or 1
        current_null_rate = sum(ingest.quality_metrics.null_counts.values()) / total_rows
        current_dupe_rate = ingest.quality_metrics.duplicate_count / total_rows

        null_delta = current_null_rate - baseline["avg_null_rate"]
        dupe_delta = current_dupe_rate - baseline["avg_duplicate_rate"]

        return json.dumps({
            "scenario_name": scenario_name,
            "baseline_available": True,
            "sample_size": baseline["sample_size"],
            "current_null_rate": round(current_null_rate, 4),
            "baseline_null_rate": round(baseline["avg_null_rate"], 4),
            "null_rate_delta": round(null_delta, 4),
            "current_duplicate_rate": round(current_dupe_rate, 4),
            "baseline_duplicate_rate": round(baseline["avg_duplicate_rate"], 4),
            "duplicate_rate_delta": round(dupe_delta, 4),
            "anomaly_detected": abs(null_delta) > 0.05 or abs(dupe_delta) > 0.05,
        }, indent=2)

    def check_known_failure(window_hours: int = 24) -> str:
        """
        Check whether the current scenario and failure type have been seen
        recently (within window_hours). Returns is_known and recurrence count.
        """
        cutoff_runs = store.get_recent(
            scenario_name=state.run.scenario_name,
            limit=100,
        )
        from datetime import timedelta
        cutoff = (datetime.utcnow() - timedelta(hours=window_hours)).isoformat()
        recent_failures = [
            r for r in cutoff_runs
            if r.get("failure_type") == state.run.failure_type
            and r.get("outcome") in ("FAILED", "UNRESOLVED", "RESOLVED")
            and r.get("timestamp", "") >= cutoff
        ]
        is_known = len(recent_failures) >= 2
        return json.dumps({
            "scenario_name": state.run.scenario_name,
            "failure_type": state.run.failure_type,
            "recent_occurrences": len(recent_failures),
            "is_known_recurring": is_known,
            "window_hours": window_hours,
        }, indent=2)

    return [
        save_run_to_history,
        get_run_history,
        get_failure_trends,
        compare_metrics_to_baseline,
        check_known_failure,
    ]
