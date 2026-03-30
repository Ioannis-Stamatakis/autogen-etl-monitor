"""Run history store — persists pipeline run results to JSON for trend analysis."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


_DEFAULT_PATH = Path(__file__).parent.parent.parent.parent / "data" / "run_history.json"


class HistoryStore:
    """Reads and writes pipeline run history to a JSON file."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or _DEFAULT_PATH
        self._records: list[dict[str, Any]] = self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> list[dict[str, Any]]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                return []
        return []

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._records, indent=2))

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def append(self, record: dict[str, Any]) -> None:
        self._records.append(record)
        self.save()

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_recent(
        self,
        scenario_name: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        records = self._records
        if scenario_name:
            records = [r for r in records if r.get("scenario_name") == scenario_name]
        return records[-limit:]

    def get_failure_trend(
        self,
        failure_type: str | None = None,
        window_hours: int = 24,
    ) -> dict[str, Any]:
        cutoff = (datetime.utcnow() - timedelta(hours=window_hours)).isoformat()
        records = [
            r for r in self._records
            if r.get("timestamp", "") >= cutoff
        ]
        if failure_type:
            records = [r for r in records if r.get("failure_type") == failure_type]

        total = len(records)
        failed = [r for r in records if r.get("outcome") in ("FAILED", "UNRESOLVED")]
        resolved = [r for r in records if r.get("outcome") == "RESOLVED"]

        # MTTR: mean seconds from run start to resolved
        mttr_seconds: float | None = None
        if resolved:
            durations = [r["duration_seconds"] for r in resolved if r.get("duration_seconds")]
            if durations:
                mttr_seconds = sum(durations) / len(durations)

        # Recurrence: how many times this failure_type appeared
        recurrence_counts: dict[str, int] = {}
        for r in records:
            ft = r.get("failure_type") or "none"
            recurrence_counts[ft] = recurrence_counts.get(ft, 0) + 1

        return {
            "window_hours": window_hours,
            "failure_type_filter": failure_type,
            "total_runs": total,
            "failed_count": len(failed),
            "resolved_count": len(resolved),
            "mttr_seconds": mttr_seconds,
            "recurrence_by_type": recurrence_counts,
        }

    def get_baseline_metrics(self, scenario_name: str) -> dict[str, Any]:
        """Return historical average null_rate and duplicate_rate for a scenario."""
        records = [
            r for r in self._records
            if r.get("scenario_name") == scenario_name
            and r.get("data_quality") is not None
        ]
        if not records:
            return {"scenario_name": scenario_name, "sample_size": 0}

        total_rows_list = [r["data_quality"].get("total_rows", 0) for r in records]
        null_counts_list = [r["data_quality"].get("total_nulls", 0) for r in records]
        dupe_counts_list = [r["data_quality"].get("duplicate_count", 0) for r in records]

        def _safe_avg(nums: list[int]) -> float:
            return sum(nums) / len(nums) if nums else 0.0

        avg_total = _safe_avg(total_rows_list)
        avg_nulls = _safe_avg(null_counts_list)
        avg_dupes = _safe_avg(dupe_counts_list)

        return {
            "scenario_name": scenario_name,
            "sample_size": len(records),
            "avg_total_rows": avg_total,
            "avg_null_rate": avg_nulls / avg_total if avg_total else 0.0,
            "avg_duplicate_rate": avg_dupes / avg_total if avg_total else 0.0,
        }

    def is_known_recurring_failure(
        self,
        scenario_name: str,
        failure_type: str | None,
        window_hours: int = 24,
        min_occurrences: int = 2,
    ) -> bool:
        """Return True if this scenario+failure_type has failed recently enough to be 'known'."""
        cutoff = (datetime.utcnow() - timedelta(hours=window_hours)).isoformat()
        matches = [
            r for r in self._records
            if r.get("scenario_name") == scenario_name
            and r.get("failure_type") == failure_type
            and r.get("outcome") in ("FAILED", "UNRESOLVED", "RESOLVED")
            and r.get("timestamp", "") >= cutoff
        ]
        return len(matches) >= min_occurrences
