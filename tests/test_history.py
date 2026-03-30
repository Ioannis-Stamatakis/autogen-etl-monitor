"""Unit tests for HistoryStore and history tools."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from etl_monitor.pipeline.history import HistoryStore
from etl_monitor.pipeline.simulator import simulate_pipeline
from etl_monitor.tools.history import make_history_tools


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_store(tmp_path: Path) -> HistoryStore:
    return HistoryStore(path=tmp_path / "run_history.json")


def _record(
    scenario: str = "healthy",
    failure_type: str | None = None,
    outcome: str = "HEALTHY",
    offset_hours: int = 0,
    duration: float = 10.0,
    total_rows: int = 200,
    total_nulls: int = 0,
    duplicate_count: int = 0,
) -> dict:
    ts = (datetime.utcnow() - timedelta(hours=offset_hours)).isoformat()
    return {
        "run_id": f"test-{scenario}-{offset_hours}",
        "scenario_name": scenario,
        "failure_type": failure_type,
        "timestamp": ts,
        "outcome": outcome,
        "duration_seconds": duration,
        "remediation_applied": [],
        "failed_stages": [],
        "data_quality": {
            "total_rows": total_rows,
            "total_nulls": total_nulls,
            "duplicate_count": duplicate_count,
            "schema_violations": [],
        },
    }


# ---------------------------------------------------------------------------
# HistoryStore — persistence
# ---------------------------------------------------------------------------

class TestHistoryStorePersistence:
    def test_creates_file_on_save(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record())
        assert (tmp_path / "run_history.json").exists()

    def test_loads_existing_file(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(scenario="healthy"))
        store.append(_record(scenario="schema_drift_type_change"))

        store2 = _make_store(tmp_path)
        assert len(store2.get_recent(limit=10)) == 2

    def test_handles_corrupted_file(self, tmp_path):
        bad_file = tmp_path / "run_history.json"
        bad_file.write_text("not valid json{{{")
        store = HistoryStore(path=bad_file)
        assert store.get_recent() == []


# ---------------------------------------------------------------------------
# HistoryStore — querying
# ---------------------------------------------------------------------------

class TestHistoryStoreQuerying:
    def test_get_recent_returns_all_when_no_filter(self, tmp_path):
        store = _make_store(tmp_path)
        for i in range(5):
            store.append(_record(scenario=f"s{i}"))
        assert len(store.get_recent(limit=10)) == 5

    def test_get_recent_respects_limit(self, tmp_path):
        store = _make_store(tmp_path)
        for i in range(10):
            store.append(_record())
        assert len(store.get_recent(limit=3)) == 3

    def test_get_recent_filters_by_scenario(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(scenario="healthy"))
        store.append(_record(scenario="healthy"))
        store.append(_record(scenario="schema_drift_type_change"))
        result = store.get_recent(scenario_name="healthy", limit=10)
        assert len(result) == 2
        assert all(r["scenario_name"] == "healthy" for r in result)

    def test_get_failure_trend_counts_correctly(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(failure_type="schema_drift", outcome="FAILED"))
        store.append(_record(failure_type="schema_drift", outcome="RESOLVED", duration=30.0))
        store.append(_record(failure_type="data_quality", outcome="FAILED"))
        trend = store.get_failure_trend(window_hours=24)
        assert trend["total_runs"] == 3
        assert trend["failed_count"] == 2
        assert trend["resolved_count"] == 1

    def test_get_failure_trend_filters_by_type(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(failure_type="schema_drift", outcome="FAILED"))
        store.append(_record(failure_type="data_quality", outcome="FAILED"))
        trend = store.get_failure_trend(failure_type="schema_drift", window_hours=24)
        assert trend["total_runs"] == 1

    def test_get_failure_trend_excludes_old_records(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(failure_type="schema_drift", outcome="FAILED", offset_hours=30))
        trend = store.get_failure_trend(window_hours=24)
        assert trend["total_runs"] == 0

    def test_mttr_calculated_from_resolved_runs(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(outcome="RESOLVED", duration=20.0))
        store.append(_record(outcome="RESOLVED", duration=40.0))
        trend = store.get_failure_trend(window_hours=24)
        assert trend["mttr_seconds"] == pytest.approx(30.0)

    def test_mttr_none_when_no_resolved_runs(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(outcome="FAILED"))
        trend = store.get_failure_trend(window_hours=24)
        assert trend["mttr_seconds"] is None

    def test_baseline_metrics_no_data(self, tmp_path):
        store = _make_store(tmp_path)
        result = store.get_baseline_metrics("healthy")
        assert result["sample_size"] == 0

    def test_baseline_metrics_averages(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(scenario="healthy", total_rows=200, total_nulls=0, duplicate_count=0))
        store.append(_record(scenario="healthy", total_rows=200, total_nulls=10, duplicate_count=4))
        baseline = store.get_baseline_metrics("healthy")
        assert baseline["sample_size"] == 2
        assert baseline["avg_null_rate"] == pytest.approx(0.025)   # 5 avg nulls / 200 rows
        assert baseline["avg_duplicate_rate"] == pytest.approx(0.01)  # 2 avg dupes / 200 rows

    def test_is_known_recurring_false_when_few_occurrences(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(scenario="s", failure_type="schema_drift", outcome="FAILED"))
        assert store.is_known_recurring_failure("s", "schema_drift") is False

    def test_is_known_recurring_true_when_enough_occurrences(self, tmp_path):
        store = _make_store(tmp_path)
        for _ in range(3):
            store.append(_record(scenario="s", failure_type="schema_drift", outcome="FAILED"))
        assert store.is_known_recurring_failure("s", "schema_drift") is True

    def test_is_known_recurring_false_for_old_records(self, tmp_path):
        store = _make_store(tmp_path)
        for _ in range(3):
            store.append(_record(scenario="s", failure_type="schema_drift",
                                 outcome="FAILED", offset_hours=30))
        assert store.is_known_recurring_failure("s", "schema_drift", window_hours=24) is False


# ---------------------------------------------------------------------------
# History tools — factory / tool behaviour
# ---------------------------------------------------------------------------

class TestHistoryTools:
    def test_save_run_to_history_persists_record(self, tmp_path):
        store = _make_store(tmp_path)
        state = simulate_pipeline("healthy")
        tools = make_history_tools(state, store)
        save_fn = next(t for t in tools if t.__name__ == "save_run_to_history")
        result = json.loads(save_fn())
        assert result["saved"] is True
        assert result["outcome"] == "HEALTHY"
        assert len(store.get_recent(limit=10)) == 1

    def test_save_run_records_failed_outcome(self, tmp_path):
        store = _make_store(tmp_path)
        state = simulate_pipeline("schema_drift_type_change")
        tools = make_history_tools(state, store)
        save_fn = next(t for t in tools if t.__name__ == "save_run_to_history")
        result = json.loads(save_fn())
        assert result["outcome"] == "FAILED"

    def test_get_run_history_returns_json(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(scenario="healthy"))
        state = simulate_pipeline("healthy")
        tools = make_history_tools(state, store)
        get_fn = next(t for t in tools if t.__name__ == "get_run_history")
        result = json.loads(get_fn(scenario_name="healthy", limit=5))
        assert result["total_returned"] == 1
        assert result["scenario_filter"] == "healthy"

    def test_get_failure_trends_returns_json(self, tmp_path):
        store = _make_store(tmp_path)
        store.append(_record(failure_type="schema_drift", outcome="FAILED"))
        state = simulate_pipeline("healthy")
        tools = make_history_tools(state, store)
        trend_fn = next(t for t in tools if t.__name__ == "get_failure_trends")
        result = json.loads(trend_fn(window_hours=24))
        assert "total_runs" in result
        assert result["total_runs"] == 1

    def test_compare_metrics_no_baseline(self, tmp_path):
        store = _make_store(tmp_path)
        state = simulate_pipeline("healthy")
        tools = make_history_tools(state, store)
        cmp_fn = next(t for t in tools if t.__name__ == "compare_metrics_to_baseline")
        result = json.loads(cmp_fn(scenario_name="healthy"))
        assert result["baseline_available"] is False

    def test_compare_metrics_with_baseline(self, tmp_path):
        store = _make_store(tmp_path)
        # Seed two prior runs with zero nulls/dupes
        store.append(_record(scenario="healthy", total_rows=200, total_nulls=0, duplicate_count=0))
        store.append(_record(scenario="healthy", total_rows=200, total_nulls=0, duplicate_count=0))
        state = simulate_pipeline("healthy")
        tools = make_history_tools(state, store)
        cmp_fn = next(t for t in tools if t.__name__ == "compare_metrics_to_baseline")
        result = json.loads(cmp_fn(scenario_name="healthy"))
        assert result["baseline_available"] is True
        assert "current_null_rate" in result
        assert "anomaly_detected" in result

    def test_check_known_failure_new(self, tmp_path):
        store = _make_store(tmp_path)
        state = simulate_pipeline("schema_drift_type_change")
        tools = make_history_tools(state, store)
        check_fn = next(t for t in tools if t.__name__ == "check_known_failure")
        result = json.loads(check_fn(window_hours=24))
        assert result["is_known_recurring"] is False

    def test_check_known_failure_recurring(self, tmp_path):
        store = _make_store(tmp_path)
        for _ in range(3):
            store.append(_record(
                scenario="schema_drift_type_change",
                failure_type="schema_drift",
                outcome="FAILED",
            ))
        state = simulate_pipeline("schema_drift_type_change")
        # state.run.failure_type is set by simulator; ensure it matches
        state.run.failure_type = "schema_drift"
        tools = make_history_tools(state, store)
        check_fn = next(t for t in tools if t.__name__ == "check_known_failure")
        result = json.loads(check_fn(window_hours=24))
        assert result["is_known_recurring"] is True
        assert result["recent_occurrences"] == 3
