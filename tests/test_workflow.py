"""
Integration test — runs the full agent workflow with the Gemini API.

Requires GEMINI_API_KEY environment variable to be set.
Skip with: uv run pytest tests/test_workflow.py -k "not integration"
"""

import os

import pytest

from etl_monitor.config import get_model_client
from etl_monitor.pipeline.simulator import simulate_pipeline
from etl_monitor.pipeline.state import StageStatus
from etl_monitor.team.workflow import build_etl_team

pytestmark = pytest.mark.asyncio

_HAS_API_KEY = bool(os.environ.get("GEMINI_API_KEY"))


@pytest.mark.skipif(not _HAS_API_KEY, reason="GEMINI_API_KEY not set")
async def test_healthy_scenario_skips_to_reporter():
    """Monitor should detect no issues and route directly to Reporter."""
    state = simulate_pipeline("healthy")
    model_client = get_model_client()
    team = build_etl_team(model_client, state)

    agents_seen = []
    task = (
        "Pipeline run completed. Run ID: test-healthy. "
        "Overall status: success. Please investigate and produce a report."
    )

    async for msg in team.run_stream(task=task):
        source = getattr(msg, "source", None)
        if source and source not in agents_seen:
            agents_seen.append(source)

    await model_client.close()

    # Diagnostician and Remediation should be skipped
    assert "reporter_agent" in agents_seen
    assert "diagnostician_agent" not in agents_seen
    assert "remediation_agent" not in agents_seen
    assert state.incident_report is not None


@pytest.mark.skipif(not _HAS_API_KEY, reason="GEMINI_API_KEY not set")
async def test_failed_scenario_activates_all_agents():
    """Schema drift should activate all 4 agents."""
    state = simulate_pipeline("schema_drift_type_change")
    model_client = get_model_client()
    team = build_etl_team(model_client, state)

    agents_seen = []
    task = (
        f"Pipeline run completed. Run ID: {state.run.run_id}. "
        f"Overall status: {state.run.overall_status.value}. "
        "Please investigate, diagnose, remediate if possible, and produce a report."
    )

    async for msg in team.run_stream(task=task):
        source = getattr(msg, "source", None)
        if source and source not in agents_seen:
            agents_seen.append(source)

    await model_client.close()

    assert "monitor_agent" in agents_seen
    assert "diagnostician_agent" in agents_seen
    assert "remediation_agent" in agents_seen
    assert "reporter_agent" in agents_seen


@pytest.mark.skipif(not _HAS_API_KEY, reason="GEMINI_API_KEY not set")
async def test_reporter_produces_incident_report():
    """Reporter should always call format_incident_report."""
    state = simulate_pipeline("data_quality_nulls")
    model_client = get_model_client()
    team = build_etl_team(model_client, state)

    task = (
        f"Pipeline run completed. Run ID: {state.run.run_id}. "
        "Please investigate and produce a complete incident report."
    )

    async for _ in team.run_stream(task=task):
        pass

    await model_client.close()

    assert state.incident_report is not None
    assert "ETL PIPELINE INCIDENT REPORT" in state.incident_report
