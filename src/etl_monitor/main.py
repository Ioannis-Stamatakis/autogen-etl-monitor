"""
Entry point — runs ETL failure scenarios through the multi-agent team.

Usage:
    uv run python -m etl_monitor.main                        # runs all scenarios
    uv run python -m etl_monitor.main healthy                # single scenario
    uv run python -m etl_monitor.main schema_drift_type_change
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

from autogen_agentchat.messages import ToolCallExecutionEvent, ToolCallRequestEvent

from .config import get_model_client
from .display.console import (
    console,
    print_agent_message,
    print_final_summary,
    print_history_summary,
    print_incident_report,
    print_scenario_header,
    print_startup_banner,
)
from .pipeline.failures import SCENARIOS
from .pipeline.history import HistoryStore
from .pipeline.simulator import simulate_pipeline
from .team.workflow import build_etl_team
from .tools.history import make_history_tools

ALL_SCENARIOS = [
    "healthy",
    "schema_drift_type_change",
    "data_quality_nulls",
    "data_quality_duplicates",
    "schema_drift_missing_column",
    "pk_constraint_violation",
]

# Seconds between scenarios — generous to avoid 429s on the free tier
_SCENARIO_PAUSE = 60


def _is_rate_limit(exc: Exception) -> bool:
    msg = str(exc)
    return "429" in msg or "RESOURCE_EXHAUSTED" in msg or "rate limit" in msg.lower()


async def _stream_team(team, task_msg: str, state, agents_activated: list[str]) -> None:
    async for msg in team.run_stream(task=task_msg):
        source = getattr(msg, "source", None)
        content = getattr(msg, "content", None)

        if isinstance(msg, (ToolCallExecutionEvent, ToolCallRequestEvent)):
            continue

        if source and content and isinstance(content, str) and content.strip():
            if source not in agents_activated:
                agents_activated.append(source)
            print_agent_message(source, content)

            if source == "reporter_agent" and state.incident_report:
                print_incident_report(state.incident_report)


async def run_scenario(scenario_name: str, store: HistoryStore) -> dict[str, Any]:
    scenario = SCENARIOS.get(scenario_name, SCENARIOS["healthy"])
    state = simulate_pipeline(scenario_name)

    print_scenario_header(
        scenario_name=scenario_name,
        description=scenario.description,
        run_id=state.run.run_id,
    )

    agents_activated: list[str] = []
    task_msg = (
        f"A new ETL pipeline run has completed for scenario '{scenario_name}'. "
        f"Run ID: {state.run.run_id}. "
        f"Overall status: {state.run.overall_status.value}. "
        "Please investigate this pipeline run, diagnose any issues, apply remediation "
        "if needed, and produce a full incident report."
    )

    max_retries = 5
    for attempt in range(max_retries):
        model_client = get_model_client()
        team = build_etl_team(model_client, state, store)
        try:
            await _stream_team(team, task_msg, state, agents_activated)
            await model_client.close()
            break
        except Exception as exc:
            await model_client.close()
            if _is_rate_limit(exc) and attempt < max_retries - 1:
                wait = 60 * (attempt + 1)
                console.print(
                    f"  [bold yellow]Rate limit (attempt {attempt + 1}/{max_retries}). "
                    f"Waiting {wait}s...[/]"
                )
                await asyncio.sleep(wait)
                agents_activated.clear()
            else:
                raise

    # Persist run to history
    history_tools = make_history_tools(state, store)
    save_fn = next(t for t in history_tools if t.__name__ == "save_run_to_history")
    save_fn()

    outcome = "HEALTHY"
    if state.remediation_applied:
        outcome = "UNRESOLVED" if state.has_failures else "RESOLVED"
    elif state.has_failures:
        outcome = "FAILED"

    return {
        "scenario": scenario_name,
        "outcome": outcome,
        "agents_activated": agents_activated,
        "run_id": state.run.run_id,
    }


async def main() -> None:
    print_startup_banner()

    store = HistoryStore()

    # Allow passing a single scenario name as CLI argument
    scenarios = sys.argv[1:] if len(sys.argv) > 1 else ALL_SCENARIOS

    # Validate
    invalid = [s for s in scenarios if s not in SCENARIOS]
    if invalid:
        console.print(f"[bold red]Unknown scenario(s): {invalid}[/]")
        console.print(f"Available: {list(SCENARIOS.keys())}")
        return

    results = []
    for i, scenario_name in enumerate(scenarios):
        if i > 0:
            console.print(
                f"  [dim]Waiting {_SCENARIO_PAUSE}s before next scenario...[/]\n"
            )
            await asyncio.sleep(_SCENARIO_PAUSE)

        try:
            result = await run_scenario(scenario_name, store)
            results.append(result)
        except Exception as exc:
            console.print(f"[bold red]ERROR in scenario '{scenario_name}': {exc}[/]")
            results.append({
                "scenario": scenario_name,
                "outcome": "ERROR",
                "agents_activated": [],
            })

    print_final_summary(results)
    print_history_summary(store)


if __name__ == "__main__":
    asyncio.run(main())
