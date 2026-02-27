"""ETL pipeline simulator — orchestrates runs with optional failure injection."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .failures import SCENARIOS, FailureScenario
from .stages import run_ingestion, run_load, run_transform
from .state import PipelineRun, PipelineState, Severity, StageStatus

_DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"


def _load_raw_data() -> dict[str, Any]:
    """Load all source data files into a dict of DataFrames."""
    sales = pd.read_csv(_DATA_DIR / "sales_transactions.csv")
    with open(_DATA_DIR / "customer_profiles.json") as f:
        customers = pd.DataFrame(json.load(f))
    products = pd.read_csv(_DATA_DIR / "product_catalog.csv")
    return {"sales": sales, "customers": customers, "products": products}


def simulate_pipeline(scenario_name: str = "healthy") -> PipelineState:
    """
    Run a full ETL pipeline simulation for the given scenario.

    Returns a fully-populated PipelineState reflecting the run outcome.
    """
    scenario: FailureScenario = SCENARIOS.get(scenario_name, SCENARIOS["healthy"])

    # Build fresh state
    state = PipelineState(
        run=PipelineRun(
            pipeline_name="sales_etl",
            scenario_name=scenario_name,
            failure_injected=scenario.failure_type != "none",
            failure_type=scenario.failure_type if scenario.failure_type != "none" else None,
        )
    )

    state.add_log(
        Severity.INFO, "simulator",
        f"Starting pipeline run for scenario: '{scenario_name}'",
        scenario_description=scenario.description,
    )

    # Load raw data then inject failures
    raw_data = _load_raw_data()
    scenario.inject(raw_data)

    # Run stages
    run_ingestion(state, raw_data)
    run_transform(state)
    run_load(state)

    # Determine overall status
    if any(s.status == StageStatus.FAILED for s in state.stages.values()):
        state.run.overall_status = StageStatus.FAILED
        state.add_log(
            Severity.ERROR, "simulator",
            f"Pipeline run FAILED for scenario: '{scenario_name}'",
        )
    else:
        state.run.overall_status = StageStatus.SUCCESS
        state.add_log(
            Severity.INFO, "simulator",
            f"Pipeline run SUCCEEDED for scenario: '{scenario_name}'",
        )

    state.run.finished_at = datetime.utcnow().isoformat()
    return state
