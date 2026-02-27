"""Failure scenario registry — injects faults into pipeline data."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd


@dataclass
class FailureScenario:
    name: str
    description: str
    failure_type: str
    inject: Callable[[dict[str, Any]], None]


def _inject_schema_drift_new_column(data: dict[str, Any]) -> None:
    """Add an unexpected column to sales data."""
    df = data.get("sales")
    if df is not None:
        df["loyalty_points"] = (df["unit_price"] * df["quantity"] * 10).astype(int)
        data["sales"] = df


def _inject_schema_drift_type_change(data: dict[str, Any]) -> None:
    """Change unit_price from float to string (simulates upstream type change)."""
    df = data.get("sales")
    if df is not None:
        df["unit_price"] = df["unit_price"].astype(str) + "_USD"
        data["sales"] = df


def _inject_schema_drift_missing_column(data: dict[str, Any]) -> None:
    """Drop required 'region' column."""
    df = data.get("sales")
    if df is not None and "region" in df.columns:
        data["sales"] = df.drop(columns=["region"])


def _inject_null_quality(data: dict[str, Any]) -> None:
    """Introduce ~30% null emails in customer profiles."""
    customers = data.get("customers")
    if customers is not None:
        mask = [random.random() < 0.30 for _ in range(len(customers))]
        customers_copy = customers.copy()
        customers_copy.loc[mask, "email"] = None
        data["customers"] = customers_copy


def _inject_duplicates(data: dict[str, Any]) -> None:
    """Duplicate ~15% of sales rows."""
    df = data.get("sales")
    if df is not None:
        n_dupes = max(1, int(len(df) * 0.15))
        dupe_rows = df.sample(n=n_dupes, random_state=42)
        data["sales"] = pd.concat([df, dupe_rows], ignore_index=True)


def _inject_missing_file(data: dict[str, Any]) -> None:
    """Remove products data to simulate a missing source file."""
    data.pop("products", None)
    data["_missing_source"] = "product_catalog"


def _inject_pk_violation(data: dict[str, Any]) -> None:
    """Force duplicate transaction_ids (PK constraint violation)."""
    df = data.get("sales")
    if df is not None and len(df) > 5:
        df_copy = df.copy()
        # Overwrite some IDs to create duplicates
        df_copy.loc[df_copy.index[-5:], "transaction_id"] = df_copy["transaction_id"].iloc[:5].values
        data["sales"] = df_copy


SCENARIOS: dict[str, FailureScenario] = {
    "healthy": FailureScenario(
        name="healthy",
        description="All pipeline stages run cleanly — no issues injected.",
        failure_type="none",
        inject=lambda data: None,
    ),
    "schema_drift_new_column": FailureScenario(
        name="schema_drift_new_column",
        description="Upstream source adds unexpected 'loyalty_points' column.",
        failure_type="schema_drift",
        inject=_inject_schema_drift_new_column,
    ),
    "schema_drift_type_change": FailureScenario(
        name="schema_drift_type_change",
        description="unit_price field changes from float to string (e.g. '29.99_USD').",
        failure_type="schema_drift",
        inject=_inject_schema_drift_type_change,
    ),
    "schema_drift_missing_column": FailureScenario(
        name="schema_drift_missing_column",
        description="Required 'region' column dropped from source.",
        failure_type="schema_drift",
        inject=_inject_schema_drift_missing_column,
    ),
    "data_quality_nulls": FailureScenario(
        name="data_quality_nulls",
        description="~30% of customer email addresses are null.",
        failure_type="data_quality",
        inject=_inject_null_quality,
    ),
    "data_quality_duplicates": FailureScenario(
        name="data_quality_duplicates",
        description="~15% of sales records are duplicated.",
        failure_type="data_quality",
        inject=_inject_duplicates,
    ),
    "missing_source_file": FailureScenario(
        name="missing_source_file",
        description="Product catalog file is missing from the source system.",
        failure_type="pipeline_failure",
        inject=_inject_missing_file,
    ),
    "pk_constraint_violation": FailureScenario(
        name="pk_constraint_violation",
        description="Duplicate transaction_ids violate PK constraints at load.",
        failure_type="data_quality",
        inject=_inject_pk_violation,
    ),
}
