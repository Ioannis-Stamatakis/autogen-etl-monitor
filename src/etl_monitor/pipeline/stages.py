"""ETL pipeline stage functions — ingestion, transform, load."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .state import DataQualityMetrics, PipelineState, Severity, StageStatus

_DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"

# ---------------------------------------------------------------------------
# Exchange rates (static for simulation)
# ---------------------------------------------------------------------------
_FX_RATES = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "CAD": 0.74, "AUD": 0.65}


def run_ingestion(state: PipelineState, source_data: dict[str, Any]) -> None:
    """
    Ingestion stage: load CSV/JSON files into DataFrames, validate schema,
    record quality metrics. Fails fast on missing required source.
    """
    stage = state.get_stage("ingestion")
    stage.start()
    stage.rows_in = 0

    try:
        # --- Sales transactions ---
        sales = source_data.get("sales")
        if sales is None:
            sales = pd.read_csv(_DATA_DIR / "sales_transactions.csv")

        missing_source = source_data.get("_missing_source")
        if missing_source:
            raise FileNotFoundError(
                f"Source file for '{missing_source}' not found in data lake."
            )

        # --- Customer profiles ---
        customers = source_data.get("customers")
        if customers is None:
            with open(_DATA_DIR / "customer_profiles.json") as f:
                customers = pd.DataFrame(json.load(f))

        # --- Products ---
        products = source_data.get("products")
        if products is None:
            products = pd.read_csv(_DATA_DIR / "product_catalog.csv")

        total_rows = len(sales) + len(customers) + len(products)
        stage.rows_in = total_rows

        # --- Schema validation for sales ---
        expected_cols = {
            "transaction_id", "customer_id", "product_id", "quantity",
            "unit_price", "currency", "transaction_date", "region", "status",
        }
        actual_cols = set(sales.columns)
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols

        metrics = DataQualityMetrics(total_rows=len(sales))

        if missing_cols:
            for col in missing_cols:
                metrics.schema_violations.append(f"Missing required column: '{col}'")
            state.add_log(
                Severity.ERROR, "ingestion",
                f"Schema validation failed: missing columns {missing_cols}",
                missing_columns=list(missing_cols),
            )

        if extra_cols:
            state.add_log(
                Severity.WARNING, "ingestion",
                f"Unexpected columns detected: {extra_cols}",
                extra_columns=list(extra_cols),
            )

        # --- Type validation for unit_price ---
        if "unit_price" in sales.columns:
            # pandas 3+ uses StringDtype ('str'), pandas 2 uses object
            price_dtype = sales["unit_price"].dtype
            is_string_type = price_dtype == object or str(price_dtype) in ("string", "str", "StringDtype")
            if is_string_type:
                metrics.type_mismatches["unit_price"] = f"expected float, got {price_dtype}"
                state.add_log(
                    Severity.ERROR, "ingestion",
                    "Type mismatch: 'unit_price' expected float, got string",
                    column="unit_price",
                    sample_values=sales["unit_price"].head(3).tolist(),
                )

        # --- Null checks ---
        for col in sales.columns:
            null_count = int(sales[col].isna().sum())
            if null_count > 0:
                metrics.null_counts[col] = null_count

        # --- Customer email nulls ---
        if "email" in customers.columns:
            email_nulls = int(customers["email"].isna().sum())
            if email_nulls > 0:
                metrics.null_counts["customer.email"] = email_nulls
                pct = email_nulls / len(customers) * 100
                state.add_log(
                    Severity.WARNING, "ingestion",
                    f"Null emails in customer_profiles: {email_nulls}/{len(customers)} ({pct:.1f}%)",
                    null_count=email_nulls,
                    percentage=round(pct, 1),
                )

        # --- Duplicate check ---
        if "transaction_id" in sales.columns:
            dupe_count = int(sales.duplicated(subset=["transaction_id"]).sum())
            if dupe_count > 0:
                metrics.duplicate_count = dupe_count
                state.add_log(
                    Severity.WARNING, "ingestion",
                    f"Duplicate transaction_ids detected: {dupe_count} rows",
                    duplicate_count=dupe_count,
                )

        stage.quality_metrics = metrics

        # Store raw data for transform stage
        state.source_data.update({
            "sales": sales,
            "customers": customers,
            "products": products,
        })

        if metrics.schema_violations or metrics.type_mismatches:
            stage.fail("Schema validation errors — see logs for details")
        else:
            stage.succeed(total_rows)
            state.add_log(Severity.INFO, "ingestion",
                          f"Ingestion complete: {total_rows} rows loaded",
                          sales_rows=len(sales),
                          customer_rows=len(customers),
                          product_rows=len(products))

    except FileNotFoundError as exc:
        stage.fail(str(exc))
        state.add_log(Severity.CRITICAL, "ingestion", str(exc))
    except Exception as exc:
        stage.fail(f"Unexpected error: {exc}")
        state.add_log(Severity.ERROR, "ingestion", f"Unexpected ingestion error: {exc}")


def run_transform(state: PipelineState) -> None:
    """
    Transform stage: currency conversion, customer enrichment, aggregation.
    Depends on ingestion succeeding.
    """
    stage = state.get_stage("transform")

    ingest_stage = state.stages.get("ingestion")
    if ingest_stage and ingest_stage.status == StageStatus.FAILED:
        stage.status = StageStatus.SKIPPED
        state.add_log(Severity.WARNING, "transform",
                      "Transform skipped — ingestion stage failed")
        return

    stage.start()

    try:
        sales: pd.DataFrame = state.source_data["sales"].copy()
        customers: pd.DataFrame = state.source_data["customers"]

        stage.rows_in = len(sales)

        # --- Currency conversion to USD ---
        if "unit_price" in sales.columns and sales["unit_price"].dtype != object:
            def convert_to_usd(row: pd.Series) -> float:
                rate = _FX_RATES.get(row.get("currency", "USD"), 1.0)
                return float(row["unit_price"]) * rate

            sales["unit_price_usd"] = sales.apply(convert_to_usd, axis=1)
            sales["revenue_usd"] = sales["unit_price_usd"] * sales["quantity"].astype(float)
        else:
            state.add_log(Severity.ERROR, "transform",
                          "Cannot compute revenue: unit_price has wrong type",
                          column_dtype=str(sales["unit_price"].dtype) if "unit_price" in sales.columns else "missing")
            stage.fail("Revenue computation failed — unit_price type invalid")
            return

        # --- Customer enrichment ---
        cust_lookup = customers.set_index("customer_id")[["name", "tier", "country"]].to_dict("index")
        sales["customer_name"] = sales["customer_id"].map(
            lambda cid: cust_lookup.get(cid, {}).get("name", "Unknown")
        )
        sales["customer_tier"] = sales["customer_id"].map(
            lambda cid: cust_lookup.get(cid, {}).get("tier", "unknown")
        )

        # --- Daily aggregation ---
        if "transaction_date" in sales.columns:
            daily_agg = (
                sales.groupby("transaction_date")
                .agg(
                    total_revenue=("revenue_usd", "sum"),
                    transaction_count=("transaction_id", "nunique"),
                    avg_order_value=("revenue_usd", "mean"),
                )
                .reset_index()
            )
        else:
            daily_agg = pd.DataFrame()

        state.processed_data.update({
            "sales_enriched": sales,
            "daily_aggregation": daily_agg,
        })

        stage.succeed(len(sales))
        state.add_log(Severity.INFO, "transform",
                      f"Transform complete: {len(sales)} records enriched",
                      daily_agg_rows=len(daily_agg))

    except KeyError as exc:
        stage.fail(f"Missing data for transform: {exc}")
        state.add_log(Severity.ERROR, "transform", f"Transform KeyError: {exc}")
    except Exception as exc:
        stage.fail(f"Transform error: {exc}")
        state.add_log(Severity.ERROR, "transform", f"Unexpected transform error: {exc}")


def run_load(state: PipelineState) -> None:
    """
    Load stage: write to simulated target, check PK constraints.
    """
    stage = state.get_stage("load")

    transform_stage = state.stages.get("transform")
    if transform_stage and transform_stage.status in (StageStatus.FAILED, StageStatus.SKIPPED):
        stage.status = StageStatus.SKIPPED
        state.add_log(Severity.WARNING, "load",
                      "Load skipped — transform stage did not succeed")
        return

    stage.start()

    try:
        sales_enriched = state.processed_data.get("sales_enriched")
        if sales_enriched is None:
            raise ValueError("No enriched sales data available for loading")

        stage.rows_in = len(sales_enriched)

        # --- PK constraint check ---
        if "transaction_id" in sales_enriched.columns:
            dupes = sales_enriched.duplicated(subset=["transaction_id"])
            dupe_count = int(dupes.sum())
            if dupe_count > 0:
                raise ValueError(
                    f"PK constraint violation: {dupe_count} duplicate transaction_ids "
                    f"(e.g. {sales_enriched.loc[dupes, 'transaction_id'].head(3).tolist()})"
                )

        # --- Simulate write to target (no actual I/O) ---
        state.processed_data["loaded_count"] = len(sales_enriched)
        state.processed_data["load_target"] = "sales_fact_table"

        stage.succeed(len(sales_enriched))
        state.add_log(Severity.INFO, "load",
                      f"Load complete: {len(sales_enriched)} rows written to sales_fact_table")

    except ValueError as exc:
        stage.fail(str(exc))
        state.add_log(Severity.ERROR, "load", str(exc))
    except Exception as exc:
        stage.fail(f"Load error: {exc}")
        state.add_log(Severity.ERROR, "load", f"Unexpected load error: {exc}")
