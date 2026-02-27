"""Remediation tools — Remediation Agent applies fixes to pipeline state."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from ..pipeline.stages import run_load, run_transform
from ..pipeline.state import PipelineState, Severity, StageStatus


def make_remediation_tools(state: PipelineState) -> list[Any]:
    """Factory: bind remediation tools to a specific PipelineState instance."""

    def apply_schema_fix(fix_type: str) -> str:
        """
        Apply a schema fix to correct schema drift issues.

        Args:
            fix_type: One of:
                - 'drop_extra_columns' — remove unexpected columns
                - 'fix_type_cast' — cast unit_price back to float
                - 'add_missing_column_default' — add missing column with default
        """
        sales = state.source_data.get("sales")
        if sales is None:
            return json.dumps({"success": False, "error": "No sales data in state"})

        expected_cols = {
            "transaction_id", "customer_id", "product_id", "quantity",
            "unit_price", "currency", "transaction_date", "region", "status",
        }

        applied = []

        if fix_type == "drop_extra_columns":
            extra = [c for c in sales.columns if c not in expected_cols]
            if extra:
                state.source_data["sales"] = sales.drop(columns=extra)
                applied.append(f"Dropped extra columns: {extra}")
            else:
                applied.append("No extra columns to drop")

        elif fix_type == "fix_type_cast":
            price_dtype = sales["unit_price"].dtype if "unit_price" in sales.columns else None
            is_string = price_dtype is not None and (price_dtype == object or str(price_dtype) in ("string", "str", "StringDtype"))
            if "unit_price" in sales.columns and is_string:
                # Strip non-numeric suffix and cast to float
                state.source_data["sales"] = sales.copy()
                state.source_data["sales"]["unit_price"] = (
                    pd.to_numeric(
                        sales["unit_price"].str.extract(r"([\d.]+)", expand=False),
                        errors="coerce",
                    )
                )
                applied.append("Cast unit_price from string to float")
            else:
                applied.append("unit_price already numeric or not present")

        elif fix_type == "add_missing_column_default":
            missing = [c for c in expected_cols if c not in sales.columns]
            if missing:
                fixed = sales.copy()
                for col in missing:
                    fixed[col] = "UNKNOWN"
                state.source_data["sales"] = fixed
                applied.append(f"Added missing columns with default 'UNKNOWN': {missing}")
            else:
                applied.append("No missing columns")

        else:
            return json.dumps({
                "success": False,
                "error": f"Unknown fix_type: '{fix_type}'. "
                         "Use 'drop_extra_columns', 'fix_type_cast', or 'add_missing_column_default'.",
            })

        # Mark ingestion as remediated
        ingest = state.stages.get("ingestion")
        if ingest:
            ingest.status = StageStatus.REMEDIATED
            ingest.error_message = None

        remedy = f"apply_schema_fix({fix_type})"
        if remedy not in state.remediation_applied:
            state.remediation_applied.append(remedy)

        state.add_log(Severity.INFO, "remediation",
                      f"Schema fix applied: {fix_type}",
                      actions=applied)

        return json.dumps({
            "success": True,
            "fix_type": fix_type,
            "actions_taken": applied,
            "timestamp": datetime.utcnow().isoformat(),
        })

    def deduplicate_records(dataset: str = "sales") -> str:
        """
        Remove duplicate records from the specified dataset.

        Args:
            dataset: 'sales' or 'customers'.
        """
        if dataset == "sales":
            df = state.source_data.get("sales")
            if df is None:
                return json.dumps({"success": False, "error": "No sales data"})

            original_count = len(df)
            key_col = "transaction_id" if "transaction_id" in df.columns else None

            if key_col:
                deduped = df.drop_duplicates(subset=[key_col], keep="first")
            else:
                deduped = df.drop_duplicates(keep="first")

            removed = original_count - len(deduped)
            state.source_data["sales"] = deduped
            label = "sales"

        elif dataset == "customers":
            df = state.source_data.get("customers")
            if df is None:
                return json.dumps({"success": False, "error": "No customer data"})

            original_count = len(df)
            deduped = df.drop_duplicates(subset=["customer_id"], keep="first")
            removed = original_count - len(deduped)
            state.source_data["customers"] = deduped
            label = "customers"

        else:
            return json.dumps({"success": False, "error": f"Unknown dataset: '{dataset}'"})

        remedy = f"deduplicate_records({dataset})"
        if remedy not in state.remediation_applied:
            state.remediation_applied.append(remedy)

        state.add_log(Severity.INFO, "remediation",
                      f"Deduplication applied to {label}: removed {removed} duplicate rows",
                      dataset=label, rows_removed=removed)

        return json.dumps({
            "success": True,
            "dataset": label,
            "rows_before": original_count,
            "rows_after": len(deduped),
            "rows_removed": removed,
        })

    def fill_null_defaults(dataset: str, column: str, default_value: str = "UNKNOWN") -> str:
        """
        Fill null values in a specific column with a default value.

        Args:
            dataset: 'sales' or 'customers'.
            column: Column name to fill nulls in.
            default_value: The value to use for nulls (default: 'UNKNOWN').
        """
        if dataset == "sales":
            df = state.source_data.get("sales")
        elif dataset == "customers":
            df = state.source_data.get("customers")
        else:
            return json.dumps({"success": False, "error": f"Unknown dataset: '{dataset}'"})

        if df is None:
            return json.dumps({"success": False, "error": f"No data for '{dataset}'"})

        if column not in df.columns:
            return json.dumps({
                "success": False,
                "error": f"Column '{column}' not found in {dataset}",
                "available_columns": list(df.columns),
            })

        null_before = int(df[column].isna().sum())
        df_fixed = df.copy()
        df_fixed[column] = df_fixed[column].fillna(default_value)
        null_after = int(df_fixed[column].isna().sum())

        if dataset == "sales":
            state.source_data["sales"] = df_fixed
        else:
            state.source_data["customers"] = df_fixed

        remedy = f"fill_null_defaults({dataset}.{column})"
        if remedy not in state.remediation_applied:
            state.remediation_applied.append(remedy)

        state.add_log(Severity.INFO, "remediation",
                      f"Filled {null_before} nulls in {dataset}.{column} with '{default_value}'",
                      column=column, null_before=null_before, null_after=null_after)

        return json.dumps({
            "success": True,
            "dataset": dataset,
            "column": column,
            "default_value": default_value,
            "nulls_before": null_before,
            "nulls_after": null_after,
            "rows_fixed": null_before - null_after,
        })

    def rerun_stage(stage_name: str) -> str:
        """
        Re-execute a failed pipeline stage after remediation has been applied.

        Args:
            stage_name: One of 'ingestion', 'transform', or 'load'.
        """
        valid_stages = {"ingestion", "transform", "load"}
        if stage_name not in valid_stages:
            return json.dumps({
                "success": False,
                "error": f"Invalid stage '{stage_name}'. Must be one of: {valid_stages}",
            })

        state.add_log(Severity.INFO, "remediation",
                      f"Re-running stage: {stage_name}")

        try:
            if stage_name == "ingestion":
                from ..pipeline.stages import run_ingestion
                # Use current (remediated) source_data
                run_ingestion(state, state.source_data)

            elif stage_name == "transform":
                run_transform(state)

            elif stage_name == "load":
                run_load(state)

            stage = state.stages.get(stage_name)
            new_status = stage.status.value if stage else "unknown"
            success = new_status in ("success", "remediated")

            remedy = f"rerun_stage({stage_name})"
            if remedy not in state.remediation_applied:
                state.remediation_applied.append(remedy)

            return json.dumps({
                "success": success,
                "stage": stage_name,
                "new_status": new_status,
                "error_message": stage.error_message if stage else None,
            })

        except Exception as exc:
            return json.dumps({
                "success": False,
                "stage": stage_name,
                "error": str(exc),
            })

    def rollback_stage(stage_name: str) -> str:
        """
        Roll back a failed stage to a clean state (marks it as skipped
        and clears processed data for that stage).

        Args:
            stage_name: Stage to roll back ('transform' or 'load').
        """
        stage = state.stages.get(stage_name)
        if stage is None:
            return json.dumps({"success": False, "error": f"Stage '{stage_name}' not found"})

        # Clear processed data for the stage
        if stage_name == "transform":
            state.processed_data.pop("sales_enriched", None)
            state.processed_data.pop("daily_aggregation", None)
        elif stage_name == "load":
            state.processed_data.pop("loaded_count", None)
            state.processed_data.pop("load_target", None)

        stage.status = StageStatus.SKIPPED
        stage.error_message = None

        remedy = f"rollback_stage({stage_name})"
        if remedy not in state.remediation_applied:
            state.remediation_applied.append(remedy)

        state.add_log(Severity.WARNING, "remediation",
                      f"Stage '{stage_name}' rolled back to SKIPPED state")

        return json.dumps({
            "success": True,
            "stage": stage_name,
            "new_status": "skipped",
            "message": f"Stage '{stage_name}' rolled back. Re-run after fixing root cause.",
        })

    return [apply_schema_fix, deduplicate_records, fill_null_defaults, rerun_stage, rollback_stage]
