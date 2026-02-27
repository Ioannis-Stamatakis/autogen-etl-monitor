"""Log analysis tools — Diagnostician Agent finds root causes."""

from __future__ import annotations

import json
from typing import Any

from ..pipeline.state import PipelineState, Severity


def make_log_analysis_tools(state: PipelineState) -> list[Any]:
    """Factory: bind log analysis tools to a specific PipelineState instance."""

    def analyze_logs(level_filter: str = "all", stage_filter: str = "all") -> str:
        """
        Retrieve and analyze pipeline logs, optionally filtered by severity
        level and/or stage name.

        Args:
            level_filter: 'all', 'info', 'warning', 'error', or 'critical'.
            stage_filter: 'all' or a specific stage name (e.g. 'ingestion').
        """
        logs = state.logs

        if level_filter != "all":
            logs = [l for l in logs if l.level.value == level_filter]

        if stage_filter != "all":
            logs = [l for l in logs if l.stage == stage_filter]

        entries = [
            {
                "timestamp": log.timestamp,
                "level": log.level.value,
                "stage": log.stage,
                "message": log.message,
                "details": log.details,
            }
            for log in logs
        ]

        severity_counts = {}
        for log in state.logs:
            severity_counts[log.level.value] = severity_counts.get(log.level.value, 0) + 1

        result = {
            "total_logs": len(state.logs),
            "filtered_count": len(entries),
            "severity_counts": severity_counts,
            "entries": entries,
        }
        return json.dumps(result, indent=2)

    def check_schema_drift() -> str:
        """
        Analyze schema drift by comparing actual column names and types
        against expected schema definitions. Returns a drift report.
        """
        ingest = state.stages.get("ingestion")
        if ingest is None:
            return json.dumps({"error": "Ingestion stage has not run"})

        metrics = ingest.quality_metrics
        sales_df = state.source_data.get("sales")

        if sales_df is None:
            return json.dumps({"error": "No sales data in state — ingestion likely failed before loading data"})

        expected_cols = {
            "transaction_id", "customer_id", "product_id", "quantity",
            "unit_price", "currency", "transaction_date", "region", "status",
        }
        actual_cols = set(sales_df.columns)

        missing_cols = list(expected_cols - actual_cols)
        extra_cols = list(actual_cols - expected_cols)

        # Detect type mismatches
        type_issues = {}
        expected_types = {
            "quantity": "int64",
            "unit_price": "float64",
        }
        for col, expected_type in expected_types.items():
            if col in sales_df.columns:
                actual_type = str(sales_df[col].dtype)
                if actual_type != expected_type:
                    type_issues[col] = {
                        "expected": expected_type,
                        "actual": actual_type,
                        "sample_values": sales_df[col].head(3).tolist(),
                    }

        drift_detected = bool(missing_cols or extra_cols or type_issues)

        result = {
            "drift_detected": drift_detected,
            "missing_required_columns": missing_cols,
            "unexpected_extra_columns": extra_cols,
            "type_mismatches": type_issues,
            "schema_violations_from_stage": metrics.schema_violations,
            "recommendation": (
                "Apply schema fix to handle drift" if drift_detected
                else "Schema matches expected definition"
            ),
        }
        return json.dumps(result, indent=2)

    def trace_error_chain() -> str:
        """
        Trace the chain of errors across pipeline stages to identify the
        root cause and downstream impact of failures.
        """
        chain = []
        root_cause = None

        for stage_name in ["ingestion", "transform", "load"]:
            stage = state.stages.get(stage_name)
            if stage is None:
                continue

            stage_errors = [
                log for log in state.logs
                if log.stage == stage_name and log.level in (Severity.ERROR, Severity.CRITICAL)
            ]

            entry = {
                "stage": stage_name,
                "status": stage.status.value,
                "error_message": stage.error_message,
                "error_logs": [
                    {"level": l.level.value, "message": l.message, "details": l.details}
                    for l in stage_errors
                ],
            }
            chain.append(entry)

            if stage.error_message and root_cause is None:
                root_cause = {
                    "stage": stage_name,
                    "error": stage.error_message,
                    "downstream_impact": [
                        s for s in ["transform", "load"]
                        if s != stage_name
                    ],
                }

        result = {
            "root_cause": root_cause,
            "error_chain": chain,
            "total_errors": len(state.error_logs),
            "failure_type": state.run.failure_type,
        }
        return json.dumps(result, indent=2)

    def compare_data_samples() -> str:
        """
        Compare sample rows from source data vs. processed data to identify
        transformation anomalies or data corruption.
        """
        source_sales = state.source_data.get("sales")
        processed = state.processed_data.get("sales_enriched")

        if source_sales is None:
            return json.dumps({"error": "No source sales data available"})

        source_sample = source_sales.head(5).to_dict(orient="records")

        processed_sample = None
        if processed is not None:
            cols = [c for c in ["transaction_id", "customer_id", "unit_price",
                                 "unit_price_usd", "revenue_usd", "customer_tier"]
                    if c in processed.columns]
            processed_sample = processed[cols].head(5).to_dict(orient="records")

        # Detect null concentration
        null_summary = {}
        if source_sales is not None:
            for col in source_sales.columns:
                nc = int(source_sales[col].isna().sum())
                if nc > 0:
                    null_summary[col] = {
                        "null_count": nc,
                        "null_pct": round(nc / len(source_sales) * 100, 1),
                    }

        customers = state.source_data.get("customers")
        customer_null_summary = {}
        if customers is not None:
            for col in customers.columns:
                nc = int(customers[col].isna().sum())
                if nc > 0:
                    customer_null_summary[f"customers.{col}"] = {
                        "null_count": nc,
                        "null_pct": round(nc / len(customers) * 100, 1),
                    }

        result = {
            "source_row_count": len(source_sales),
            "processed_row_count": len(processed) if processed is not None else None,
            "source_columns": list(source_sales.columns),
            "source_sample": source_sample,
            "processed_sample": processed_sample,
            "null_concentration": {**null_summary, **customer_null_summary},
        }
        return json.dumps(result, default=str, indent=2)

    return [analyze_logs, check_schema_drift, trace_error_chain, compare_data_samples]
