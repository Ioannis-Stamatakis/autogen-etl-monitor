"""Inspection tools — Monitor Agent discovers pipeline health issues."""

from __future__ import annotations

import json
from typing import Any

from ..pipeline.state import PipelineState, StageStatus


def make_inspection_tools(state: PipelineState) -> list[Any]:
    """Factory: bind inspection tools to a specific PipelineState instance."""

    def inspect_pipeline_run() -> str:
        """
        Inspect the current ETL pipeline run and return a structured summary
        of run metadata, overall status, and all stage statuses.
        """
        stages_summary = {}
        for name, stage in state.stages.items():
            stages_summary[name] = {
                "status": stage.status.value,
                "rows_in": stage.rows_in,
                "rows_out": stage.rows_out,
                "started_at": stage.started_at,
                "finished_at": stage.finished_at,
                "error_message": stage.error_message,
            }

        result = {
            "run_id": state.run.run_id,
            "pipeline_name": state.run.pipeline_name,
            "scenario": state.run.scenario_name,
            "overall_status": state.run.overall_status.value,
            "failure_injected": state.run.failure_injected,
            "failure_type": state.run.failure_type,
            "started_at": state.run.started_at,
            "finished_at": state.run.finished_at,
            "stages": stages_summary,
            "total_log_entries": len(state.logs),
            "error_log_count": len(state.error_logs),
        }
        return json.dumps(result, indent=2)

    def check_data_quality() -> str:
        """
        Check data quality metrics from the ingestion stage including
        null counts, duplicate counts, schema violations, and type mismatches.
        """
        ingest = state.stages.get("ingestion")
        if ingest is None:
            return json.dumps({"error": "Ingestion stage has not run yet"})

        metrics = ingest.quality_metrics
        has_issues = bool(
            metrics.null_counts
            or metrics.duplicate_count > 0
            or metrics.schema_violations
            or metrics.type_mismatches
            or metrics.out_of_range_count > 0
        )

        result = {
            "stage": "ingestion",
            "total_rows": metrics.total_rows,
            "has_quality_issues": has_issues,
            "null_counts": metrics.null_counts,
            "duplicate_count": metrics.duplicate_count,
            "out_of_range_count": metrics.out_of_range_count,
            "schema_violations": metrics.schema_violations,
            "type_mismatches": metrics.type_mismatches,
        }
        return json.dumps(result, indent=2)

    def get_pipeline_status() -> str:
        """
        Get the high-level health status of the pipeline, categorized as
        HEALTHY, DEGRADED, or FAILED with a brief reason.
        """
        overall = state.run.overall_status
        failed_stages = [
            name for name, s in state.stages.items()
            if s.status == StageStatus.FAILED
        ]
        skipped_stages = [
            name for name, s in state.stages.items()
            if s.status == StageStatus.SKIPPED
        ]
        error_count = len(state.error_logs)

        if overall == StageStatus.SUCCESS and error_count == 0:
            health = "HEALTHY"
            reason = "All stages completed successfully with no errors."
        elif overall == StageStatus.FAILED:
            health = "FAILED"
            reason = f"Failed stages: {failed_stages}. Error count: {error_count}."
        else:
            health = "DEGRADED"
            reason = f"Warnings present. Skipped stages: {skipped_stages}. Errors: {error_count}."

        result = {
            "health": health,
            "overall_status": overall.value,
            "reason": reason,
            "failed_stages": failed_stages,
            "skipped_stages": skipped_stages,
            "error_log_count": error_count,
            "remediation_applied": state.remediation_applied,
        }
        return json.dumps(result, indent=2)

    def get_stage_metrics(stage_name: str) -> str:
        """
        Get detailed metrics for a specific pipeline stage.

        Args:
            stage_name: One of 'ingestion', 'transform', or 'load'.
        """
        stage = state.stages.get(stage_name)
        if stage is None:
            available = list(state.stages.keys())
            return json.dumps({
                "error": f"Stage '{stage_name}' not found",
                "available_stages": available,
            })

        metrics = stage.quality_metrics
        result = {
            "stage_name": stage.stage_name,
            "status": stage.status.value,
            "rows_in": stage.rows_in,
            "rows_out": stage.rows_out,
            "started_at": stage.started_at,
            "finished_at": stage.finished_at,
            "error_message": stage.error_message,
            "quality_metrics": {
                "total_rows": metrics.total_rows,
                "null_counts": metrics.null_counts,
                "duplicate_count": metrics.duplicate_count,
                "schema_violations": metrics.schema_violations,
                "type_mismatches": metrics.type_mismatches,
            },
            "metadata": stage.metadata,
        }
        return json.dumps(result, indent=2)

    return [inspect_pipeline_run, check_data_quality, get_pipeline_status, get_stage_metrics]
