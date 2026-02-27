"""Reporting tools — Reporter Agent generates incident reports."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from ..pipeline.state import PipelineState, StageStatus


def make_reporting_tools(state: PipelineState) -> list[Any]:
    """Factory: bind reporting tools to a specific PipelineState instance."""

    def format_incident_report() -> str:
        """
        Compile a complete structured incident report summarizing the pipeline
        run: health status, failures detected, root cause, remediation steps
        applied, and final outcome. Store the report in pipeline state.
        """
        now = datetime.utcnow().isoformat()

        # Determine overall health
        overall = state.run.overall_status
        all_stages = list(state.stages.values())
        failed_stages = [s for s in all_stages if s.status == StageStatus.FAILED]
        remediated_stages = [s for s in all_stages if s.status == StageStatus.REMEDIATED]
        error_logs = state.error_logs

        if not failed_stages and not error_logs:
            health = "HEALTHY"
            severity = "NONE"
        elif failed_stages:
            health = "FAILED"
            severity = "HIGH" if len(failed_stages) > 1 else "MEDIUM"
        else:
            health = "DEGRADED"
            severity = "LOW"

        # Collect all errors
        errors = [
            {
                "stage": log.stage,
                "level": log.level.value,
                "message": log.message,
                "details": log.details,
            }
            for log in error_logs
        ]

        # Collect warnings
        warnings = [
            {
                "stage": log.stage,
                "message": log.message,
            }
            for log in state.logs
            if log.level.value == "warning"
        ]

        # Stage summary
        stage_summary = {}
        for name, stage in state.stages.items():
            stage_summary[name] = {
                "status": stage.status.value,
                "rows_in": stage.rows_in,
                "rows_out": stage.rows_out,
                "error": stage.error_message,
            }

        report = {
            "report_id": f"INC-{state.run.run_id}",
            "generated_at": now,
            "pipeline": state.run.pipeline_name,
            "run_id": state.run.run_id,
            "scenario": state.run.scenario_name,
            "health_status": health,
            "severity": severity,
            "overall_pipeline_status": overall.value,
            "failure_type": state.run.failure_type,
            "timeline": {
                "started_at": state.run.started_at,
                "finished_at": state.run.finished_at,
            },
            "stage_summary": stage_summary,
            "errors_detected": errors,
            "warnings": warnings,
            "remediation_applied": state.remediation_applied,
            "remediation_count": len(state.remediation_applied),
            "data_quality_summary": {
                "null_counts": state.stages.get("ingestion", StageStatus).quality_metrics.null_counts
                if hasattr(state.stages.get("ingestion", None), "quality_metrics") else {},
                "duplicate_count": state.stages["ingestion"].quality_metrics.duplicate_count
                if "ingestion" in state.stages else 0,
                "schema_violations": state.stages["ingestion"].quality_metrics.schema_violations
                if "ingestion" in state.stages else [],
            },
            "final_outcome": (
                "RESOLVED" if state.remediation_applied and not failed_stages
                else "UNRESOLVED" if failed_stages
                else "HEALTHY"
            ),
            "recommendations": _generate_recommendations(state),
        }

        # Format as readable text report
        text_report = _format_text_report(report)
        state.incident_report = text_report

        return json.dumps({
            "report_id": report["report_id"],
            "health_status": health,
            "severity": severity,
            "final_outcome": report["final_outcome"],
            "full_report": text_report,
        }, indent=2)

    def _generate_recommendations(state: PipelineState) -> list[str]:
        recs = []

        ingest = state.stages.get("ingestion")
        if ingest:
            metrics = ingest.quality_metrics
            if metrics.schema_violations:
                recs.append("Implement schema version pinning in the ingestion contract.")
            if metrics.type_mismatches:
                recs.append("Add explicit type validation and casting at ingestion boundary.")
            if metrics.duplicate_count > 0:
                recs.append("Enable upstream deduplication or add unique constraint monitoring.")
            if metrics.null_counts:
                recs.append("Define and enforce NOT NULL constraints for critical fields.")

        if state.run.failure_type == "pipeline_failure":
            recs.append("Implement file arrival SLA checks before pipeline trigger.")
        if not recs:
            recs.append("Continue monitoring. No significant issues detected.")
        return recs

    def _format_text_report(report: dict) -> str:
        lines = [
            "=" * 70,
            f"  ETL PIPELINE INCIDENT REPORT",
            f"  Report ID : {report['report_id']}",
            f"  Generated : {report['generated_at']}",
            "=" * 70,
            "",
            f"PIPELINE     : {report['pipeline']}",
            f"RUN ID       : {report['run_id']}",
            f"SCENARIO     : {report['scenario']}",
            f"HEALTH       : {report['health_status']}",
            f"SEVERITY     : {report['severity']}",
            f"FAILURE TYPE : {report['failure_type'] or 'N/A'}",
            f"OUTCOME      : {report['final_outcome']}",
            "",
            "TIMELINE",
            f"  Started  : {report['timeline']['started_at']}",
            f"  Finished : {report['timeline']['finished_at']}",
            "",
            "STAGE SUMMARY",
        ]
        for stage, info in report["stage_summary"].items():
            status_icon = {"success": "✓", "failed": "✗", "skipped": "↷",
                           "remediated": "⟳", "pending": "?", "running": "►"}.get(info["status"], "?")
            lines.append(
                f"  {status_icon} {stage:12s} | {info['status']:12s} | "
                f"in={info['rows_in']:5d} out={info['rows_out']:5d}"
                + (f" | ERR: {info['error']}" if info["error"] else "")
            )

        if report["errors_detected"]:
            lines += ["", "ERRORS DETECTED"]
            for err in report["errors_detected"]:
                lines.append(f"  [{err['level'].upper()}] [{err['stage']}] {err['message']}")

        if report["warnings"]:
            lines += ["", "WARNINGS"]
            for warn in report["warnings"]:
                lines.append(f"  [WARN] [{warn['stage']}] {warn['message']}")

        if report["remediation_applied"]:
            lines += ["", "REMEDIATION APPLIED"]
            for i, action in enumerate(report["remediation_applied"], 1):
                lines.append(f"  {i}. {action}")
        else:
            lines += ["", "REMEDIATION APPLIED", "  None required"]

        lines += ["", "RECOMMENDATIONS"]
        for rec in report["recommendations"]:
            lines.append(f"  • {rec}")

        lines += ["", "=" * 70]
        return "\n".join(lines)

    return [format_incident_report]
