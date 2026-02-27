"""Unit tests for agent tool functions."""

import json

import pytest

from etl_monitor.pipeline.simulator import simulate_pipeline
from etl_monitor.pipeline.state import StageStatus
from etl_monitor.tools.inspection import make_inspection_tools
from etl_monitor.tools.log_analysis import make_log_analysis_tools
from etl_monitor.tools.remediation import make_remediation_tools
from etl_monitor.tools.reporting import make_reporting_tools


@pytest.fixture
def healthy_state():
    return simulate_pipeline("healthy")


@pytest.fixture
def failed_state():
    return simulate_pipeline("schema_drift_type_change")


@pytest.fixture
def null_state():
    return simulate_pipeline("data_quality_nulls")


@pytest.fixture
def dupe_state():
    return simulate_pipeline("data_quality_duplicates")


@pytest.fixture
def pk_state():
    return simulate_pipeline("pk_constraint_violation")


# ---------------------------------------------------------------------------
# Inspection tool tests
# ---------------------------------------------------------------------------

class TestInspectionTools:
    def test_inspect_pipeline_run_returns_valid_json(self, healthy_state):
        tools = make_inspection_tools(healthy_state)
        inspect_fn = tools[0]  # inspect_pipeline_run
        result = json.loads(inspect_fn())
        assert "run_id" in result
        assert "stages" in result
        assert "overall_status" in result

    def test_inspect_pipeline_run_shows_all_stages(self, healthy_state):
        tools = make_inspection_tools(healthy_state)
        result = json.loads(tools[0]())
        assert set(result["stages"].keys()) == {"ingestion", "transform", "load"}

    def test_check_data_quality_healthy(self, healthy_state):
        tools = make_inspection_tools(healthy_state)
        result = json.loads(tools[1]())  # check_data_quality
        assert result["has_quality_issues"] is False

    def test_check_data_quality_detects_type_mismatch(self, failed_state):
        tools = make_inspection_tools(failed_state)
        result = json.loads(tools[1]())
        assert result["has_quality_issues"] is True

    def test_get_pipeline_status_healthy(self, healthy_state):
        tools = make_inspection_tools(healthy_state)
        result = json.loads(tools[2]())  # get_pipeline_status
        assert result["health"] == "HEALTHY"

    def test_get_pipeline_status_failed(self, failed_state):
        tools = make_inspection_tools(failed_state)
        result = json.loads(tools[2]())
        assert result["health"] == "FAILED"
        assert "ingestion" in result["failed_stages"]

    def test_get_stage_metrics_ingestion(self, healthy_state):
        tools = make_inspection_tools(healthy_state)
        result = json.loads(tools[3]("ingestion"))  # get_stage_metrics
        assert result["stage_name"] == "ingestion"
        assert result["status"] == "success"

    def test_get_stage_metrics_unknown_stage(self, healthy_state):
        tools = make_inspection_tools(healthy_state)
        result = json.loads(tools[3]("nonexistent"))
        assert "error" in result
        assert "available_stages" in result


# ---------------------------------------------------------------------------
# Log analysis tool tests
# ---------------------------------------------------------------------------

class TestLogAnalysisTools:
    def test_analyze_logs_returns_entries(self, healthy_state):
        tools = make_log_analysis_tools(healthy_state)
        result = json.loads(tools[0]())  # analyze_logs
        assert "entries" in result
        assert result["total_logs"] > 0

    def test_analyze_logs_filters_by_level(self, failed_state):
        tools = make_log_analysis_tools(failed_state)
        result = json.loads(tools[0](level_filter="error"))
        for entry in result["entries"]:
            assert entry["level"] == "error"

    def test_analyze_logs_filters_by_stage(self, healthy_state):
        tools = make_log_analysis_tools(healthy_state)
        result = json.loads(tools[0](stage_filter="ingestion"))
        for entry in result["entries"]:
            assert entry["stage"] == "ingestion"

    def test_check_schema_drift_healthy(self, healthy_state):
        tools = make_log_analysis_tools(healthy_state)
        result = json.loads(tools[1]())  # check_schema_drift
        assert result["drift_detected"] is False

    def test_check_schema_drift_detects_type_change(self, failed_state):
        tools = make_log_analysis_tools(failed_state)
        result = json.loads(tools[1]())
        assert result["drift_detected"] is True
        assert "unit_price" in result["type_mismatches"]

    def test_trace_error_chain_no_root_cause_healthy(self, healthy_state):
        tools = make_log_analysis_tools(healthy_state)
        result = json.loads(tools[2]())  # trace_error_chain
        assert result["root_cause"] is None

    def test_trace_error_chain_finds_root_cause(self, failed_state):
        tools = make_log_analysis_tools(failed_state)
        result = json.loads(tools[2]())
        assert result["root_cause"] is not None
        assert result["root_cause"]["stage"] == "ingestion"

    def test_compare_data_samples_returns_samples(self, healthy_state):
        tools = make_log_analysis_tools(healthy_state)
        result = json.loads(tools[3]())  # compare_data_samples
        assert "source_sample" in result
        assert len(result["source_sample"]) > 0

    def test_compare_data_samples_detects_nulls(self, null_state):
        tools = make_log_analysis_tools(null_state)
        result = json.loads(tools[3]())
        assert "customers.email" in result["null_concentration"]


# ---------------------------------------------------------------------------
# Remediation tool tests
# ---------------------------------------------------------------------------

class TestRemediationTools:
    def test_apply_schema_fix_drop_extra_columns(self):
        state = simulate_pipeline("schema_drift_new_column")
        # Verify extra column exists
        assert "loyalty_points" in state.source_data["sales"].columns

        tools = make_remediation_tools(state)
        result = json.loads(tools[0]("drop_extra_columns"))  # apply_schema_fix
        assert result["success"] is True
        assert "loyalty_points" not in state.source_data["sales"].columns

    def test_apply_schema_fix_type_cast(self):
        state = simulate_pipeline("schema_drift_type_change")
        price_dtype = str(state.source_data["sales"]["unit_price"].dtype)
        # pandas 2 uses "object", pandas 3 uses "str" / "StringDtype"
        assert price_dtype in ("object", "str", "string") or "String" in price_dtype

        tools = make_remediation_tools(state)
        result = json.loads(tools[0]("fix_type_cast"))
        assert result["success"] is True
        # After fix, dtype should be numeric (float64)
        import pandas as pd
        fixed_dtype = state.source_data["sales"]["unit_price"].dtype
        assert pd.api.types.is_float_dtype(fixed_dtype)

    def test_apply_schema_fix_add_missing_column(self):
        state = simulate_pipeline("schema_drift_missing_column")
        assert "region" not in state.source_data["sales"].columns

        tools = make_remediation_tools(state)
        result = json.loads(tools[0]("add_missing_column_default"))
        assert result["success"] is True
        assert "region" in state.source_data["sales"].columns

    def test_deduplicate_records_removes_dupes(self, dupe_state):
        tools = make_remediation_tools(dupe_state)
        original_count = len(dupe_state.source_data["sales"])
        result = json.loads(tools[1]("sales"))  # deduplicate_records
        assert result["success"] is True
        assert result["rows_removed"] > 0
        assert len(dupe_state.source_data["sales"]) < original_count

    def test_deduplicate_records_invalid_dataset(self, healthy_state):
        tools = make_remediation_tools(healthy_state)
        result = json.loads(tools[1]("bogus"))
        assert result["success"] is False

    def test_fill_null_defaults_fills_nulls(self, null_state):
        tools = make_remediation_tools(null_state)
        null_before = null_state.source_data["customers"]["email"].isna().sum()
        assert null_before > 0

        result = json.loads(tools[2]("customers", "email", "no-email@placeholder.com"))
        assert result["success"] is True
        assert result["rows_fixed"] == null_before
        assert null_state.source_data["customers"]["email"].isna().sum() == 0

    def test_fill_null_defaults_missing_column(self, healthy_state):
        tools = make_remediation_tools(healthy_state)
        result = json.loads(tools[2]("sales", "nonexistent_col"))
        assert result["success"] is False

    def test_rerun_stage_after_fix(self):
        state = simulate_pipeline("schema_drift_type_change")
        assert state.stages["ingestion"].status == StageStatus.FAILED

        rem_tools = make_remediation_tools(state)
        # Fix then rerun
        json.loads(rem_tools[0]("fix_type_cast"))
        result = json.loads(rem_tools[3]("ingestion"))  # rerun_stage
        assert result["success"] is True
        assert result["new_status"] in ("success", "remediated")

    def test_rollback_stage(self, pk_state):
        tools = make_remediation_tools(pk_state)
        result = json.loads(tools[4]("load"))  # rollback_stage
        assert result["success"] is True
        assert pk_state.stages["load"].status == StageStatus.SKIPPED

    def test_remediation_recorded_in_state(self, dupe_state):
        tools = make_remediation_tools(dupe_state)
        tools[1]("sales")  # deduplicate_records
        assert any("deduplicate_records" in r for r in dupe_state.remediation_applied)


# ---------------------------------------------------------------------------
# Reporting tool tests
# ---------------------------------------------------------------------------

class TestReportingTools:
    def test_format_incident_report_healthy(self, healthy_state):
        tools = make_reporting_tools(healthy_state)
        result = json.loads(tools[0]())  # format_incident_report
        assert result["health_status"] == "HEALTHY"
        assert result["final_outcome"] == "HEALTHY"

    def test_format_incident_report_failed(self, failed_state):
        tools = make_reporting_tools(failed_state)
        result = json.loads(tools[0]())
        assert result["health_status"] == "FAILED"

    def test_format_incident_report_contains_report_id(self, healthy_state):
        tools = make_reporting_tools(healthy_state)
        result = json.loads(tools[0]())
        assert result["report_id"].startswith("INC-")

    def test_format_incident_report_stores_in_state(self, healthy_state):
        tools = make_reporting_tools(healthy_state)
        tools[0]()
        assert healthy_state.incident_report is not None
        assert "ETL PIPELINE INCIDENT REPORT" in healthy_state.incident_report

    def test_format_incident_report_full_report_text(self, healthy_state):
        tools = make_reporting_tools(healthy_state)
        result = json.loads(tools[0]())
        report_text = result["full_report"]
        assert "STAGE SUMMARY" in report_text
        assert "RECOMMENDATIONS" in report_text
        assert "ingestion" in report_text
