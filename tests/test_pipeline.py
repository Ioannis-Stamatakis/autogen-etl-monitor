"""Unit tests for the ETL pipeline simulator."""

import pytest

from etl_monitor.pipeline.simulator import simulate_pipeline
from etl_monitor.pipeline.state import StageStatus


class TestHealthyPipeline:
    def test_healthy_run_succeeds(self):
        state = simulate_pipeline("healthy")
        assert state.run.overall_status == StageStatus.SUCCESS

    def test_healthy_all_stages_pass(self):
        state = simulate_pipeline("healthy")
        for stage_name in ["ingestion", "transform", "load"]:
            assert stage_name in state.stages
            assert state.stages[stage_name].status == StageStatus.SUCCESS

    def test_healthy_no_error_logs(self):
        state = simulate_pipeline("healthy")
        assert len(state.error_logs) == 0

    def test_healthy_rows_flow_through(self):
        state = simulate_pipeline("healthy")
        ingest = state.stages["ingestion"]
        assert ingest.rows_out > 0
        assert state.stages["load"].rows_out > 0

    def test_healthy_produces_enriched_data(self):
        state = simulate_pipeline("healthy")
        assert "sales_enriched" in state.processed_data
        df = state.processed_data["sales_enriched"]
        assert "revenue_usd" in df.columns
        assert "customer_tier" in df.columns


class TestSchemaScenarios:
    def test_type_change_fails_ingestion(self):
        state = simulate_pipeline("schema_drift_type_change")
        assert state.run.overall_status == StageStatus.FAILED
        assert state.stages["ingestion"].status == StageStatus.FAILED

    def test_type_change_detects_type_mismatch(self):
        state = simulate_pipeline("schema_drift_type_change")
        ingest = state.stages["ingestion"]
        assert "unit_price" in ingest.quality_metrics.type_mismatches

    def test_new_column_detected_as_warning(self):
        state = simulate_pipeline("schema_drift_new_column")
        # Extra column is a warning, not a hard fail
        warning_logs = [l for l in state.logs if l.level.value == "warning"]
        assert any("loyalty_points" in l.message or "unexpected" in l.message.lower()
                   for l in warning_logs)

    def test_missing_column_fails_ingestion(self):
        state = simulate_pipeline("schema_drift_missing_column")
        assert state.stages["ingestion"].status == StageStatus.FAILED
        ingest = state.stages["ingestion"]
        assert any("region" in v for v in ingest.quality_metrics.schema_violations)

    def test_missing_source_file_fails_ingestion(self):
        state = simulate_pipeline("missing_source_file")
        assert state.stages["ingestion"].status == StageStatus.FAILED
        assert "product_catalog" in state.stages["ingestion"].error_message


class TestDataQualityScenarios:
    def test_null_emails_recorded(self):
        state = simulate_pipeline("data_quality_nulls")
        ingest = state.stages["ingestion"]
        assert "customer.email" in ingest.quality_metrics.null_counts
        assert ingest.quality_metrics.null_counts["customer.email"] > 0

    def test_duplicates_detected(self):
        state = simulate_pipeline("data_quality_duplicates")
        ingest = state.stages["ingestion"]
        assert ingest.quality_metrics.duplicate_count > 0

    def test_pk_violation_fails_load(self):
        state = simulate_pipeline("pk_constraint_violation")
        assert state.stages["load"].status == StageStatus.FAILED
        assert "PK constraint" in state.stages["load"].error_message


class TestStateProperties:
    def test_has_failures_true_when_failed(self):
        state = simulate_pipeline("schema_drift_type_change")
        assert state.has_failures is True

    def test_has_failures_false_when_healthy(self):
        state = simulate_pipeline("healthy")
        assert state.has_failures is False

    def test_run_id_is_set(self):
        state = simulate_pipeline("healthy")
        assert state.run.run_id
        assert len(state.run.run_id) > 0

    def test_logs_contain_entries(self):
        state = simulate_pipeline("healthy")
        assert len(state.logs) > 0

    def test_get_stage_creates_if_missing(self):
        state = simulate_pipeline("healthy")
        stage = state.get_stage("nonexistent")
        assert stage.stage_name == "nonexistent"
        assert stage.status == StageStatus.PENDING
