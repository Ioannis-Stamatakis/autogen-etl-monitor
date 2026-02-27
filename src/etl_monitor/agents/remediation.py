"""Remediation Agent — applies fixes to heal the pipeline."""

from __future__ import annotations

from autogen_agentchat.agents import AssistantAgent
from autogen_core.models import ChatCompletionClient

from ..pipeline.state import PipelineState
from ..tools.remediation import make_remediation_tools

_SYSTEM_PROMPT = """\
You are the ETL Pipeline Remediation Agent. Apply the fixes recommended by the Diagnostician using your tools.

Remediation rules:
- Fix data FIRST, then call rerun_stage.
- Schema drift: apply_schema_fix → rerun_stage("ingestion") → rerun_stage("transform") → rerun_stage("load")
- Duplicates: deduplicate_records("sales") → rerun_stage("load")
- Nulls: fill_null_defaults(dataset, column, default_value) → rerun_stage for affected stage
- If a stage still fails, use rollback_stage and explain why.

apply_schema_fix accepts fix_type values: "drop_extra_columns", "fix_type_cast", "add_missing_column_default"

CRITICAL: After your tool calls complete, you MUST write a non-empty text response. Your response MUST include:
1. A plain-English summary of every tool you called and whether each action succeeded or failed.
2. The final status: whether the pipeline issue was resolved or not.

Your text response after tools MUST NOT be empty. Always write the summary.
"""


def make_remediation_agent(model_client: ChatCompletionClient, state: PipelineState) -> AssistantAgent:
    """Create and return the Remediation AssistantAgent bound to the given state."""
    tools = make_remediation_tools(state)
    return AssistantAgent(
        name="remediation_agent",
        model_client=model_client,
        tools=tools,
        system_message=_SYSTEM_PROMPT,
        reflect_on_tool_use=True,
    )
