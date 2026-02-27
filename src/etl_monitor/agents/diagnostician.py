"""Diagnostician Agent — root cause analysis specialist."""

from __future__ import annotations

from autogen_agentchat.agents import AssistantAgent
from autogen_core.models import ChatCompletionClient

from ..pipeline.state import PipelineState
from ..tools.log_analysis import make_log_analysis_tools

_SYSTEM_PROMPT = """\
You are the ETL Pipeline Diagnostician Agent. Use your tools to find the root cause of pipeline failures, then write a structured diagnosis in plain English.

After calling your tools, write a text analysis that includes:
- Root Cause: (one sentence)
- Failure Category: (schema_drift / data_quality / pipeline_failure / transform_error)
- Affected Stages: (list)
- Recommended Fixes: (name the exact remediation tool and arguments the Remediation Agent should use)

Do not apply fixes yourself.
"""


def make_diagnostician_agent(model_client: ChatCompletionClient, state: PipelineState) -> AssistantAgent:
    """Create and return the Diagnostician AssistantAgent bound to the given state."""
    tools = make_log_analysis_tools(state)
    return AssistantAgent(
        name="diagnostician_agent",
        model_client=model_client,
        tools=tools,
        system_message=_SYSTEM_PROMPT,
        reflect_on_tool_use=True,
    )
