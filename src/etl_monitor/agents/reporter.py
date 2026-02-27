"""Reporter Agent — produces the final structured incident report."""

from __future__ import annotations

from autogen_agentchat.agents import AssistantAgent
from autogen_core.models import ChatCompletionClient

from ..pipeline.state import PipelineState
from ..tools.reporting import make_reporting_tools

_SYSTEM_PROMPT = """\
You are the ETL Pipeline Reporter Agent. Call format_incident_report to generate the structured incident report.

CRITICAL: After your tool call completes, you MUST write a non-empty text response. Your response MUST include:
1. A plain-English summary covering: health status, failure type, remediation outcome, and top recommendations.
2. The word TERMINATE on its own line at the very end.

Your text response after the tool call MUST NOT be empty. Always write the summary and end with TERMINATE.
"""


def make_reporter_agent(model_client: ChatCompletionClient, state: PipelineState) -> AssistantAgent:
    """Create and return the Reporter AssistantAgent bound to the given state."""
    tools = make_reporting_tools(state)
    return AssistantAgent(
        name="reporter_agent",
        model_client=model_client,
        tools=tools,
        system_message=_SYSTEM_PROMPT,
        reflect_on_tool_use=True,
    )
