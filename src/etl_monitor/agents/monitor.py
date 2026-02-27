"""Monitor Agent — first responder that detects pipeline health issues."""

from __future__ import annotations

from autogen_agentchat.agents import AssistantAgent
from autogen_core.models import ChatCompletionClient

from ..pipeline.state import PipelineState
from ..tools.inspection import make_inspection_tools

_SYSTEM_PROMPT = """\
You are the ETL Pipeline Monitor Agent.

Step 1: Call your tools: get_pipeline_status, inspect_pipeline_run, check_data_quality, get_stage_metrics.

Step 2: After your tools return results, write a response that MUST include ALL THREE of the following:

a) A plain-English paragraph summarizing which stages passed or failed, any errors or data quality issues, and the overall verdict.

b) Exactly one routing token on its own line at the end — this token is MANDATORY and the system cannot continue without it:

If any stage failed OR any quality issues exist → write:
PIPELINE_FAIL

If all stages succeeded with zero errors and zero quality issues → write:
PIPELINE_OK

CRITICAL: Your text response after the tool calls MUST NOT be empty. You must always write the summary paragraph and the routing token.
"""


def make_monitor_agent(model_client: ChatCompletionClient, state: PipelineState) -> AssistantAgent:
    """Create and return the Monitor AssistantAgent bound to the given state."""
    tools = make_inspection_tools(state)
    return AssistantAgent(
        name="monitor_agent",
        model_client=model_client,
        tools=tools,
        system_message=_SYSTEM_PROMPT,
        reflect_on_tool_use=True,
    )
