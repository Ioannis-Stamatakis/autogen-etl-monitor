"""Watchdog Agent — pre-monitor triage using run history."""

from __future__ import annotations

from autogen_agentchat.agents import AssistantAgent
from autogen_core.models import ChatCompletionClient

from ..pipeline.history import HistoryStore
from ..pipeline.state import PipelineState
from ..tools.history import make_history_tools

_SYSTEM_PROMPT = """\
You are the ETL Pipeline Watchdog Agent. Your job is to triage this pipeline run \
using historical data BEFORE the Monitor Agent inspects it.

Step 1: Call check_known_failure to determine if this scenario and failure type \
have been seen recently.

Step 2: Call get_failure_trends to understand the broader pattern (recurrence, MTTR).

Step 3: Call compare_metrics_to_baseline to check if data quality has degraded \
compared to the historical average.

Step 4: Write a triage summary that MUST include:
- Whether this is a known recurring failure or a new/rare one
- Recurrence count and MTTR if available
- Any anomalous metric deltas vs. baseline

Step 5: End your response with EXACTLY ONE of these routing tokens on its own line:

If this is a KNOWN RECURRING failure (seen 2+ times in the last 24 hours):
WATCHDOG_KNOWN_FAILURE

If this is a NEW or RARE failure (less than 2 recent occurrences):
WATCHDOG_NEW_FAILURE

CRITICAL: Your response after tool calls MUST NOT be empty. Always write the triage \
summary and the routing token.
"""


def make_watchdog_agent(
    model_client: ChatCompletionClient,
    state: PipelineState,
    store: HistoryStore,
) -> AssistantAgent:
    """Create and return the Watchdog AssistantAgent bound to the given state and store."""
    tools = make_history_tools(state, store)
    # Watchdog only needs the query tools, not save_run_to_history
    query_tools = [t for t in tools if t.__name__ != "save_run_to_history"]
    return AssistantAgent(
        name="watchdog_agent",
        model_client=model_client,
        tools=query_tools,
        system_message=_SYSTEM_PROMPT,
        reflect_on_tool_use=True,
    )
