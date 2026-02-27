"""GraphFlow assembly — conditional DAG wiring for the ETL monitor team."""

from __future__ import annotations

from autogen_agentchat.conditions import MaxMessageTermination, TextMentionTermination
from autogen_agentchat.teams import DiGraphBuilder, GraphFlow
from autogen_core.models import ChatCompletionClient

from ..agents.diagnostician import make_diagnostician_agent
from ..agents.monitor import make_monitor_agent
from ..agents.remediation import make_remediation_agent
from ..agents.reporter import make_reporter_agent
from ..pipeline.state import PipelineState


def build_etl_team(model_client: ChatCompletionClient, state: PipelineState) -> GraphFlow:
    """
    Assemble the 4-agent ETL monitoring team as a conditional DAG GraphFlow.

    Graph structure:
        Monitor ──(ISSUES_DETECTED)──► Diagnostician ──► Remediation ──► Reporter
        Monitor ──(NO_ISSUES_DETECTED)──────────────────────────────────► Reporter
    """
    monitor = make_monitor_agent(model_client, state)
    diagnostician = make_diagnostician_agent(model_client, state)
    remediation = make_remediation_agent(model_client, state)
    reporter = make_reporter_agent(model_client, state)

    builder = DiGraphBuilder()
    builder.add_node(monitor)
    builder.add_node(diagnostician)
    builder.add_node(remediation)
    builder.add_node(reporter)

    # String conditions — GraphFlow checks if substring appears in the message
    builder.add_edge(monitor, diagnostician, condition="PIPELINE_FAIL")
    # Both paths leading to Reporter use the same activation_group with "any" condition,
    # so Reporter fires when EITHER Monitor (healthy) OR Remediation (issue path) completes.
    builder.add_edge(
        monitor, reporter,
        condition="PIPELINE_OK",
        activation_group="reporter_trigger",
        activation_condition="any",
    )

    # Unconditional edges through the issue path
    builder.add_edge(diagnostician, remediation)
    builder.add_edge(
        remediation, reporter,
        activation_group="reporter_trigger",
        activation_condition="any",
    )

    builder.set_entry_point(monitor)

    termination = TextMentionTermination("TERMINATE") | MaxMessageTermination(100)

    return GraphFlow(
        participants=builder.get_participants(),
        graph=builder.build(),
        termination_condition=termination,
    )
