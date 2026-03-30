"""GraphFlow assembly — conditional DAG wiring for the ETL monitor team."""

from __future__ import annotations

from autogen_agentchat.conditions import MaxMessageTermination, TextMentionTermination
from autogen_agentchat.teams import DiGraphBuilder, GraphFlow
from autogen_core.models import ChatCompletionClient

from ..agents.diagnostician import make_diagnostician_agent
from ..agents.monitor import make_monitor_agent
from ..agents.remediation import make_remediation_agent
from ..agents.reporter import make_reporter_agent
from ..agents.watchdog import make_watchdog_agent
from ..pipeline.history import HistoryStore
from ..pipeline.state import PipelineState


def build_etl_team(
    model_client: ChatCompletionClient,
    state: PipelineState,
    store: HistoryStore,
) -> GraphFlow:
    """
    Assemble the 5-agent ETL monitoring team as a conditional DAG GraphFlow.

    Graph structure:
        Watchdog ──(WATCHDOG_KNOWN_FAILURE)──────────────────────► Remediation ──► Reporter
        Watchdog ──(WATCHDOG_NEW_FAILURE)──► Monitor ──(PIPELINE_FAIL)──► Diagnostician ──► Remediation
                                             Monitor ──(PIPELINE_OK)─────────────────────────────────► Reporter
    """
    watchdog = make_watchdog_agent(model_client, state, store)
    monitor = make_monitor_agent(model_client, state)
    diagnostician = make_diagnostician_agent(model_client, state)
    remediation = make_remediation_agent(model_client, state)
    reporter = make_reporter_agent(model_client, state)

    builder = DiGraphBuilder()
    builder.add_node(watchdog)
    builder.add_node(monitor)
    builder.add_node(diagnostician)
    builder.add_node(remediation)
    builder.add_node(reporter)

    # Watchdog routes: known recurring failure → skip straight to Remediation
    builder.add_edge(watchdog, remediation, condition="WATCHDOG_KNOWN_FAILURE")
    # Watchdog routes: new/rare failure → normal Monitor flow
    builder.add_edge(watchdog, monitor, condition="WATCHDOG_NEW_FAILURE")

    # Monitor routes
    builder.add_edge(monitor, diagnostician, condition="PIPELINE_FAIL")
    builder.add_edge(
        monitor, reporter,
        condition="PIPELINE_OK",
        activation_group="reporter_trigger",
        activation_condition="any",
    )

    # Issue path: Diagnostician → Remediation → Reporter
    builder.add_edge(diagnostician, remediation)
    builder.add_edge(
        remediation, reporter,
        activation_group="reporter_trigger",
        activation_condition="any",
    )

    builder.set_entry_point(watchdog)

    termination = TextMentionTermination("TERMINATE") | MaxMessageTermination(100)

    return GraphFlow(
        participants=builder.get_participants(),
        graph=builder.build(),
        termination_condition=termination,
    )
