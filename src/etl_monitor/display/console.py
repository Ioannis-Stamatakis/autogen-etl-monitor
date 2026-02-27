"""Rich terminal formatting for agent conversations and reports."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.syntax import Syntax
from rich.text import Text
from rich.theme import Theme

_THEME = Theme({
    "agent.monitor": "bold cyan",
    "agent.diagnostician": "bold yellow",
    "agent.remediation": "bold green",
    "agent.reporter": "bold magenta",
    "agent.tool": "dim white",
    "status.healthy": "bold green",
    "status.failed": "bold red",
    "status.degraded": "bold yellow",
    "info": "dim cyan",
})

console = Console(theme=_THEME, highlight=False)

_AGENT_STYLES: dict[str, str] = {
    "monitor_agent": "agent.monitor",
    "diagnostician_agent": "agent.diagnostician",
    "remediation_agent": "agent.remediation",
    "reporter_agent": "agent.reporter",
}

_AGENT_LABELS: dict[str, str] = {
    "monitor_agent": "Monitor",
    "diagnostician_agent": "Diagnostician",
    "remediation_agent": "Remediation",
    "reporter_agent": "Reporter",
}


def print_scenario_header(scenario_name: str, description: str, run_id: str) -> None:
    console.print()
    console.print(Rule(f"[bold white] Scenario: {scenario_name} [/]", style="bold blue"))
    console.print(f"  [info]Run ID:[/] {run_id}  |  [info]Description:[/] {description}")
    console.print()


def print_agent_message(agent_name: str, content: str) -> None:
    style = _AGENT_STYLES.get(agent_name, "white")
    label = _AGENT_LABELS.get(agent_name, agent_name)

    # Truncate very long tool output echoes
    display_content = content
    if len(content) > 2000:
        display_content = content[:2000] + "\n... [dim](truncated)[/dim]"

    console.print(
        Panel(
            Text(display_content, overflow="fold"),
            title=f"[{style}]  {label}  [/]",
            border_style=style,
            padding=(0, 1),
        )
    )


def print_tool_call(tool_name: str, args: dict) -> None:
    args_str = ", ".join(f"{k}={v!r}" for k, v in args.items()) if args else ""
    console.print(f"  [agent.tool]  ⚙ {tool_name}({args_str})[/]")


def print_tool_result(tool_name: str, result_preview: str) -> None:
    preview = result_preview[:300] + "..." if len(result_preview) > 300 else result_preview
    console.print(f"  [agent.tool]  ↳ {preview}[/]")


def print_incident_report(report_text: str) -> None:
    console.print()
    console.print(Rule("[bold magenta] Incident Report [/]", style="magenta"))
    console.print(
        Syntax(report_text, "text", theme="monokai", word_wrap=True),
    )


def print_final_summary(results: list[dict]) -> None:
    console.print()
    console.print(Rule("[bold white] Run Summary [/]", style="bold blue"))
    console.print()
    for r in results:
        scenario = r.get("scenario", "unknown")
        outcome = r.get("outcome", "unknown")
        agents = r.get("agents_activated", [])

        status_style = {
            "HEALTHY": "status.healthy",
            "RESOLVED": "status.healthy",
            "FAILED": "status.failed",
            "DEGRADED": "status.degraded",
            "UNRESOLVED": "status.failed",
        }.get(outcome, "white")

        agents_str = " → ".join(agents) if agents else "N/A"
        console.print(
            f"  [{status_style}]{outcome:12s}[/]  {scenario:35s}  Agents: {agents_str}"
        )
    console.print()


def print_startup_banner() -> None:
    banner = """
  ╔══════════════════════════════════════════════════════════╗
  ║         Multi-Agent ETL Monitor & Self-Healer            ║
  ║         AutoGen 0.4+ (AgentChat) + Google Gemini         ║
  ╚══════════════════════════════════════════════════════════╝
"""
    console.print(f"[bold blue]{banner}[/]")
