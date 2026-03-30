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
    "agent.watchdog": "bold blue",
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
    "watchdog_agent": "agent.watchdog",
}

_AGENT_LABELS: dict[str, str] = {
    "monitor_agent": "Monitor",
    "diagnostician_agent": "Diagnostician",
    "remediation_agent": "Remediation",
    "reporter_agent": "Reporter",
    "watchdog_agent": "Watchdog",
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


def print_history_summary(store: "HistoryStore") -> None:  # type: ignore[name-defined]
    """Print a trend summary from the run history store."""
    from rich.table import Table

    trend = store.get_failure_trend(window_hours=24)
    records = store.get_recent(limit=20)

    console.print()
    console.print(Rule("[bold blue] Run History & Trends (last 24 h) [/]", style="bold blue"))
    console.print(
        f"  Total runs: [bold]{trend['total_runs']}[/]  |  "
        f"Failed: [status.failed]{trend['failed_count']}[/]  |  "
        f"Resolved: [status.healthy]{trend['resolved_count']}[/]  |  "
        f"MTTR: [bold]{f\"{trend['mttr_seconds']:.0f}s\" if trend['mttr_seconds'] else 'N/A'}[/]"
    )

    if trend["recurrence_by_type"]:
        console.print()
        table = Table(title="Recurrence by Failure Type", show_header=True, header_style="bold")
        table.add_column("Failure Type", style="cyan")
        table.add_column("Count", justify="right")
        for ft, count in sorted(trend["recurrence_by_type"].items(), key=lambda x: -x[1]):
            table.add_row(ft, str(count))
        console.print(table)

    if records:
        console.print()
        table = Table(title="Recent Runs", show_header=True, header_style="bold")
        table.add_column("Run ID", style="dim")
        table.add_column("Scenario")
        table.add_column("Failure Type")
        table.add_column("Outcome")
        table.add_column("Duration")
        for r in records[-10:]:
            outcome = r.get("outcome", "?")
            style = {"HEALTHY": "status.healthy", "RESOLVED": "status.healthy",
                     "FAILED": "status.failed", "UNRESOLVED": "status.failed"}.get(outcome, "white")
            dur = r.get("duration_seconds")
            dur_str = f"{dur:.1f}s" if dur is not None else "N/A"
            table.add_row(
                r.get("run_id", "?"),
                r.get("scenario_name", "?"),
                r.get("failure_type") or "none",
                f"[{style}]{outcome}[/]",
                dur_str,
            )
        console.print(table)
    console.print()


def print_startup_banner() -> None:
    banner = """
  ╔══════════════════════════════════════════════════════════╗
  ║         Multi-Agent ETL Monitor & Self-Healer            ║
  ║         AutoGen 0.4+ (AgentChat) + Google Gemini         ║
  ╚══════════════════════════════════════════════════════════╝
"""
    console.print(f"[bold blue]{banner}[/]")
