<div align="center">

# 🤖 AutoGen ETL Monitor

### Multi-agent AI system that monitors ETL pipelines, diagnoses failures, and heals itself

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![AutoGen](https://img.shields.io/badge/AutoGen-0.4+-FF6B35?style=flat)](https://github.com/microsoft/autogen)
[![Gemini](https://img.shields.io/badge/Google_Gemini-2.5_Flash-4285F4?style=flat&logo=google&logoColor=white)](https://ai.google.dev)
[![uv](https://img.shields.io/badge/uv-package_manager-DE5FE9?style=flat)](https://github.com/astral-sh/uv)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=flat)](LICENSE)

</div>

---

Four specialized AI agents collaborate in a **conditional DAG** to keep your ETL pipelines healthy — no human in the loop required.

```
              ┌──── PIPELINE_OK ─────────────────────────────────────┐
              │                                                       ▼
[Monitor] ───┤                                                   [Reporter]
              │                                                       ▲
              └── PIPELINE_FAIL ──► [Diagnostician] ──► [Remediation]┘
```

> The Monitor inspects the pipeline and routes to either an immediate report (healthy) or a full diagnosis-and-repair cycle (failed).

---

## ✨ What it does

| Scenario injected | What the agents do |
|---|---|
| Schema drift (type change, missing column) | Diagnose → apply schema fix → rerun stages |
| Duplicate records / PK violations | Detect → deduplicate → reload |
| Null data quality issues | Identify column → fill defaults → rerun |
| Missing source file | Detect → rollback → report |
| Clean run | Monitor confirms healthy → Reporter issues green report |

Every run ends with a **structured incident report** printed to the terminal.

---

## 🏗️ Agent Architecture

| Agent | Role | Tools |
|---|---|---|
| **Monitor** | First responder — health checks | `get_pipeline_status`, `inspect_pipeline_run`, `check_data_quality`, `get_stage_metrics` |
| **Diagnostician** | Root cause analyst | `analyze_logs`, `check_schema_drift`, `trace_error_chain`, `compare_data_samples` |
| **Remediation** | Applies fixes | `apply_schema_fix`, `deduplicate_records`, `fill_null_defaults`, `rerun_stage`, `rollback_stage` |
| **Reporter** | Incident reporting | `format_incident_report` |

**Orchestration**: Microsoft AutoGen 0.4+ `GraphFlow` with `DiGraphBuilder` conditional edges.
**LLM**: Google Gemini 2.5 Flash via the OpenAI-compatible endpoint.
**Tool binding**: Factory/closure pattern — `make_*_tools(state)` — no global mutable state.

---

## 🚀 Quick start

```bash
# 1. Clone
git clone https://github.com/Ioannis-Stamatakis/autogen-etl-monitor.git
cd autogen-etl-monitor

# 2. Set your Gemini API key
cp .env.example .env
# Edit .env → GEMINI_API_KEY=your-key-here

# 3. Install (uv creates the venv automatically)
uv sync

# 4. Run all scenarios
uv run python -m etl_monitor.main

# Or run a single scenario
uv run python -m etl_monitor.main pk_constraint_violation
uv run python -m etl_monitor.main schema_drift_type_change
uv run python -m etl_monitor.main healthy
```

Get a free Gemini API key at [aistudio.google.com](https://aistudio.google.com).

---

## 📋 Failure scenarios

```bash
uv run python -m etl_monitor.main healthy                   # Clean run → 2 agents
uv run python -m etl_monitor.main schema_drift_new_column   # Unexpected column upstream
uv run python -m etl_monitor.main schema_drift_type_change  # unit_price: float → string
uv run python -m etl_monitor.main schema_drift_missing_column  # Required column dropped
uv run python -m etl_monitor.main data_quality_nulls        # ~30% null emails
uv run python -m etl_monitor.main data_quality_duplicates   # ~15% duplicate records
uv run python -m etl_monitor.main missing_source_file       # Product catalog missing
uv run python -m etl_monitor.main pk_constraint_violation   # Duplicate transaction IDs
```

---

## 🧪 Tests

```bash
# Unit tests — no API key required (50 tests)
uv run pytest tests/test_pipeline.py tests/test_tools.py -v

# Integration test — requires GEMINI_API_KEY
uv run pytest tests/test_workflow.py -v
```

---

## 📁 Project structure

```
autogen-etl-monitor/
├── src/etl_monitor/
│   ├── config.py            # Gemini client (OpenAIChatCompletionClient)
│   ├── main.py              # Entry point — scenario runner
│   ├── agents/
│   │   ├── monitor.py
│   │   ├── diagnostician.py
│   │   ├── remediation.py
│   │   └── reporter.py
│   ├── tools/
│   │   ├── inspection.py    # Pipeline health check tools
│   │   ├── log_analysis.py  # Root cause analysis tools
│   │   ├── remediation.py   # Fix application tools
│   │   └── reporting.py     # Incident report formatter
│   ├── pipeline/
│   │   ├── state.py         # Core dataclasses (PipelineState, StageResult …)
│   │   ├── simulator.py     # Runs pipeline stages with failure injection
│   │   ├── stages.py        # Ingestion / Transform / Load implementations
│   │   └── failures.py      # 8 failure scenario registry
│   ├── team/workflow.py     # GraphFlow DAG — conditional routing
│   └── display/console.py  # Rich terminal panels
├── data/                    # Synthetic sales CSV/JSON + JSON schemas
├── tests/                   # pytest unit + integration tests
├── pyproject.toml
└── .env.example
```

---

## 🔧 Gemini integration

AutoGen doesn't have a native Gemini client, but Gemini exposes an OpenAI-compatible endpoint:

```python
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_core.models import ModelInfo

client = OpenAIChatCompletionClient(
    model="models/gemini-2.5-flash",
    api_key=GEMINI_API_KEY,
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    model_info=ModelInfo(
        vision=False, function_calling=True,
        json_output=True, family="gemini", structured_output=True,
    ),
)
```

---

## 📦 Dependencies

| Package | Purpose |
|---|---|
| `autogen-agentchat>=0.4` | Multi-agent orchestration, GraphFlow |
| `autogen-ext[openai]>=0.4` | OpenAI-compatible model client |
| `pandas>=2.2` | ETL simulator data processing |
| `python-dotenv>=1.0` | API key management |
| `rich>=13.0` | Terminal formatting |

---

<div align="center">
Built with <a href="https://github.com/microsoft/autogen">Microsoft AutoGen</a> · Powered by <a href="https://ai.google.dev">Google Gemini</a>
</div>
