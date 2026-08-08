# 🛡️ DataOps SWAT Team

**Autonomous incident response for data pipelines — powered by DataHub**

[Demo Video](TODO: add link after recording) | [Devpost Submission](TODO: add link after submitting)

## The Problem

Data pipeline incidents (schema drift, freshness violations, ownership gaps) are detected too late, diagnosed too slowly, and fixed manually. The average MTTR for a broken dbt model is 4+ hours.

On-call engineers chase logs across disconnected tools. Nobody notices a stale table until a dashboard breaks. When something finally does break, the diagnosis is tribal knowledge and the fix is hand-written SQL that ships without any check against the rest of the data graph.

## The Solution

A multi-agent SWAT team that autonomously detects, diagnoses, engineers fixes for, and validates data incidents — all using DataHub as the single source of metadata truth.

When a pipeline incident happens, four specialized agents take over:

1. **Sentry** scans DataHub metadata and flags anomalies the moment they appear.
2. **Detective** traces lineage upstream to pinpoint the root cause with a confidence score.
3. **Engineer** writes a concrete SQL fix grounded in the dataset's real schema.
4. **Validator** proves the fix is safe before it touches anything downstream.

A Streamlit **Mission Control** dashboard gives operators full visibility and one-click control — detect, run the full pipeline, inspect diagnosis, review SQL, and approve or escalate. The whole loop turns a 4-hour MTTR into a four-stage pipeline that runs in minutes.

## Architecture

- **Sentry Agent** — Scans DataHub metadata to detect anomalies (freshness, ownership, lineage, schema gaps)
- **Detective Agent** — Traces lineage upstream to identify root causes with confidence scores
- **Engineer Agent** — Generates SQL fixes using real DataHub schema context + LLM (OpenRouter/Ollama)
- **Validator Agent** — Validates fixes against downstream lineage before deployment
- **DataHub MCP Client** — Async GraphQL wrapper for all metadata queries
- **SWAT Orchestrator** — Coordinates the 4-agent pipeline with real-time progress
- **Streamlit UI** — Mission control dashboard with live incident tracking

```
DataHub OSS (GraphQL)
        │
        ▼
┌──────────────────────────────┐
│   DataHub MCP Client         │   async GraphQL wrapper
└──────────────────────────────┘
        │
        ▼
┌──────────────────────────────┐
│   SWAT Orchestrator          │   EventBus + IncidentStore + state machine
└──────────────────────────────┘
   │         │          │           │
   ▼         ▼          ▼           ▼
 Sentry   Detective   Engineer   Validator
 DETECT    DIAGNOSE   ENGINEER   VALIDATE
        │
        ▼
┌──────────────────────────────┐
│   Streamlit Mission Control  │   4 tabs: incidents, detail, lineage, fixes
└──────────────────────────────┘
```

## Tech Stack

- Python 3.12+, Pydantic v2, asyncio
- DataHub OSS (GraphQL API)
- Streamlit (UI)
- OpenRouter / Ollama (LLM gateway)
- sqlparse (SQL validation)

## Quick Start

### Prerequisites

- DataHub OSS running (see [DataHub Quickstart](https://datahubproject.io/docs/quickstart))
- Python 3.12+
- OpenRouter API key (optional — template fallback works without it)

### Install

```bash
git clone https://github.com/jlorow/dataops-swat-team.git
cd dataops-swat-team
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configure

Configuration is read from environment variables (see `.env.example`). No `.env` autoloading — export them in your shell:

```bash
# Point at your DataHub GMS (defaults to http://localhost:8080)
export DATAHUB_GMS_URL=http://localhost:8080

# Optional: enables LLM-generated SQL fixes (otherwise deterministic templates)
export OPENROUTER_API_KEY=sk-or-v1-...
```

To stand up DataHub OSS locally, this repo ships a full compose stack (GMS, frontend, MySQL, Kafka, Elasticsearch):

```bash
docker compose up -d
# wait for GMS health on http://localhost:8080, UI on http://localhost:9002
```

### Run the pipeline headless

```bash
python - <<'EOF'
import asyncio
from src.orchestrator.swat_orchestrator import SWATOrchestrator

async def main():
    orch = SWATOrchestrator()
    async for stage in orch.run_full_pipeline(detect_limit=20):
        print(f"[{stage.stage}] {stage.status}: {stage.result_summary}")

asyncio.run(main())
EOF
```

### Run the Mission Control UI

```bash
streamlit run ui/app.py
```

Then in the browser:

1. **Sidebar → 🚨 Run Full Pipeline** — watch Detect → Diagnose → Engineer → Validate progress live.
2. **Active Incidents** tab — inspect every incident, or trigger a one-off **Run Detection**.
3. **Incident Detail** tab — root cause, confidence, owner, fix SQL, full agent log.
4. **Lineage Graph** tab — real DataHub lineage: victim red, upstreams orange, downstreams green.
5. **Fix Preview** tab — review generated SQL + validation report, then **Approve** or **Escalate**.

## Project Structure

```
├── src/
│   ├── agents/            # Sentry, Detective, Engineer, Validator agents
│   ├── datahub/           # DataHubMCPClient (async GraphQL)
│   ├── llm/               # OpenRouter + Ollama gateway
│   ├── models/            # Pydantic v2 schemas (Incident, Diagnosis, Fix, Events)
│   ├── orchestrator/      # SWATOrchestrator, EventBus, IncidentStore, state machine
│   └── main.py            # CLI entry point
├── ui/
│   └── app.py             # Streamlit Mission Control dashboard
├── examples/              # Real pipeline artifacts (see below)
├── tests/                 # 93 unit tests
├── docker-compose.yml     # DataHub OSS stack
└── requirements.txt
```

## Example Outputs

The [`examples/`](examples/) directory contains a **real end-to-end run** against a live DataHub instance:

| File | What it shows |
|------|---------------|
| [`examples/pipeline_run.log`](examples/pipeline_run.log) | All 4 stages: 5 incidents detected, diagnosed, 2 fixes generated, 2 validated |
| [`examples/incident_walkthrough.md`](examples/incident_walkthrough.md) | One incident followed through the full lifecycle |
| [`examples/sample_fix.sql`](examples/sample_fix.sql) | A real generated SQL fix |
| [`examples/validation_report.json`](examples/validation_report.json) | The Validator's full safety report (score 1.0 → DEPLOY) |
| [`examples/lineage.dot`](examples/lineage.dot) | Real lineage graph (victim red, upstreams orange) |

## Testing

```bash
pytest tests/     # 93 tests, no network required
```

## Troubleshooting

- **"DataHub unreachable"** — confirm `DATAHUB_GMS_URL` points at a live GMS and the host is reachable (`curl http://<host>:8080/api/graphql`). The UI degrades gracefully and shows a clear error; the rest of the dashboard keeps working.
- **No SQL fixes generated** — the Engineer needs incidents in `ROOT_CAUSE_IDENTIFIED` state; run the full pipeline, and set `OPENROUTER_API_KEY` for LLM-generated fixes (deterministic templates work without it).
- **Empty dashboard** — run **Run Detection** first; the incident store starts empty.

## Roadmap

- [ ] Auto-deploy validated fixes via GitHub PR (PR client scaffolding in `src/github/`)
- [ ] True real-time stage streaming in the UI (background workers + polling)
- [ ] Slack/on-call notifications on high-severity incidents
- [ ] Historical MTTR analytics

## License

Apache License 2.0 — see [LICENSE](LICENSE).
