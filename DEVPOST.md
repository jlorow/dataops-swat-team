# Devpost Submission — DataOps SWAT Team

Copy-paste-ready fields for the Devpost submission. Links marked `TODO` need final values (demo video, repo URL if private).

---

## Title

🛡️ DataOps SWAT Team — Autonomous Incident Response for Data Pipelines

## Tagline

Four AI agents detect, diagnose, engineer fixes for, and validate data pipeline incidents — with DataHub as the single source of metadata truth.

## Elevator Pitch

When a data pipeline breaks, it takes teams hours to notice, diagnose, and fix it. DataOps SWAT Team is a multi-agent system that does it autonomously: Sentry detects anomalies in DataHub metadata, Detective traces lineage to find the root cause, Engineer writes a schema-aware SQL fix, and Validator proves the fix is safe before an operator clicks approve — all from a live Mission Control dashboard that turns a 4-hour MTTR into a four-stage pipeline that runs in minutes.

## About (project description)

Data pipeline incidents — schema drift, freshness violations, ownership gaps — are detected too late, diagnosed too slowly, and fixed manually. On-call engineers chase logs across disconnected tools; nobody notices a stale table until a dashboard breaks; and the eventual fix is hand-written SQL shipped without any check against the rest of the data graph.

DataOps SWAT Team solves this by treating DataHub's metadata graph as the source of truth and giving a team of specialized agents a mission:

1. **🛡️ Sentry Agent** continuously scans DataHub metadata and flags anomalies the moment they appear (freshness violations, ownership gaps, lineage gaps, schema drift).
2. **🔍 Detective Agent** traces lineage upstream to pinpoint the root cause, returning a confidence score, impacted datasets, and the affected owner.
3. **🔧 Engineer Agent** generates a concrete SQL fix grounded in the dataset's real schema — either via an LLM (OpenRouter) or deterministic, schema-aware templates.
4. **✅ Validator Agent** proves the fix is safe before deployment: syntax check via sqlparse, schema diff against DataHub's actual fields, and a downstream-lineage impact analysis. It scores every fix and either recommends deploy or blocks it.

A Streamlit **Mission Control** dashboard ties it all together: live incident tracking, full incident detail with agent logs, real DataHub lineage visualization (victim red / upstreams orange / downstreams green), and fix preview with manual approve-or-escalate controls. Operators stay in the loop; agents do the grunt work.

## How We Built It

**Architecture:** a Python 3.12 async codebase. A `DataHubMCPClient` wraps DataHub OSS's GraphQL API (search, schema, lineage, ownership) behind typed Pydantic models. A `SWATOrchestrator` runs the four-agent pipeline with per-stage progress reporting, exception isolation (one failed stage can't kill the run), and a JSONL-persisted incident store. Agents communicate over an in-process event bus; an incident state machine enforces legal status transitions (DETECTED → DIAGNOSING → ROOT_CAUSE_IDENTIFIED → FIXING → FIX_PROPOSED → VALIDATING → READY_TO_DEPLOY).

**DataHub as the backbone:** every agent reads live metadata — Sentry queries dataset properties and freshness; Detective walks upstream lineage and ownership; Engineer reads the real schema to write column-aware SQL; Validator re-checks schema and downstream lineage to catch breaking changes. Lineage drives both root-cause analysis and the UI's live graph.

**Resilience by design:** the whole UI degrades gracefully if DataHub is unreachable — clear error messages, empty states, no crashes. The LLM is optional: with `OPENROUTER_API_KEY` set, Engineer writes LLM-generated fixes; without it, it falls back to deterministic SQL templates. That means the full pipeline runs (and demoes) with zero external dependencies beyond DataHub.

**Stack:** Python 3.12, Pydantic v2, asyncio, DataHub OSS (GraphQL), Streamlit, OpenRouter/Ollama LLM gateway, sqlparse, 93 unit tests.

## Inspirations

- The pain of being the "data on-call" — MTTR for broken dbt models measured in hours while executives watch dashboards go stale.
- DataHub's metadata graph as the untapped source of truth: everything needed to diagnose an incident was already there, nobody was asking it questions.
- Classic incident-response team structure (a SWAT team) mapped onto AI agents: separate roles, clear handoffs, a human commander in the loop.

## Challenges We Ran Into

- **DataHub's GraphQL is finicky:** search takes a singular `type` (not `types`), lineage lives on the `Dataset` entity rather than a root query, and owner types resolve to a union. We validated every query against a live GMS instance with real introspection before wiring it into agents.
- **Streamlit async:** agent calls are async but Streamlit is sync — we wrapped the pipeline in `asyncio.run` and surfaced real-time stage updates through the sidebar.
- **Messages getting eaten by `st.rerun()`:** Streamlit rebuilds the element tree on rerun, silently discarding `st.error`/`st.success`. We built a session-state flash mechanism so the "DataHub unreachable" error actually reaches the user.
- **State machines are strict:** our fakes in unit tests skipped legal transitions and blew up the pipeline; aligning them with the real DETECTED→…→READY_TO_DEPLOY chain surfaced a bug before it ever hit production.

## Accomplishments That We're Proud Of

- A real end-to-end run: 5 incidents detected from live DataHub metadata, all diagnosed with root causes and confidence scores, SQL fixes generated, and 2 fixes validated safe (score 1.0 → DEPLOY) — captured in `examples/`.
- Validator that actually catches breaking changes: it diffs every referenced/added/altered/dropped column against DataHub's real schema and checks 13 downstream datasets for impact.
- The whole thing runs with one command: `streamlit run ui/app.py`, and the pipeline works with or without an LLM key.

## What We Learned

- Metadata-first incident response is real: DataHub already knows about freshness, owners, lineage, and schema — you don't need a new monitoring stack, you need agents that can read the metadata you already have.
- Multi-agent design pays off when the agents have distinct, testable contracts (scan → investigate → fix → verify) instead of one big prompt.
- Resilience isn't optional for demos: every external dependency needs a graceful degradation path.

## What's Next

- Auto-deploy validated fixes as GitHub PRs (PR client scaffolding already in `src/github/`).
- True real-time stage streaming in the UI via background workers.
- Slack/on-call notifications for high-severity incidents.
- Historical MTTR analytics to prove the MTTR reduction.

## Built With

Python · Pydantic · asyncio · DataHub OSS (GraphQL API) · Streamlit · OpenRouter · Ollama · sqlparse · pytest · Docker

## Try It Out

- GitHub: `https://github.com/jlorow/dataops-swat-team` (branch `main`)
- Run: `pip install -r requirements.txt` → `streamlit run ui/app.py` → click **Run Full Pipeline** in the sidebar
- Real output artifacts: see the `examples/` directory in the repo (pipeline run log, incident walkthrough, sample SQL fix, validation report, lineage graph)
- Demo video: TODO — add link after recording

## Team

| Member | Role |
|--------|------|
| (Your name) | TODO |
| (Your name) | TODO |

## Links

- Demo video: TODO
- Repo: https://github.com/jlorow/dataops-swat-team
- Devpost project page: TODO (this page)
