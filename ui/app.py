"""DataOps SWAT Team — Mission Control dashboard (Streamlit).

Real orchestrator integration:

- Tab 1 lists live incidents from the IncidentStore (with Run Detection).
- Tab 2 shows full incident detail: status badge, diagnosis, fix SQL, agent logs.
- Tab 3 renders a real DataHub lineage graph (victim red / upstream orange /
  downstream green) via Graphviz.
- Tab 4 previews generated fixes with validation results and manual
  Approve / Escalate controls.
- Sidebar shows live pipeline stage progress and can run the full pipeline.

The app never crashes when DataHub is unreachable — it surfaces a clear error
and keeps the empty state visible.
"""
import asyncio
import os
import re
import uuid
from datetime import datetime, timezone

import streamlit as st

from src.agents.sentry_agent import _display_name_from_urn
from src.datahub.mcp_client import DataHubMCPClient, DataHubMCPError
from src.models import FailureType, Incident, IncidentStatus
from src.orchestrator import IncidentStateMachine
from src.orchestrator.swat_orchestrator import SWATOrchestrator

GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://67.205.141.90:8080")

st.set_page_config(page_title="DataOps SWAT Team", page_icon="🛡️", layout="wide")

# --------------------------------------------------------------------------
# Header (unchanged)
# --------------------------------------------------------------------------
st.title("🛡️ DataOps SWAT Team — Mission Control")
st.subheader("AI-powered incident response for data pipelines")
st.divider()

# --------------------------------------------------------------------------
# Shared helpers
# --------------------------------------------------------------------------
STAGE_AGENTS = {
    "DETECT": "🛡️ Sentry",
    "DIAGNOSE": "🔍 Detective",
    "ENGINEER": "🔧 Engineer",
    "VALIDATE": "✅ Validator",
}
STATUS_ICONS = {
    "PENDING": "⚪",
    "RUNNING": "🟡",
    "COMPLETE": "🟢",
    "FAILED": "🔴",
    "SKIPPED": "⚪",
}

STATUS_COLORS = {
    "DETECTED": "red",
    "DIAGNOSING": "goldenrod",
    "ROOT_CAUSE_IDENTIFIED": "teal",
    "FIXING": "blue",
    "FIX_PROPOSED": "indigo",
    "VALIDATING": "purple",
    "READY_TO_DEPLOY": "green",
    "RESOLVED": "green",
    "ESCALATED": "orange",
}


def _in_runtime() -> bool:
    """True when executing inside the Streamlit runtime (not a plain import)."""
    try:
        return st.runtime.exists()
    except Exception:
        return False


def _run(coro):
    """Run an async coroutine to completion from the sync Streamlit script."""
    return asyncio.run(coro)


def get_orchestrator() -> SWATOrchestrator:
    """Build once, reuse for the whole session (no re-connect on rerun)."""
    if "orch" not in st.session_state:
        st.session_state["orch"] = SWATOrchestrator(
            gms_url=GMS_URL,
            llm_api_key=os.getenv("OPENROUTER_API_KEY") or None,
        )
    return st.session_state["orch"]


def refresh() -> None:
    """Reload the incident list from the store into session state."""
    orch = get_orchestrator()
    st.session_state["incidents"] = orch.get_incidents()
    st.session_state["last_refresh"] = datetime.now().strftime("%H:%M:%S")


def status_badge(status: str) -> str:
    """Render a colored status badge via HTML."""
    color = STATUS_COLORS.get(status, "gray")
    return (
        f'<span style="background-color:{color}; color:white; padding:2px 10px; '
        f'border-radius:12px; font-weight:600;">{status}</span>'
    )


def _severity_of(incident: Incident) -> str:
    """Severity is recorded by Sentry in the first agent log."""
    for log in incident.agent_logs:
        match = re.search(r"severity=(\w+)", log.input_summary)
        if match:
            return match.group(1)
    return "—"


def _duration_of(incident: Incident) -> str:
    delta = datetime.utcnow() - incident.created_at
    return str(delta).split(".")[0]


def flash(message: str, kind: str = "success") -> None:
    """Queue a one-shot banner that survives the upcoming ``st.rerun()``.

    Streamlit rebuilds the whole element tree on rerun, so any ``st.error`` /
    ``st.success`` written before ``st.rerun()`` is silently discarded. Banners
    are stored in session state and rendered by ``render_flash()`` on the next
    run instead.
    """
    st.session_state["flash"] = (kind, message)


def render_flash() -> None:
    """Render any queued one-shot banner (cleared once shown)."""
    pending = st.session_state.pop("flash", None)
    if pending is None:
        return
    kind, message = pending
    if kind == "error":
        st.error(message)
    elif kind == "warning":
        st.warning(message)
    elif kind == "info":
        st.info(message)
    else:
        st.success(message)


# --------------------------------------------------------------------------
# Sidebar — Agent Status / controls
# --------------------------------------------------------------------------
def render_sidebar() -> None:
    with st.sidebar:
        st.header("Agent Status")
        orch = get_orchestrator()
        stages = {stage.stage: stage for stage in orch.get_pipeline_status()}
        for stage_name, agent_label in STAGE_AGENTS.items():
            stage = stages.get(stage_name)
            if stage is None or stage.status == "PENDING":
                st.markdown(f"**{agent_label}** — ⚪ Idle")
                continue
            detail = f" ({stage.incident_count})" if stage.incident_count else ""
            st.markdown(
                f"**{agent_label}** — {STATUS_ICONS.get(stage.status, '⚪')} "
                f"{stage.status}{detail}"
            )
        st.divider()

        if st.button("🚀 Run Full Pipeline", type="primary", width="stretch"):
            run_full_pipeline_ui()
        st.divider()
        if st.button("🧪 Inject Test Incident", width="stretch"):
            inject_test_incident()


def run_full_pipeline_ui() -> None:
    """Run the 4-stage pipeline with live progress in the sidebar."""
    orch = get_orchestrator()
    progress = st.sidebar.empty()

    async def go() -> None:
        async for _stage in orch.run_full_pipeline(detect_limit=20):
            lines = []
            for stage in orch.get_pipeline_status():
                icon = STATUS_ICONS.get(stage.status, "⚪")
                summary = f" — {stage.result_summary}" if stage.result_summary else ""
                lines.append(f"- **{STAGE_AGENTS.get(stage.stage, stage.stage)}**: {icon} {stage.status}{summary}")
            progress.markdown("\n".join(lines))

    try:
        with st.spinner("Running Detect → Diagnose → Engineer → Validate..."):
            _run(go())
        flash("Pipeline complete ✅ — all four agents finished.")
    except DataHubMCPError as exc:
        flash(f"DataHub unreachable: {exc}", "error")
    except Exception as exc:  # noqa: BLE001 - UI must never crash
        flash(f"Pipeline failed: {exc}", "error")
    refresh()
    st.rerun()


def inject_test_incident() -> None:
    """Create a real DETECTED Incident in the store for a real DataHub dataset."""
    orch = get_orchestrator()
    try:
        datasets = _run(orch.mcp.search_datasets("*", count=10))
        if datasets:
            urn, label = datasets[0].urn, datasets[0].name or _display_name_from_urn(datasets[0].urn)
        else:
            urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,manual.test_dataset,PROD)"
            label = "manual.test_dataset"
    except DataHubMCPError as exc:
        flash(f"DataHub unreachable — using a placeholder dataset instead: {exc}", "warning")
        urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,manual.test_dataset,PROD)"
        label = "manual.test_dataset"
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not look up a dataset: {exc}")
        return

    incident = Incident(
        id=f"inc-manual-{uuid.uuid4().hex[:8]}",
        status=IncidentStatus.DETECTED,
        victim_urn=urn,
        failure_type=FailureType.SCHEMA_DRIFT,
    )
    orch.store.create(incident)
    refresh()
    flash(f"Test incident injected for `{label}` (status DETECTED)")
    st.rerun()


# --------------------------------------------------------------------------
# Tab 1 — Active Incidents
# --------------------------------------------------------------------------
def render_active(orch: SWATOrchestrator) -> None:
    st.subheader("Active Incidents")
    col_refresh, col_detect, col_info = st.columns([1, 1, 3])
    with col_refresh:
        if st.button("🔄 Refresh", width="stretch"):
            refresh()
            st.rerun()
    with col_detect:
        if st.button("🚨 Run Detection", type="primary", width="stretch"):
            try:
                created = _run(orch.detect(limit=20))
                refresh()
                if created:
                    flash(f"Detected {len(created)} new incident(s) from DataHub")
                else:
                    flash("No new incidents — all known anomalies are already tracked.", "info")
            except DataHubMCPError as exc:
                flash(f"DataHub unreachable: {exc}", "error")
            except Exception as exc:  # noqa: BLE001
                flash(f"Detection failed: {exc}", "error")
            st.rerun()
    with col_info:
        last = st.session_state.get("last_refresh", "—")
        st.caption(f"Last refreshed: {last}")

    incidents = st.session_state.get("incidents", [])
    if not incidents:
        st.info(
            "No incidents yet. Click **Run Detection** to scan DataHub, "
            "or **Run Full Pipeline** in the sidebar."
        )
        return

    rows = []
    for incident in incidents:
        rows.append(
            {
                "ID": incident.id,
                "Status": incident.status.value,
                "Type": incident.failure_type.value,
                "Dataset": _display_name_from_urn(incident.victim_urn),
                "Severity": _severity_of(incident),
                "Duration": _duration_of(incident),
                "Created": incident.created_at.strftime("%Y-%m-%d %H:%M"),
            }
        )
    st.dataframe(rows, width="stretch", hide_index=True)
    st.caption(f"{len(incidents)} incident(s) tracked in the store.")


# --------------------------------------------------------------------------
# Tab 2 — Incident Detail
# --------------------------------------------------------------------------
def render_detail(orch: SWATOrchestrator) -> None:
    st.subheader("Incident Detail")
    incidents = st.session_state.get("incidents", [])
    if not incidents:
        st.info("No incidents to inspect yet — run detection first.")
        return

    options = {
        f"{incident.id} — {_display_name_from_urn(incident.victim_urn)}": incident.id
        for incident in incidents
    }
    selected_label = st.selectbox("Select Incident", list(options.keys()))
    detail = orch.get_incident_detail(options[selected_label])
    if detail is None:
        st.warning("Incident not found in the store.")
        return

    incident = detail["incident"]
    diagnosis = detail["diagnosis"]
    fix = detail["fix_report"]

    col_badge, col_owner = st.columns(2)
    with col_badge:
        st.markdown("**Status**")
        st.markdown(status_badge(incident.status.value), unsafe_allow_html=True)
    with col_owner:
        st.markdown("**Owner**")
        st.markdown(diagnosis.owner_email if diagnosis and diagnosis.owner_email else "— (unassigned)")

    st.markdown("**Dataset URN**")
    st.code(incident.victim_urn)

    st.markdown("**Root Cause**")
    if diagnosis:
        st.markdown(diagnosis.summary_text or "No summary recorded.")
        st.caption(
            f"Confidence: {diagnosis.confidence_score} | Recommended fix: "
            f"{diagnosis.recommended_fix_type} | Impact: {diagnosis.impact_assessment}"
        )
    else:
        st.markdown("Not diagnosed yet — run the pipeline to trigger the Detective Agent.")

    if fix:
        st.markdown("**Fix SQL**")
        st.code(fix.sql_code, language="sql")

    st.markdown("**Agent Log**")
    with st.expander(f"View agent log for {incident.id}", expanded=True):
        logs = list(incident.agent_logs)
        events = detail.get("events") or []
        if not logs and not events:
            st.markdown("No agent activity recorded yet.")
        for log in logs:
            ts = log.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            output = log.output_summary or log.input_summary
            st.markdown(f"`{ts}` — **{log.agent_name}** ({log.action}): {output}")
        for event in events:
            ts = event.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            message = event.payload.get("message", "")
            st.markdown(
                f"`{ts}` — 📡 **{event.agent_name}** [{event.event_type.value}]: {message}"
            )


# --------------------------------------------------------------------------
# Tab 3 — Lineage Graph (real DataHub lineage)
# --------------------------------------------------------------------------
def _build_dot(urn: str, upstreams: list, downstreams: list) -> str:
    """Build a Graphviz DOT graph: victim red, upstreams orange, downstreams green."""
    victim = _display_name_from_urn(urn).replace('"', '\\"')
    lines = [
        "digraph G {",
        "  rankdir=LR;",
        f'  "{urn}" [color=red, style=filled, fillcolor=lightcoral, '
        f'label="{victim}\\n(victim)"];',
    ]
    for edge in upstreams:
        name = (edge.name or _display_name_from_urn(edge.urn)).replace('"', '\\"')
        lines.append(
            f'  "{edge.urn}" [color=orange, style=filled, fillcolor=lightyellow, '
            f'label="{name}"];'
        )
        lines.append(f'  "{edge.urn}" -> "{urn}";')
    for edge in downstreams:
        name = (edge.name or _display_name_from_urn(edge.urn)).replace('"', '\\"')
        lines.append(
            f'  "{edge.urn}" [color=green, style=filled, fillcolor=lightgreen, '
            f'label="{name}"];'
        )
        lines.append(f'  "{urn}" -> "{edge.urn}";')
    if not upstreams and not downstreams:
        lines.append(f'  "{urn}" [label="{victim}\\n(no lineage registered)"];')
    lines.append("}")
    return "\n".join(lines)


@st.cache_data(ttl=120, show_spinner=False)
def _lineage_dot(urn: str) -> tuple:
    """Fetch real lineage for a dataset (cached per URN) and return (dot, error)."""
    try:
        client = DataHubMCPClient(gms_url=GMS_URL)

        async def fetch():
            lineage = await client.get_dataset_lineage(urn)
            return lineage

        lineage = asyncio.run(fetch())
    except DataHubMCPError as exc:
        return "", f"DataHub unreachable: {exc}"
    except Exception as exc:  # noqa: BLE001
        return "", f"Could not fetch lineage: {exc}"
    return _build_dot(urn, lineage.upstreams, lineage.downstreams), ""


def render_lineage(orch: SWATOrchestrator) -> None:
    st.subheader("Lineage Graph")
    incidents = st.session_state.get("incidents", [])
    if not incidents:
        st.info("No incidents yet — nothing to trace. Run detection first.")
        return

    options = {
        f"{incident.id} — {_display_name_from_urn(incident.victim_urn)}": incident.id
        for incident in incidents
    }
    selected_label = st.selectbox("Select incident to trace", list(options.keys()))
    selected_id = options[selected_label]
    urn = next(incident.victim_urn for incident in incidents if incident.id == selected_id)

    dot, error = _lineage_dot(urn)
    if error:
        st.error(error)
        st.info("Lineage is unavailable right now — the rest of the dashboard still works.")
        return
    st.caption("Red = incident dataset · Orange = upstream producers · Green = downstream consumers")
    st.graphviz_chart(dot)


# --------------------------------------------------------------------------
# Tab 4 — Fix Preview (with manual approve / escalate)
# --------------------------------------------------------------------------
def _approve_fix(orch: SWATOrchestrator, incident: Incident) -> None:
    machine = IncidentStateMachine(incident)
    try:
        if incident.status == IncidentStatus.FIX_PROPOSED:
            machine.transition_to(IncidentStatus.VALIDATING)
            machine.transition_to(IncidentStatus.READY_TO_DEPLOY)
        elif incident.status == IncidentStatus.VALIDATING:
            machine.transition_to(IncidentStatus.READY_TO_DEPLOY)
        else:
            flash(f"{incident.id} is already {incident.status.value}.", "info")
            return
        orch.store.update(incident)
        flash(f"{incident.id} approved → READY_TO_DEPLOY")
    except ValueError as exc:
        flash(f"Cannot approve from {incident.status.value}: {exc}", "error")


def _escalate_incident(orch: SWATOrchestrator, incident: Incident) -> None:
    machine = IncidentStateMachine(incident)
    try:
        if incident.status in (IncidentStatus.FIX_PROPOSED, IncidentStatus.VALIDATING):
            machine.transition_to(IncidentStatus.ESCALATED)
        else:
            flash(f"{incident.id} is already {incident.status.value}.", "info")
            return
        orch.store.update(incident)
        flash(f"{incident.id} escalated → ESCALATED")
    except ValueError as exc:
        flash(f"Cannot escalate from {incident.status.value}: {exc}", "error")


def render_fix(orch: SWATOrchestrator) -> None:
    st.subheader("Fix Preview")
    incidents = st.session_state.get("incidents", [])
    fixable = [
        incident
        for incident in incidents
        if incident.fix is not None
        and incident.status
        in (IncidentStatus.FIX_PROPOSED, IncidentStatus.VALIDATING, IncidentStatus.READY_TO_DEPLOY)
    ]
    if not fixable:
        st.info(
            "No fixes ready yet. Run the full pipeline until the Engineer Agent "
            "generates fixes for diagnosed incidents."
        )
        return

    options = {
        f"{incident.id} — {_display_name_from_urn(incident.victim_urn)} ({incident.status.value})": incident.id
        for incident in fixable
    }
    selected_label = st.selectbox("Select fix to review", list(options.keys()))
    incident = next(inc for inc in fixable if inc.id == options[selected_label])
    fix = incident.fix

    st.code(fix.sql_code, language="sql")
    st.caption(
        f"Fix type: {fix.fix_type} | Generated: "
        f"{fix.generated_at.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    detail = orch.get_incident_detail(incident.id)
    validation = detail.get("validation_result") if detail else None
    if validation:
        score = validation.get("safety_score")
        recommendation = validation.get("recommendation")
        col1, col2, col3 = st.columns(3)
        col1.metric("Safety Score", f"{score:.2f}" if score is not None else "—")
        col2.metric("Recommendation", recommendation or "—")
        col3.metric("Breaking Changes", len(validation.get("breaking_changes", [])))
        with st.expander("Validation details"):
            st.json(validation)
    else:
        st.info("This fix has not been validated yet — run the full pipeline to trigger the Validator Agent.")

    col_approve, col_escalate = st.columns(2)
    with col_approve:
        if st.button(
            "✅ Approve for deploy",
            type="primary",
            width="stretch",
            disabled=incident.status == IncidentStatus.READY_TO_DEPLOY,
        ):
            _approve_fix(orch, incident)
            refresh()
            st.rerun()
    with col_escalate:
        if st.button(
            "🚫 Escalate",
            width="stretch",
            disabled=incident.status == IncidentStatus.ESCALATED,
        ):
            _escalate_incident(orch, incident)
            refresh()
            st.rerun()


# --------------------------------------------------------------------------
# Main content — Tabs
# --------------------------------------------------------------------------
def main() -> None:
    orch = get_orchestrator()
    refresh()
    render_flash()

    tab_active, tab_detail, tab_lineage, tab_fix = st.tabs(
        ["Active Incidents", "Incident Detail", "Lineage Graph", "Fix Preview"]
    )
    with tab_active:
        render_active(orch)
    with tab_detail:
        render_detail(orch)
    with tab_lineage:
        render_lineage(orch)
    with tab_fix:
        render_fix(orch)


# Only run the full app inside the Streamlit runtime so plain imports (e.g.
# headless smoke tests) stay fast and side-effect free.
if _in_runtime():
    main()

# --------------------------------------------------------------------------
# Footer (unchanged)
# --------------------------------------------------------------------------
st.divider()
st.caption("Built for DataHub Agent Hackathon | DataOps SWAT Team v0.1")
