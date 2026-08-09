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

Visual design: a DataHub-inspired dark NOC theme (pure CSS + HTML wrappers —
no application logic, state management, or DataHub behaviour is changed).
"""
import asyncio
import html
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
# Theme — DataHub-inspired NOC look (CSS only)
# --------------------------------------------------------------------------
_THEME_CSS = """
<style>
/* ==================== DataOps SWAT Team — DataHub NOC theme ==================== */
:root{
  --swat-bg:#0B1220; --swat-card:#111827; --swat-card2:#0F1526;
  --swat-border:rgba(255,255,255,0.08);
  --swat-accent:#2563EB; --swat-accent-hover:#3B82F6; --swat-accent-deep:#1D4ED8;
  --swat-success:#22C55E; --swat-muted:#9CA3AF; --swat-text:#E2E8F0; --swat-text-bright:#F8FAFC;
}

html, body, [data-testid="stAppViewContainer"]{ background:var(--swat-bg); }
[data-testid="stHeader"], [data-testid="stToolbar"]{ background:transparent; }

/* Subtle decorative backdrop: faint grid + data-flow bands + tiny glowing dots
   (all well under 5% opacity so content stays readable). */
[data-testid="stAppViewContainer"]::before{
  content:""; position:fixed; inset:0; pointer-events:none; z-index:0;
  background-image:
    radial-gradient(circle, rgba(59,130,246,0.07) 1.1px, transparent 1.6px),
    linear-gradient(rgba(255,255,255,0.022) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,0.022) 1px, transparent 1px),
    linear-gradient(115deg, transparent 42%, rgba(37,99,235,0.045) 46%, rgba(59,130,246,0.06) 50%, transparent 55%),
    linear-gradient(115deg, transparent 63%, rgba(37,99,235,0.04) 66%, transparent 70%);
  background-size:96px 96px, 100% 48px, 48px 100%, 100% 100%, 100% 100%;
}

[data-testid="stAppViewContainer"], [data-testid="stSidebar"]{
  font-family:-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
}
[data-testid="stMarkdownContainer"]{ font-size:15px; line-height:1.55; color:var(--swat-text); }

/* ---------- Hero header ---------- */
.swat-hero{
  display:flex; align-items:center; justify-content:space-between; gap:24px; flex-wrap:wrap;
  padding:38px 12px 30px; margin-bottom:10px;
  background:linear-gradient(180deg, rgba(37,99,235,0.16), rgba(37,99,235,0.03) 72%, transparent);
  border-bottom:1px solid var(--swat-border);
  border-radius:0 0 18px 18px;
  box-shadow:inset 0 0 70px rgba(37,99,235,0.10), 0 12px 44px -28px rgba(37,99,235,0.45);
}
.swat-hero-left{ display:flex; align-items:center; gap:20px; }
.swat-hero-icon{
  width:76px; height:76px; flex:none; display:flex; align-items:center; justify-content:center;
  font-size:38px; border-radius:20px;
  background:linear-gradient(135deg, var(--swat-accent), var(--swat-accent-deep));
  border:1px solid rgba(255,255,255,0.14);
  box-shadow:0 10px 30px rgba(37,99,235,0.45), inset 0 1px 0 rgba(255,255,255,0.25);
}
.swat-hero-title{ font-size:44px; font-weight:800; letter-spacing:-0.03em; line-height:1.05; color:var(--swat-text-bright); }
.swat-hero-subtitle{ font-size:16px; color:var(--swat-muted); margin-top:9px; }
.swat-hero-badge{
  display:inline-flex; align-items:center; gap:9px; padding:9px 18px; border-radius:999px;
  background:rgba(34,197,94,0.12); border:1px solid rgba(34,197,94,0.4); color:#4ADE80;
  font-weight:600; font-size:14px; white-space:nowrap;
  box-shadow:0 0 26px rgba(34,197,94,0.18);
}
.swat-hero-dot{ width:9px; height:9px; border-radius:50%; background:var(--swat-success); animation:swatPulse 2.4s ease-in-out infinite; }
@keyframes swatPulse{ 0%,100%{ box-shadow:0 0 5px var(--swat-success);} 50%{ box-shadow:0 0 13px var(--swat-success);} }

/* ---------- Navigation tabs ---------- */
[data-testid="stTabs"] [data-baseweb="tab-list"]{ gap:14px; border-bottom:1px solid var(--swat-border); margin-bottom:22px; }
[data-testid="stTabs"] [data-baseweb="tab"]{
  padding:11px 16px; color:var(--swat-muted); font-weight:500; font-size:15px;
  border-radius:8px 8px 0 0; transition:color .18s ease, background .18s ease;
}
[data-testid="stTabs"] [data-baseweb="tab"]:hover{ color:var(--swat-text-bright); background:rgba(37,99,235,0.08); }
[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"]{ color:var(--swat-text-bright); }
[data-testid="stTabs"] [data-baseweb="tab-highlight"]{ background:var(--swat-accent); height:3px; border-radius:3px 3px 0 0; }

/* ---------- Buttons ---------- */
div[data-testid="stButton"] > button{ border-radius:10px; font-weight:600; transition:all .18s ease; }
div[data-testid="stButton"] > button:disabled{ opacity:0.5; cursor:not-allowed; box-shadow:none; transform:none; }
button[data-testid="stBaseButton-primary"], div[data-testid="stButton"] > button[kind="primary"]{
  background:linear-gradient(135deg, var(--swat-accent), var(--swat-accent-deep));
  border:1px solid rgba(255,255,255,0.12); padding:0.7rem 1.2rem;
  box-shadow:0 5px 20px rgba(37,99,235,0.35);
}
button[data-testid="stBaseButton-primary"]:hover, div[data-testid="stButton"] > button[kind="primary"]:hover{
  background:linear-gradient(135deg, var(--swat-accent-hover), var(--swat-accent));
  box-shadow:0 8px 26px rgba(37,99,235,0.5); transform:translateY(-1px);
}
button[data-testid="stBaseButton-secondary"], div[data-testid="stButton"] > button[kind="secondary"]{
  background:transparent; color:#93C5FD; border:1px solid var(--swat-accent);
}
button[data-testid="stBaseButton-secondary"]:hover, div[data-testid="stButton"] > button[kind="secondary"]:hover{
  background:rgba(37,99,235,0.16); border-color:var(--swat-accent-hover); color:#DBEAFE;
}

/* ---------- Cards / containers ---------- */
[data-testid="stVerticalBlockBorderWrapper"]{
  background:var(--swat-card); border:1px solid var(--swat-border); border-radius:14px;
  box-shadow:0 12px 36px -18px rgba(0,0,0,0.6); transition:box-shadow .25s ease;
}
[data-testid="stVerticalBlockBorderWrapper"]:hover{ box-shadow:0 16px 44px -18px rgba(37,99,235,0.28); }
[data-testid="stVerticalBlockBorderWrapper"] > [data-testid="stVerticalBlock"]{ padding:24px; }
.swat-card-header{ display:flex; align-items:center; gap:12px; margin-bottom:8px; }
.swat-card-title{ font-size:24px; font-weight:700; color:var(--swat-text-bright); letter-spacing:-0.02em; }
.swat-count-badge{
  display:inline-flex; align-items:center; justify-content:center; min-width:30px; height:30px; padding:0 10px;
  border-radius:999px; background:linear-gradient(135deg, var(--swat-accent), var(--swat-accent-deep));
  color:#fff; font-size:13.5px; font-weight:700; box-shadow:0 3px 12px rgba(37,99,235,0.45);
}

/* ---------- Empty state ---------- */
.swat-empty{
  display:flex; flex-direction:column; align-items:center; text-align:center; gap:10px;
  padding:52px 32px; border-radius:14px;
  background:linear-gradient(180deg, rgba(37,99,235,0.10), rgba(37,99,235,0.04));
  border:1px dashed rgba(37,99,235,0.32);
  animation:swatFadeIn .4s ease;
}
.swat-empty-icon{ font-size:46px; opacity:0.95; }
.swat-empty-title{ font-size:20px; font-weight:700; color:var(--swat-text-bright); }
.swat-empty-desc{ font-size:14.5px; color:var(--swat-muted); max-width:480px; line-height:1.6; }
@keyframes swatFadeIn{ from{ opacity:0; transform:translateY(5px);} to{ opacity:1; transform:none;} }

/* ---------- Alerts / code / expanders / metrics / tables ---------- */
[data-testid="stAlert"]{
  background:var(--swat-card2) !important; border:1px solid var(--swat-border) !important;
  border-radius:12px !important; box-shadow:0 8px 22px -14px rgba(0,0,0,0.5); animation:swatFadeIn .35s ease;
}
[data-testid="stCodeBlock"], [data-testid="stCode"]{ background:#0A0F1E !important; border:1px solid var(--swat-border); border-radius:12px; }
[data-testid="stExpander"]{ background:var(--swat-card); border:1px solid var(--swat-border); border-radius:12px; box-shadow:0 8px 22px -14px rgba(0,0,0,0.5); }
[data-testid="stMetric"]{ background:var(--swat-card); border:1px solid var(--swat-border); border-radius:12px; padding:14px 16px; box-shadow:0 8px 22px -14px rgba(0,0,0,0.5); }
[data-testid="stDataFrame"]{
  border-radius:12px; overflow:hidden; border:1px solid var(--swat-border);
  background:var(--swat-card2); box-shadow:0 8px 24px -16px rgba(0,0,0,0.55);
}
[data-baseweb="select"] > div{ border-radius:10px !important; border-color:var(--swat-border) !important; background:var(--swat-card2) !important; }

/* ---------- Headings / misc ---------- */
[data-testid="stHeading"] h1{ font-size:44px; font-weight:800; color:var(--swat-text-bright); letter-spacing:-0.03em; }
[data-testid="stHeading"] h2{ font-size:30px; font-weight:700; color:var(--swat-text-bright); letter-spacing:-0.02em; }
[data-testid="stHeading"] h3{ font-size:28px; font-weight:700; color:var(--swat-text-bright); letter-spacing:-0.02em; }
[data-testid="stDivider"]{ border-color:var(--swat-border); }
[data-testid="stCaptionContainer"]{ color:var(--swat-muted); font-size:13px; }

/* ---------- Sidebar ---------- */
[data-testid="stSidebar"]{ background:linear-gradient(180deg, #0A0F1E, #0B1220); border-right:1px solid var(--swat-border); }
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"]{ font-size:14px; }
[data-testid="stSidebar"] [data-testid="stHeading"] h2{ font-size:20px; }
.swat-dh-card{
  margin-top:26px; padding:15px 16px; border-radius:12px;
  background:rgba(37,99,235,0.08); border:1px solid rgba(37,99,235,0.28);
}
.swat-dh-card .swat-dh-status{ font-weight:600; font-size:14px; color:#4ADE80; }
.swat-dh-card.offline .swat-dh-status{ color:#F87171; }
.swat-dh-card .swat-dh-sub{ font-size:12.5px; color:var(--swat-muted); margin-top:5px; line-height:1.5; }
.swat-dh-card .swat-dh-url{
  font-size:11.5px; color:#64748B; margin-top:7px;
  font-family:ui-monospace, SFMono-Regular, Menlo, monospace; word-break:break-all;
}

/* ---------- Footer ---------- */
.swat-footer{
  display:flex; justify-content:space-between; align-items:center; gap:16px;
  padding:18px 6px 10px; color:var(--swat-muted); font-size:13px;
}
.swat-footer span:last-child{ opacity:0.8; }
</style>
"""

# NOTE: only the {badge} placeholder may contain braces — this template is
# rendered with str.format().
_HERO_HTML = """
<div class="swat-hero">
  <div class="swat-hero-left">
    <div class="swat-hero-icon">🛡️</div>
    <div>
      <div class="swat-hero-title">DataOps SWAT Team — Mission Control</div>
      <div class="swat-hero-subtitle">AI-powered incident response for DataHub pipelines</div>
    </div>
  </div>
  {badge}
</div>
"""

_BADGE_HTML = (
    '<div class="swat-hero-badge"><span class="swat-hero-dot"></span>'
    " System Healthy</div>"
)

_FOOTER_HTML = (
    '<div class="swat-footer"><span>Built for DataHub Agent Hackathon</span>'
    "<span>Powered by DataHub</span></div>"
)


def _inject_theme(incidents: list) -> None:
    """Inject the theme CSS + hero header. Shows a health badge only when no incidents exist."""
    badge = _BADGE_HTML if not incidents else ""
    st.markdown(_THEME_CSS + _HERO_HTML.format(badge=badge), unsafe_allow_html=True)


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
    "DETECTED": "#EF4444",
    "DIAGNOSING": "#F59E0B",
    "ROOT_CAUSE_IDENTIFIED": "#06B6D4",
    "FIXING": "#2563EB",
    "FIX_PROPOSED": "#8B5CF6",
    "VALIDATING": "#A855F7",
    "READY_TO_DEPLOY": "#22C55E",
    "RESOLVED": "#22C55E",
    "ESCALATED": "#F97316",
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
    """Render a soft-pill status badge via HTML."""
    color = STATUS_COLORS.get(status, "#94A3B8")
    return (
        f'<span style="background:{color}1F; color:{color}; border:1px solid {color}55; '
        f'padding:3px 12px; border-radius:999px; font-weight:600; '
        f'font-size:12.5px;">{status}</span>'
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


@st.cache_data(ttl=60, show_spinner=False)
def _datahub_health() -> bool:
    """Lightweight cached probe (60s TTL) so the sidebar status card never hangs the UI."""
    try:
        return asyncio.run(DataHubMCPClient(gms_url=GMS_URL, timeout=4.0).healthcheck())
    except Exception:  # noqa: BLE001 - the probe must never raise
        return False


# --------------------------------------------------------------------------
# Sidebar — Agent Status / controls / connection card
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

        healthy = _datahub_health()
        cls = "swat-dh-card" if healthy else "swat-dh-card offline"
        status = "🟢 DataHub Connected" if healthy else "🔴 DataHub Offline"
        sub = (
            "Real-time metadata and lineage available."
            if healthy
            else "Metadata and lineage unavailable — the dashboard runs in offline mode."
        )
        st.markdown(
            f'<div class="{cls}">'
            f'<div class="swat-dh-status">{status}</div>'
            f'<div class="swat-dh-sub">{sub}</div>'
            f'<div class="swat-dh-url">{html.escape(GMS_URL)}</div>'
            "</div>",
            unsafe_allow_html=True,
        )


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
    incidents = st.session_state.get("incidents", [])
    with st.container(border=True):
        st.markdown(
            f'<div class="swat-card-header">'
            f'<span class="swat-card-title">Active Incidents</span>'
            f'<span class="swat-count-badge">{len(incidents)}</span>'
            "</div>",
            unsafe_allow_html=True,
        )
        col_refresh, col_detect, col_info = st.columns([1, 1, 3])
        with col_refresh:
            if st.button("🔄 Refresh", width="stretch"):
                refresh()
                st.rerun()
        with col_detect:
            if st.button("⚡ Run Detection", type="primary", width="stretch"):
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

        if not incidents:
            st.markdown(
                '<div class="swat-empty">'
                '<div class="swat-empty-icon">🗄️</div>'
                '<div class="swat-empty-title">No incidents detected</div>'
                '<div class="swat-empty-desc">Run Detection to scan your DataHub metadata.<br/>'
                "If issues are found they will appear here automatically.</div>"
                "</div>",
                unsafe_allow_html=True,
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
    # Hero renders after refresh() so the "System Healthy" badge reflects the
    # real (persisted) incident count on the very first page load.
    _inject_theme(st.session_state.get("incidents", []))
    render_sidebar()
    render_flash()

    tab_active, tab_detail, tab_lineage, tab_fix = st.tabs(
        ["🔔 Active Incidents", "📄 Incident Detail", "🔗 Lineage Graph", "🛠 Fix Preview"]
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
# Footer
# --------------------------------------------------------------------------
st.divider()
st.markdown(_FOOTER_HTML, unsafe_allow_html=True)
