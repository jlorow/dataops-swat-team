"""DataOps SWAT Team — Mission Control dashboard (Streamlit).

Mock-data shell for the hackathon demo. Real orchestration integration
(EventBus / IncidentStore / agents) lands in a later story.
"""
import streamlit as st

st.set_page_config(page_title="DataOps SWAT Team", page_icon="🛡️", layout="wide")

# --------------------------------------------------------------------------
# Header
# --------------------------------------------------------------------------
st.title("🛡️ DataOps SWAT Team — Mission Control")
st.subheader("AI-powered incident response for data pipelines")
st.divider()

# --------------------------------------------------------------------------
# Sidebar — Agent Status
# --------------------------------------------------------------------------
with st.sidebar:
    st.header("Agent Status")
    agents = {
        "🛡️ Sentry": "Idle",
        "🔍 Detective": "Idle",
        "🔧 Engineer": "Idle",
        "✅ Validator": "Idle",
    }
    for name, status in agents.items():
        icon = "🟢" if status == "Idle" else "🟡"
        st.markdown(f"**{name}** — {icon} {status}")
    st.divider()
    if st.button("Inject Test Incident", type="primary", width="stretch"):
        st.success("Incident injected! (Mock)")

# --------------------------------------------------------------------------
# Main content — Tabs
# --------------------------------------------------------------------------
tab_active, tab_detail, tab_lineage, tab_fix = st.tabs(
    ["Active Incidents", "Incident Detail", "Lineage Graph", "Fix Preview"]
)

# --- Tab 1: Active Incidents ----------------------------------------------
with tab_active:
    st.subheader("Active Incidents")
    incidents_df = st.dataframe(
        {
            "ID": ["INC-001", "INC-002"],
            "Victim Dataset": [
                "urn:li:dataset:stg_customers",
                "urn:li:dataset:fct_orders",
            ],
            "Status": ["DIAGNOSING", "DETECTED"],
            "Failure Type": ["SCHEMA_DRIFT", "FRESHNESS_VIOLATION"],
            "Duration": ["0:02:15", "0:00:45"],
            "Created": ["2026-08-07 10:00", "2026-08-07 10:30"],
        },
        width="stretch",
        hide_index=True,
    )

# --- Tab 2: Incident Detail ------------------------------------------------
STATUS_COLORS = {
    "DETECTED": "red",
    "DIAGNOSING": "yellow",
    "FIXING": "blue",
    "VALIDATING": "purple",
    "RESOLVED": "green",
    "ESCALATED": "orange",
}


def status_badge(status: str) -> str:
    """Render a colored status badge via HTML."""
    color = STATUS_COLORS.get(status, "gray")
    return f'<span style="background-color:{color}; color:white; padding:2px 10px; ' \
           f'border-radius:12px; font-weight:600;">{status}</span>'


with tab_detail:
    st.subheader("Incident Detail")
    selected = st.selectbox("Select Incident", ["INC-001", "INC-002"])

    col_badge, col_owner = st.columns(2)
    with col_badge:
        st.markdown("**Status**")
        st.markdown(
            status_badge("DIAGNOSING" if selected == "INC-001" else "DETECTED"),
            unsafe_allow_html=True,
        )
    with col_owner:
        st.markdown("**Owner**")
        st.markdown("data-team@company.com")

    st.markdown("**Root Cause**")
    st.markdown("Pending...")

    st.markdown("**Agent Log**")
    with st.expander(f"View agent log for {selected}", expanded=True):
        st.markdown("`2026-08-07 10:00:02` — 🛡️ **Sentry**: Detected schema drift on victim dataset")
        st.markdown("`2026-08-07 10:02:15` — 🔍 **Detective**: Diagnosis in progress, inspecting lineage")

# --- Tab 3: Lineage Graph ---------------------------------------------------
with tab_lineage:
    st.subheader("Lineage Graph")
    st.graphviz_chart(
        """
        digraph {
            raw_customers -> stg_customers -> marts_customer_churn;
            raw_customers [color=red, label="raw_customers\\n(SCHEMA CHANGED)"];
            stg_customers [color=orange, label="stg_customers\\n(BROKEN)"];
            marts_customer_churn [color=gray, label="marts_customer_churn\\n(stale)"];
        }
        """
    )

# --- Tab 4: Fix Preview ------------------------------------------------------
with tab_fix:
    st.subheader("Fix Preview")
    st.code(
        """-- BEFORE
SELECT customer_id, name FROM raw_customers

-- AFTER
SELECT customer_id, name, region FROM raw_customers""",
        language="sql",
    )

# --------------------------------------------------------------------------
# Footer
# --------------------------------------------------------------------------
st.divider()
st.caption("Built for DataHub Agent Hackathon | DataOps SWAT Team v0.1")
