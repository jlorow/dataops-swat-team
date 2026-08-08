"""Unit tests for the Detective Agent (no network required).

The DataHub MCP client is replaced with an offline fake so tests focus on the
agent's investigation logic, status transitions, and event publishing.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from src.agents.detective_agent import DetectiveAgent, InvestigationResult
from src.datahub.mcp_client import (
    DataHubMCPClient,
    DatasetOwnership,
    DatasetSchema,
    LineageEdge,
    LineageGraph,
    OwnerInfo,
    SchemaField,
)
from src.models import (
    AgentEventType,
    DiagnosisReport,
    FailureType,
    Incident,
    IncidentStatus,
)
from src.orchestrator import EventBus, IncidentStateMachine, IncidentStore

VICTIM_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"
UP1_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.raw_orders,PROD)"
UP2_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.stg_customers,PROD)"
DOWN_URN = "urn:li:dataset:(urn:li:dataPlatform:tableau,db.dash_orders,PROD)"


def run(coro):
    return asyncio.run(coro)


def now_ms(days_ago: int = 0) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days_ago)).timestamp() * 1000)


class FakeDataHub:
    """Offline stand-in for DataHubMCPClient driven by canned fixtures."""

    def __init__(self, props=None, owners=None, lineage=None, schemas=None):
        self.props = props or {}
        self.owners = owners or {}
        self.lineage = lineage or {}
        self.schemas = schemas or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def get_dataset_properties(self, urn):
        return self.props.get(urn, {})

    async def get_dataset_ownership(self, urn):
        return DatasetOwnership(owners=self.owners.get(urn, []))

    async def get_dataset_lineage(self, urn):
        data = self.lineage.get(urn, {"up": [], "down": []})
        return LineageGraph(
            upstreams=data.get("up", []), downstreams=data.get("down", [])
        )

    async def get_dataset_schema(self, urn):
        return DatasetSchema(fields=self.schemas.get(urn, []))


def base_fixture():
    """Victim with one upstream, one downstream consumer, an owner, and a schema."""
    return {
        "props": {VICTIM_URN: {"last_modified": now_ms()}},
        "owners": {
            VICTIM_URN: [
                OwnerInfo(owner_urn="urn:li:corpuser:alice@example.com", owner_type="BUSINESS_OWNER")
            ]
        },
        "lineage": {
            VICTIM_URN: {
                "up": [LineageEdge(urn=UP1_URN, name="raw_orders", type="DATASET")],
                "down": [LineageEdge(urn=DOWN_URN, name="dash_orders", type="CHART")],
            },
            UP1_URN: {"up": [], "down": []},
        },
        "schemas": {
            VICTIM_URN: [SchemaField(field_path="id", native_type="NUMBER", description="id")],
            UP1_URN: [SchemaField(field_path="id", native_type="NUMBER", description="id")],
        },
    }


def make_agent(fixture, store):
    return DetectiveAgent(
        mcp_client=FakeDataHub(**fixture),
        event_bus=EventBus(),
        incident_store=store,
    )


def incident(id="inc-1", urn=VICTIM_URN, ftype=FailureType.SCHEMA_DRIFT):
    return Incident(id=id, status=IncidentStatus.DETECTED, victim_urn=urn, failure_type=ftype)


# ---------------------------------------------------------------------------
# investigate_open_incidents
# ---------------------------------------------------------------------------
def test_investigate_open_incidents_picks_only_detected_and_is_pure(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    for inc in (incident("a"), incident("b"), incident("c", urn=UP1_URN)):
        store.create(inc)
    already_diagnosing = incident("x", urn=UP2_URN)
    already_diagnosing.status = IncidentStatus.DIAGNOSING
    store.create(already_diagnosing)

    agent = make_agent(base_fixture(), store)
    results = run(agent.investigate_open_incidents())

    assert {r.incident_id for r in results} == {"a", "b", "c"}
    # Pure investigation: incidents are NOT modified.
    assert store.get("a").status == IncidentStatus.DETECTED
    assert store.get("x").status == IncidentStatus.DIAGNOSING

    # limit is honored
    assert len(run(agent.investigate_open_incidents(limit=2))) == 2


# ---------------------------------------------------------------------------
# investigate: schema drift
# ---------------------------------------------------------------------------
def test_investigate_schema_drift(tmp_path):
    fixture = base_fixture()
    fixture["schemas"] = {
        VICTIM_URN: [
            SchemaField(field_path="id", native_type="NUMBER", description="id"),
            SchemaField(field_path="total", native_type="FLOAT", description="total"),
        ],
        # Upstream renames the type of a shared field and lacks "total".
        UP1_URN: [SchemaField(field_path="id", native_type="TEXT", description="id")],
    }
    agent = make_agent(fixture, IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    result = run(agent.investigate(incident("inc-schema")))

    assert result.recommended_fix_type == "SCHEMA_UPDATE"
    assert result.root_cause_dataset_urn == UP1_URN
    assert "drift" in result.root_cause_description.lower()
    assert "type mismatches" in result.root_cause_description
    assert result.affected_datasets == [VICTIM_URN, DOWN_URN]
    # lineage (+0.2) + schema comparison (+0.2) + ownership (+0.1) -> 1.0
    assert result.confidence_score == pytest.approx(1.0)
    assert result.evidence["upstream_schema_count"] == 1


# ---------------------------------------------------------------------------
# investigate: freshness
# ---------------------------------------------------------------------------
def test_investigate_freshness_traces_stalest_upstream(tmp_path):
    fixture = base_fixture()
    fixture["props"] = {
        VICTIM_URN: {"last_modified": now_ms(days_ago=30)},
        UP1_URN: {"last_modified": now_ms(days_ago=90)},  # stalest
        UP2_URN: {"last_modified": now_ms(days_ago=10)},
    }
    fixture["owners"] = {}  # no ownership info anywhere
    fixture["lineage"] = {
        VICTIM_URN: {
            "up": [LineageEdge(urn=UP1_URN, name="raw_orders", type="DATASET"),
                   LineageEdge(urn=UP2_URN, name="stg_customers", type="DATASET")],
            "down": [],
        },
        UP1_URN: {"up": [], "down": []},
        UP2_URN: {"up": [], "down": []},
    }
    agent = make_agent(fixture, IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    result = run(agent.investigate(incident("inc-fresh", ftype=FailureType.FRESHNESS_VIOLATION)))

    assert result.recommended_fix_type == "FRESHNESS_RERUN"
    assert result.root_cause_dataset_urn == UP1_URN  # oldest source
    assert result.evidence["stalest_upstream_urn"] == UP1_URN
    assert result.evidence["upstream_path"] == [UP1_URN]
    assert "stalest source" in result.root_cause_description
    # lineage (+0.2) only -> 0.7
    assert result.confidence_score == pytest.approx(0.7)


# ---------------------------------------------------------------------------
# investigate: ownership gap
# ---------------------------------------------------------------------------
def test_investigate_ownership_gap_uses_upstream_owners(tmp_path):
    fixture = base_fixture()
    fixture["owners"] = {UP1_URN: [OwnerInfo(owner_urn="urn:li:corpuser:bob@example.com", owner_type="TECHNICAL_OWNER")]}
    agent = make_agent(fixture, IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    result = run(agent.investigate(incident("inc-own", ftype=FailureType.OWNERSHIP_GAP)))

    assert result.recommended_fix_type == "OWNER_ASSIGNMENT"
    assert "upstream" in result.root_cause_description
    assert "bob@example.com" in result.root_cause_description
    assert result.evidence["upstream_owner_types"] == ["TECHNICAL_OWNER"]
    # lineage (+0.2) + ownership info upstream (+0.1) -> 0.8
    assert result.confidence_score == pytest.approx(0.8)


# ---------------------------------------------------------------------------
# investigate: lineage gap
# ---------------------------------------------------------------------------
def test_investigate_lineage_gap_uses_naming_convention(tmp_path):
    stg_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.stg_orders,PROD)"
    fixture = base_fixture()
    fixture["lineage"] = {stg_urn: {"up": [], "down": []}}
    fixture["props"] = {stg_urn: {}}
    fixture["owners"] = {}
    fixture["schemas"] = {stg_urn: []}
    agent = make_agent(fixture, IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    result = run(agent.investigate(incident("inc-lineage", urn=stg_urn, ftype=FailureType.LINEAGE_GAP)))

    assert result.recommended_fix_type == "LINEAGE_REPAIR"
    assert "stg_" in result.root_cause_description
    assert result.affected_datasets == [stg_urn]
    assert result.confidence_score == pytest.approx(0.5)  # no lineage, no schema compare, no ownership


# ---------------------------------------------------------------------------
# confidence scoring
# ---------------------------------------------------------------------------
def test_confidence_scoring_and_cap(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    assert agent._compute_confidence(False, False, False) == pytest.approx(0.5)
    assert agent._compute_confidence(True, False, False) == pytest.approx(0.7)
    assert agent._compute_confidence(True, True, False) == pytest.approx(0.9)
    assert agent._compute_confidence(True, True, True) == pytest.approx(1.0)
    assert agent._compute_confidence(True, True, True) <= 1.0  # capped


# ---------------------------------------------------------------------------
# diagnose_and_update
# ---------------------------------------------------------------------------
def test_diagnose_and_update_transitions_and_persists(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    store.create(incident("inc-1"))
    bus = EventBus()
    events = []
    bus.subscribe(AgentEventType.DIAGNOSIS_COMPLETE, events.append)
    agent = make_agent(base_fixture(), store)
    agent.bus = bus

    investigation = InvestigationResult(
        incident_id="inc-1",
        root_cause_dataset_urn=UP1_URN,
        root_cause_description="Schema drift on orders vs raw_orders",
        impact_assessment="Affects 1 downstream table(s)",
        affected_datasets=[VICTIM_URN, DOWN_URN],
        recommended_fix_type="SCHEMA_UPDATE",
        confidence_score=0.9,
        evidence={"owner_emails": ["alice@example.com"], "upstream_path": [UP1_URN]},
    )
    updated = run(agent.diagnose_and_update(investigation))

    assert updated.status == IncidentStatus.ROOT_CAUSE_IDENTIFIED
    assert store.get("inc-1").status == IncidentStatus.ROOT_CAUSE_IDENTIFIED

    diagnosis = store.get("inc-1").diagnosis
    assert isinstance(diagnosis, DiagnosisReport)
    assert diagnosis.root_cause_urn == UP1_URN
    assert diagnosis.root_cause_type == FailureType.SCHEMA_DRIFT
    assert diagnosis.summary_text == "Schema drift on orders vs raw_orders"
    assert diagnosis.recommended_fix_type == "SCHEMA_UPDATE"
    assert diagnosis.confidence_score == pytest.approx(0.9)
    assert diagnosis.owner_email == "alice@example.com"
    assert diagnosis.lineage_path == [UP1_URN]

    # The diagnosis is also recorded on the incident's agent log.
    assert updated.agent_logs[-1].action == "DIAGNOSED"
    assert updated.agent_logs[-1].output_summary == "Schema drift on orders vs raw_orders"

    assert len(events) == 1
    event = events[0]
    assert event.event_type == AgentEventType.DIAGNOSIS_COMPLETE
    assert "Root cause identified: Schema drift on orders" in event.payload["summary"]
    assert event.payload["confidence_score"] == 0.9


def test_diagnose_raises_when_incident_missing(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    investigation = InvestigationResult(
        incident_id="nope",
        root_cause_description="x",
        impact_assessment="",
        affected_datasets=[],
        recommended_fix_type="SCHEMA_UPDATE",
        confidence_score=0.5,
    )
    with pytest.raises(ValueError, match="not found"):
        run(agent.diagnose_and_update(investigation))


def test_run_continues_when_one_incident_fails(tmp_path):
    # The second incident's victim raises during investigation; run() must
    # log and continue with the healthy one instead of aborting the batch.
    broken_urn = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.broken,PROD)"
    fixture = base_fixture()

    class FailingFake(FakeDataHub):
        async def get_dataset_schema(self, urn):
            if urn == broken_urn:
                raise RuntimeError("simulated GMS failure")
            return await super().get_dataset_schema(urn)

    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    store.create(incident("ok"))
    store.create(incident("broken", urn=broken_urn, ftype=FailureType.FRESHNESS_VIOLATION))
    agent = DetectiveAgent(mcp_client=FailingFake(**fixture), event_bus=EventBus(), incident_store=store)

    updated = run(agent.run(limit=5))
    assert [u.id for u in updated] == ["ok"]
    assert updated[0].status == IncidentStatus.ROOT_CAUSE_IDENTIFIED


def test_run_pipeline_end_to_end(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    store.create(incident("inc-1"))
    agent = make_agent(base_fixture(), store)
    updated = run(agent.run(limit=5))
    assert len(updated) == 1
    assert updated[0].status == IncidentStatus.ROOT_CAUSE_IDENTIFIED
    assert updated[0].diagnosis is not None


# ---------------------------------------------------------------------------
# state machine
# ---------------------------------------------------------------------------
def test_state_machine_supports_root_cause_identified(tmp_path):
    inc = incident("sm-1")
    machine = IncidentStateMachine(inc)

    assert machine.can_transition_to(IncidentStatus.DIAGNOSING)
    machine.transition_to(IncidentStatus.DIAGNOSING)
    assert machine.can_transition_to(IncidentStatus.ROOT_CAUSE_IDENTIFIED)
    machine.transition_to(IncidentStatus.ROOT_CAUSE_IDENTIFIED)
    assert inc.status == IncidentStatus.ROOT_CAUSE_IDENTIFIED

    # Engineer Agent (next story) can pick it up.
    assert machine.can_transition_to(IncidentStatus.FIXING)

    # Direct DETECTED -> ROOT_CAUSE_IDENTIFIED is NOT allowed.
    direct = incident("sm-2")
    with pytest.raises(ValueError):
        IncidentStateMachine(direct).transition_to(IncidentStatus.ROOT_CAUSE_IDENTIFIED)


# ---------------------------------------------------------------------------
# context manager with the real client
# ---------------------------------------------------------------------------
def test_detective_agent_context_manager_with_real_client(tmp_path):
    import httpx

    def handler(request):
        return httpx.Response(200, json={"data": {"search": {"searchResults": []}}, "extensions": {}})

    client = DataHubMCPClient(gms_url="http://datahub.test", transport=httpx.MockTransport(handler))
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))

    async def go():
        async with DetectiveAgent(client, incident_store=store) as agent:
            return await agent.investigate_open_incidents()

    assert run(go()) == []
