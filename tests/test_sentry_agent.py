"""Unit tests for the Sentry Agent (no network required).

The DataHub MCP client is replaced with an offline fake so the tests focus on
the agent's detection logic and incident pipeline.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from src.agents.sentry_agent import DetectionRule, DetectedAnomaly, SentryAgent
from src.datahub.mcp_client import (
    DataHubMCPClient,
    DatasetInfo,
    DatasetOwnership,
    DatasetSchema,
    LineageEdge,
    LineageGraph,
    OwnerInfo,
    SchemaField,
)
from src.models import AgentEventType, AgentType, FailureType, IncidentStatus, IncidentType
from src.orchestrator import EventBus, IncidentStore


def run(coro):
    """Run an async coroutine from a sync test."""
    return asyncio.run(coro)


def now_ms(days_ago: int = 0) -> int:
    return int((datetime.now(timezone.utc) - timedelta(days=days_ago)).timestamp() * 1000)


class FakeDataHub:
    """Offline stand-in for DataHubMCPClient driven by canned fixtures."""

    def __init__(self, datasets, props=None, owners=None, lineage=None, schemas=None,
                 failing_urns=None):
        self.datasets = datasets  # list of DatasetInfo
        self.props = props or {}  # urn -> properties dict
        self.owners = owners or {}  # urn -> [OwnerInfo]
        self.lineage = lineage or {}  # urn -> {"up": [LineageEdge], "down": [...]}
        self.schemas = schemas or {}  # urn -> [SchemaField]
        self.failing_urns = failing_urns or set()  # schema fetch raises for these

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def search_datasets(self, query="*", count=10):
        return self.datasets[:count]

    async def get_dataset_properties(self, urn):
        return self.props.get(urn, {})

    async def get_dataset_ownership(self, urn):
        return DatasetOwnership(owners=self.owners.get(urn, []))

    async def get_dataset_lineage(self, urn):
        data = self.lineage.get(urn, {"up": [], "down": []})
        return LineageGraph(upstreams=data.get("up", []), downstreams=data.get("down", []))

    async def get_dataset_schema(self, urn):
        if urn in self.failing_urns:
            raise RuntimeError("simulated GMS failure for schema fetch")
        return DatasetSchema(fields=self.schemas.get(urn, []))


URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"


def healthy_fixture(urn=URN):
    """A dataset that should produce zero anomalies."""
    return {
        "datasets": [DatasetInfo(urn=urn, name="orders", platform="dbt")],
        "props": {urn: {"name": "orders", "last_modified": now_ms()}},
        "owners": {urn: [OwnerInfo(owner_urn="urn:li:corpuser:alice", owner_type="BUSINESS_OWNER")]},
        "lineage": {urn: {"up": [LineageEdge(urn="urn:up:raw", name="raw", type="DATASET")], "down": []}},
        "schemas": {urn: [SchemaField(field_path="id", native_type="NUMBER", description="the id")]},
    }


def make_agent(fixture, store, rules=None, bus=None):
    fake = FakeDataHub(**fixture)
    return SentryAgent(
        mcp_client=fake,
        event_bus=bus or EventBus(),
        incident_store=store,
        rules=rules or DetectionRule(),
    ), fake


# ---------------------------------------------------------------------------
# scan() — detection logic
# ---------------------------------------------------------------------------
def test_scan_returns_no_anomalies_for_healthy_dataset(tmp_path):
    agent, _ = make_agent(healthy_fixture(), store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    assert run(agent.scan()) == []


def test_scan_detects_ownership_gap(tmp_path):
    fixture = healthy_fixture()
    fixture["owners"][URN] = []  # zero owners
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == IncidentType.OWNERSHIP_GAP
    assert anomaly.severity == "HIGH"
    assert anomaly.dataset_name == "orders"
    assert anomaly.evidence == {"owner_count": 0}


def test_scan_detects_lineage_gap(tmp_path):
    fixture = healthy_fixture()
    fixture["lineage"][URN] = {"up": [], "down": []}  # orphan
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert len(anomalies) == 1
    assert anomalies[0].anomaly_type == IncidentType.LINEAGE_GAP
    assert anomalies[0].severity == "MEDIUM"


def test_scan_detects_freshness_violation(tmp_path):
    fixture = healthy_fixture()
    fixture["props"][URN]["last_modified"] = now_ms(days_ago=30)  # 720h old
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert len(anomalies) == 1
    assert anomalies[0].anomaly_type == IncidentType.FRESHNESS_VIOLATION
    assert anomalies[0].severity == "HIGH"  # 720h > 4 * 168h
    assert anomalies[0].evidence["hours_since_update"] > 168


def test_freshness_medium_when_moderately_stale(tmp_path):
    fixture = healthy_fixture()
    fixture["props"][URN]["last_modified"] = now_ms(days_ago=8)  # 192h, just over 168h
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert anomalies[0].severity == "MEDIUM"


def test_freshness_flagged_when_last_modified_is_epoch_zero(tmp_path):
    fixture = healthy_fixture()
    fixture["props"][URN]["last_modified"] = 0  # never-updated entity
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert len(anomalies) == 1
    assert anomalies[0].anomaly_type == IncidentType.FRESHNESS_VIOLATION
    assert anomalies[0].severity == "HIGH"


def test_freshness_skipped_when_no_last_modified(tmp_path):
    fixture = healthy_fixture()
    del fixture["props"][URN]["last_modified"]
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    assert run(agent.scan()) == []


def test_scan_detects_missing_schema_descriptions(tmp_path):
    fixture = healthy_fixture()
    fixture["schemas"][URN] = [
        SchemaField(field_path="a", native_type="NUMBER", description=None),
        SchemaField(field_path="b", native_type="TEXT", description=""),
        SchemaField(field_path="c", native_type="FLOAT", description="ok"),
    ]
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert len(anomalies) == 1
    anomaly = anomalies[0]
    assert anomaly.anomaly_type == IncidentType.SCHEMA_DRIFT
    assert anomaly.severity == "MEDIUM"  # 2/3 missing >= 0.5
    assert anomaly.evidence["missing_fields"] == ["a", "b"]


def test_scan_reports_multiple_anomalies_per_dataset(tmp_path):
    fixture = healthy_fixture()
    fixture["owners"][URN] = []
    fixture["lineage"][URN] = {"up": [], "down": []}
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    assert {a.anomaly_type for a in anomalies} == {IncidentType.OWNERSHIP_GAP, IncidentType.LINEAGE_GAP}


def test_rules_can_disable_checks(tmp_path):
    fixture = healthy_fixture()
    fixture["owners"][URN] = []
    fixture["lineage"][URN] = {"up": [], "down": []}
    rules = DetectionRule(require_ownership=False, require_lineage=False)
    agent, _ = make_agent(fixture, rules=rules, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    assert run(agent.scan()) == []


def test_scan_respects_dataset_limit(tmp_path):
    fixture = healthy_fixture()
    fixture["datasets"] = [
        DatasetInfo(urn=f"urn:{i}", name=f"tbl_{i}", platform="dbt") for i in range(5)
    ]
    for i in range(5):
        fixture["owners"][f"urn:{i}"] = []
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    anomalies = run(agent.scan(dataset_limit=3))
    # The limit caps the number of scanned datasets, not anomalies per dataset.
    assert {a.dataset_urn for a in anomalies} == {"urn:0", "urn:1", "urn:2"}


# ---------------------------------------------------------------------------
# create_incidents() — persistence + events
# ---------------------------------------------------------------------------
def anomaly(urn=URN, atype=IncidentType.OWNERSHIP_GAP, name="orders"):
    return DetectedAnomaly(
        dataset_urn=urn,
        dataset_name=name,
        anomaly_type=atype,
        severity="HIGH",
        description=f"anomaly {atype.value}",
        evidence={"owner_count": 0},
    )


def test_create_incidents_persists_and_publishes(tmp_path):
    bus = EventBus()
    received = []
    bus.subscribe(AgentEventType.INCIDENT_CREATED, received.append)

    agent, _ = make_agent(
        healthy_fixture(),
        bus=bus,
        store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")),
    )
    incidents = run(agent.create_incidents([anomaly()]))

    assert len(incidents) == 1
    incident = incidents[0]
    assert incident.status == IncidentStatus.DETECTED
    assert incident.victim_urn == URN
    assert incident.failure_type == FailureType.OWNERSHIP_GAP

    # Persisted in the store
    assert agent.store.get(incident.id) is incident

    # Event published on the bus
    assert len(received) == 1
    event = received[0]
    assert event.event_type == AgentEventType.INCIDENT_CREATED
    assert event.incident_id == incident.id
    assert event.agent_name == AgentType.SENTRY.value
    assert event.payload["anomaly_type"] == "OWNERSHIP_GAP"


def test_create_incidents_maps_all_anomaly_types(tmp_path):
    agent, _ = make_agent(healthy_fixture(), store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    anomalies = [
        anomaly(urn=URN + "/1", atype=IncidentType.SCHEMA_DRIFT),
        anomaly(urn=URN + "/2", atype=IncidentType.FRESHNESS_VIOLATION),
        anomaly(urn=URN + "/3", atype=IncidentType.OWNERSHIP_GAP),
        anomaly(urn=URN + "/4", atype=IncidentType.LINEAGE_GAP),
    ]
    incidents = run(agent.create_incidents(anomalies))
    assert {i.failure_type for i in incidents} == {
        FailureType.SCHEMA_DRIFT,
        FailureType.FRESHNESS_VIOLATION,
        FailureType.OWNERSHIP_GAP,
        FailureType.LINEAGE_GAP,
    }


def test_incident_ids_are_deterministic_and_deduplicated(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent, _ = make_agent(healthy_fixture(), store=store)

    first = run(agent.create_incidents([anomaly()]))
    assert len(first) == 1
    assert first[0].id.startswith("inc-ownership_gap-")

    # Re-scanning the same anomaly must not duplicate the incident/event/JSONL line.
    second = run(agent.create_incidents([anomaly()]))
    assert second == []
    assert len(store.list_all()) == 1
    lines = [l for l in (tmp_path / "i.jsonl").read_text().splitlines() if l.strip()]
    assert len(lines) == 1


def test_incident_records_agent_log_entry(tmp_path):
    agent, _ = make_agent(healthy_fixture(), store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    incident = run(agent.create_incidents([anomaly()]))[0]
    assert len(incident.agent_logs) == 1
    log = incident.agent_logs[0]
    assert log.agent_name == AgentType.SENTRY.value
    assert log.action == "DETECTED"
    assert "OWNERSHIP_GAP" in log.input_summary
    assert log.output_summary == "anomaly OWNERSHIP_GAP"


def test_scan_continues_when_one_dataset_fails(tmp_path):
    fixture = healthy_fixture()
    broken = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.broken,PROD)"
    fixture["datasets"].append(DatasetInfo(urn=broken, name="broken", platform="dbt"))
    fixture["owners"][URN] = []  # healthy dataset now has an ownership gap
    fixture["owners"][broken] = []
    fixture["failing_urns"] = {broken}  # schema fetch will raise for this one
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))

    anomalies = run(agent.scan())
    # The broken dataset is skipped gracefully, the other still gets scanned.
    assert {a.dataset_urn for a in anomalies} == {URN}


def test_run_pipeline_end_to_end(tmp_path):
    fixture = healthy_fixture()
    fixture["owners"][URN] = []
    agent, _ = make_agent(fixture, store=IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    incidents = run(agent.run())
    assert len(incidents) == 1
    assert incidents[0].failure_type == FailureType.OWNERSHIP_GAP


# ---------------------------------------------------------------------------
# context manager
# ---------------------------------------------------------------------------
def test_sentry_agent_context_manager_with_real_client():
    def handler(request):
        return httpx.Response(200, json={"data": {"search": {"searchResults": []}}, "extensions": {}})

    client = DataHubMCPClient(gms_url="http://datahub.test", transport=httpx.MockTransport(handler))

    async def go():
        async with SentryAgent(client) as agent:
            assert agent.agent_type == AgentType.SENTRY
            return await agent.scan()

    assert run(go()) == []
