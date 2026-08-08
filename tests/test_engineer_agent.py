"""Unit tests for the Engineer Agent (no network required).

The DataHub MCP client is replaced with an offline fake, and the LLM with a
stub, so tests focus on prompt building, template fallback SQL, validation,
and the ROOT_CAUSE_IDENTIFIED -> FIXING -> FIX_PROPOSED transitions.
"""

import asyncio

import pytest

from src.agents.engineer_agent import EngineerAgent, GeneratedFix
from src.datahub.mcp_client import (
    DataHubMCPClient,
    DatasetOwnership,
    DatasetSchema,
    LineageEdge,
    LineageGraph,
    SchemaField,
)
from src.models import (
    AgentEventType,
    DiagnosisReport,
    FailureType,
    FixReport,
    Incident,
    IncidentStatus,
)
from src.orchestrator import EventBus, IncidentStateMachine, IncidentStore

VICTIM_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"
UP1_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.raw_orders,PROD)"


def run(coro):
    return asyncio.run(coro)


class FakeLLM:
    """Duck-typed LLM stub capturing calls."""

    def __init__(self, response="SELECT id FROM orders;\n-- fixed"):
        self.response = response
        self.calls = []

    def generate(self, prompt, system_prompt=None, temperature=0.1, max_tokens=2048):
        self.calls.append({"prompt": prompt, "temperature": temperature, "max_tokens": max_tokens})
        return self.response

    def is_available(self):
        return True


class FailingLLM(FakeLLM):
    def generate(self, prompt, system_prompt=None, temperature=0.1, max_tokens=2048):
        self.calls.append({"prompt": prompt, "max_tokens": max_tokens})
        raise RuntimeError("LLM service unavailable")


class FakeDataHub:
    """Offline stand-in for DataHubMCPClient."""

    def __init__(self, lineage=None, schemas=None):
        self.lineage = lineage or {}
        self.schemas = schemas or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def get_dataset_schema(self, urn):
        return DatasetSchema(fields=self.schemas.get(urn, []))

    async def get_dataset_lineage(self, urn):
        data = self.lineage.get(urn, {"up": [], "down": []})
        return LineageGraph(upstreams=data.get("up", []), downstreams=data.get("down", []))


def base_fixture():
    """Victim with one upstream (raw_orders) that has an extra 'total' column."""
    return {
        "lineage": {
            VICTIM_URN: {
                "up": [LineageEdge(urn=UP1_URN, name="raw_orders", type="DATASET")],
                "down": [],
            },
            UP1_URN: {"up": [], "down": []},
        },
        "schemas": {
            VICTIM_URN: [SchemaField(field_path="id", native_type="NUMBER", description="order id")],
            UP1_URN: [
                SchemaField(field_path="id", native_type="NUMBER", description="id"),
                SchemaField(field_path="total", native_type="FLOAT", description="amount"),
            ],
        },
    }


def make_agent(fixture, store, llm=None):
    return EngineerAgent(
        mcp_client=FakeDataHub(**fixture),
        llm_client=llm,
        event_bus=EventBus(),
        incident_store=store,
    )


def diagnosed_incident(
    id="inc-1",
    urn=VICTIM_URN,
    fix_type="SCHEMA_UPDATE",
    evidence=None,
):
    incident = Incident(id=id, status=IncidentStatus.ROOT_CAUSE_IDENTIFIED, victim_urn=urn, failure_type=FailureType.SCHEMA_DRIFT)
    incident.diagnosis = DiagnosisReport(
        root_cause_urn=UP1_URN,
        root_cause_type=FailureType.SCHEMA_DRIFT,
        lineage_path=[UP1_URN],
        owner_email="alice@example.com",
        summary_text="Schema drift on orders vs raw_orders",
        impact_assessment="Affects 1 downstream table(s)",
        affected_datasets=[VICTIM_URN],
        recommended_fix_type=fix_type,
        confidence_score=0.9,
        evidence=evidence or {},
    )
    return incident


# ---------------------------------------------------------------------------
# FixReport model
# ---------------------------------------------------------------------------
def test_fixreport_model_has_engineer_fields():
    report = FixReport()
    assert report.fix_id == ""
    assert report.incident_id == ""
    assert report.target_dataset_urn == ""
    assert report.sql_code == ""
    assert report.is_valid is True
    assert report.validation_error is None
    assert report.generated_at is not None
    assert report.fix_type == "SQL_PATCH"


# ---------------------------------------------------------------------------
# _validate_sql
# ---------------------------------------------------------------------------
def test_validate_sql_accepts_plain_sql(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    is_valid, error = agent._validate_sql("SELECT id, total FROM orders WHERE id > 0;")
    assert is_valid is True
    assert error is None


def test_validate_sql_rejects_empty(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    assert agent._validate_sql("") == (False, "SQL is empty")
    assert agent._validate_sql("   \n  ") == (False, "SQL is empty")


def test_validate_sql_rejects_drop_database(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    is_valid, error = agent._validate_sql("DROP DATABASE production;")
    assert is_valid is False
    assert "DROP" in error


def test_validate_sql_rejects_drop_table(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    is_valid, error = agent._validate_sql("DROP TABLE orders;")
    assert is_valid is False
    assert "DROP TABLE" in error


def test_validate_sql_rejects_update_without_where(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    is_valid, error = agent._validate_sql("UPDATE orders SET total = 0;")
    assert is_valid is False
    assert "UPDATE" in error

    is_valid, _ = agent._validate_sql("UPDATE orders SET total = 0 WHERE id = 1;")
    assert is_valid is True


def test_validate_sql_rejects_delete_without_where(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    is_valid, error = agent._validate_sql("DELETE FROM orders;")
    assert is_valid is False
    assert "WHERE" in error

    is_valid, _ = agent._validate_sql("DELETE FROM orders WHERE id = 1;")
    assert is_valid is True


def test_validate_sql_rejects_truncate(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    is_valid, error = agent._validate_sql("TRUNCATE TABLE orders;")
    assert is_valid is False
    assert "TRUNCATE" in error


# ---------------------------------------------------------------------------
# template fallback produces real SQL
# ---------------------------------------------------------------------------
def test_template_schema_update_adds_real_missing_columns(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = diagnosed_incident()

    report = run(agent.generate_fix(incident))

    assert report.is_valid is True
    assert "ALTER TABLE orders ADD COLUMN total FLOAT;" in report.sql_code
    assert "total" in report.sql_code  # real column references from the schema
    assert "id" not in report.sql_code  # id exists downstream; only missing cols are added
    assert report.fix_type == "SCHEMA_UPDATE"
    assert report.target_dataset_urn == VICTIM_URN
    assert report.incident_id == "inc-1"
    assert report.fix_id.startswith("fix-inc-1-")
    assert report.validation_error is None


def test_template_schema_update_fixes_type_mismatches(tmp_path):
    fixture = base_fixture()
    # Field sets match, but the shared 'id' column has a type mismatch.
    fixture["schemas"] = {
        VICTIM_URN: [SchemaField(field_path="id", native_type="NUMBER", description="id")],
        UP1_URN: [SchemaField(field_path="id", native_type="TEXT", description="id")],
    }
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(fixture, store)
    incident = diagnosed_incident()

    report = run(agent.generate_fix(incident))

    assert "ALTER TABLE orders ALTER COLUMN id SET DATA TYPE TEXT;" in report.sql_code
    assert report.is_valid is True


def test_template_avoids_bare_select_star_when_no_fields(tmp_path):
    fixture = base_fixture()
    fixture["schemas"] = {VICTIM_URN: [], UP1_URN: []}
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(fixture, store)
    incident = diagnosed_incident(fix_type="LINEAGE_REPAIR", id="inc-empty")

    report = run(agent.generate_fix(incident))

    assert "SELECT * FROM" not in report.sql_code
    assert "SELECT COUNT(*) AS row_count FROM orders;" in report.sql_code
    assert report.is_valid is True


def test_template_freshness_uses_real_timestamp_column(tmp_path):
    fixture = base_fixture()
    fixture["schemas"][VICTIM_URN] = [
        SchemaField(field_path="id", native_type="NUMBER", description="id"),
        SchemaField(field_path="order_ts", native_type="TIMESTAMP", description="placed at"),
    ]
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(fixture, store)
    incident = diagnosed_incident(fix_type="FRESHNESS_RERUN", id="inc-fresh")
    incident.diagnosis.summary_text = "stale dataset"

    report = run(agent.generate_fix(incident))

    assert "MAX(order_ts) AS latest_ingested_ts" in report.sql_code
    assert report.is_valid is True


def test_template_owner_assignment_references_upstream_owner(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = diagnosed_incident(
        fix_type="OWNER_ASSIGNMENT",
        id="inc-own",
        evidence={"upstream_owner_emails": ["bob@example.com"]},
    )

    report = run(agent.generate_fix(incident))

    assert "bob@example.com" in report.sql_code
    assert report.is_valid is True


def test_template_lineage_repair_produces_real_select(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = diagnosed_incident(fix_type="LINEAGE_REPAIR", id="inc-lineage")

    report = run(agent.generate_fix(incident))

    assert "SELECT id FROM orders" in report.sql_code
    assert report.is_valid is True


# ---------------------------------------------------------------------------
# LLM path
# ---------------------------------------------------------------------------
def test_generate_fix_uses_llm_and_strips_fences(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    llm = FakeLLM(response="```sql\nSELECT id, total FROM orders WHERE total > 0;\n```\nAdds a filter to find high-value orders.")
    agent = make_agent(base_fixture(), store, llm=llm)
    incident = diagnosed_incident()

    report = run(agent.generate_fix(incident))

    assert llm.calls, "LLM should have been called"
    assert llm.calls[0]["max_tokens"] == 2048
    assert llm.calls[0]["temperature"] == 0.2
    assert "SELECT id, total FROM orders WHERE total > 0;" in report.sql_code
    assert "```" not in report.sql_code
    assert report.is_valid is True
    assert "high-value orders" in report.explanation


def test_generate_fix_falls_back_to_template_when_llm_fails(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    llm = FailingLLM()
    agent = make_agent(base_fixture(), store, llm=llm)
    incident = diagnosed_incident()

    report = run(agent.generate_fix(incident))

    assert "ALTER TABLE orders" in report.sql_code  # template fallback, real columns
    assert report.is_valid is True


def test_generate_fix_uses_template_when_no_llm(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store, llm=None)
    assert agent.llm is None

    incident = diagnosed_incident()
    report = run(agent.generate_fix(incident))

    assert "ALTER TABLE orders" in report.sql_code


# ---------------------------------------------------------------------------
# transitions / persistence / events
# ---------------------------------------------------------------------------
def test_generate_fix_transitions_and_persists(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    store.create(diagnosed_incident())
    bus = EventBus()
    events = []
    bus.subscribe(AgentEventType.FIX_GENERATED, events.append)
    agent = make_agent(base_fixture(), store)
    agent.bus = bus

    report = run(agent.generate_fix(store.get("inc-1")))

    assert store.get("inc-1").status == IncidentStatus.FIX_PROPOSED
    assert store.get("inc-1").fix is not None
    assert store.get("inc-1").fix.sql_code == report.sql_code
    assert store.get("inc-1").agent_logs[-1].action == "FIX_GENERATED"

    assert len(events) == 1
    assert events[0].event_type == AgentEventType.FIX_GENERATED
    assert events[0].payload["fix_id"] == report.fix_id
    assert events[0].payload["is_valid"] is True


def test_generate_fix_raises_without_diagnosis(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    plain = Incident(id="inc-x", status=IncidentStatus.ROOT_CAUSE_IDENTIFIED, victim_urn=VICTIM_URN, failure_type=FailureType.SCHEMA_DRIFT)
    with pytest.raises(ValueError, match="no DiagnosisReport"):
        run(agent.generate_fix(plain))


def test_generate_fix_raises_on_invalid_initial_status(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = diagnosed_incident()
    incident.status = IncidentStatus.DETECTED  # not diagnosable state
    with pytest.raises(ValueError, match="cannot transition"):
        run(agent.generate_fix(incident))


# ---------------------------------------------------------------------------
# fix_open_incidents / run
# ---------------------------------------------------------------------------
def test_fix_open_incidents_picks_root_cause_and_isolates_failures(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    store.create(diagnosed_incident("inc-a"))
    store.create(diagnosed_incident("inc-b"))
    broken = Incident(id="inc-broken", status=IncidentStatus.ROOT_CAUSE_IDENTIFIED, victim_urn=VICTIM_URN, failure_type=FailureType.SCHEMA_DRIFT)  # no diagnosis
    store.create(broken)
    agent = make_agent(base_fixture(), store)

    reports = run(agent.fix_open_incidents())

    assert {r.incident_id for r in reports} == {"inc-a", "inc-b"}
    assert all(r.is_valid for r in reports)


def test_run_pipeline(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    store.create(diagnosed_incident("inc-a"))
    store.create(diagnosed_incident("inc-b"))
    agent = make_agent(base_fixture(), store)

    reports = run(agent.run(limit=2))

    assert len(reports) == 2
    assert store.get("inc-a").status == IncidentStatus.FIX_PROPOSED


# ---------------------------------------------------------------------------
# prompt building
# ---------------------------------------------------------------------------
def test_build_prompt_contains_real_schema_and_diagnosis(tmp_path):
    agent = make_agent(base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl")))
    incident = diagnosed_incident()

    from src.datahub.mcp_client import DatasetSchema as DS

    prompt = agent._build_prompt(
        incident,
        DS(fields=[SchemaField(field_path="id", native_type="NUMBER", description="order id")]),
        [DS(fields=[SchemaField(field_path="total", native_type="FLOAT")])],
        incident.diagnosis,
    )

    assert "INCIDENT: SCHEMA_DRIFT on orders" in prompt
    assert "ROOT CAUSE: Schema drift on orders vs raw_orders" in prompt
    assert "id NUMBER -- order id" in prompt
    assert "total FLOAT" in prompt
    assert "GENERATE A SQL FIX THAT:" in prompt


# ---------------------------------------------------------------------------
# state machine
# ---------------------------------------------------------------------------
def test_state_machine_supports_fix_proposed():
    incident = diagnosed_incident()
    machine = IncidentStateMachine(incident)

    assert machine.can_transition_to(IncidentStatus.FIXING)
    machine.transition_to(IncidentStatus.FIXING)
    assert machine.can_transition_to(IncidentStatus.FIX_PROPOSED)
    machine.transition_to(IncidentStatus.FIX_PROPOSED)
    assert incident.status == IncidentStatus.FIX_PROPOSED

    # Validator Agent (next story) can pick it up.
    assert machine.can_transition_to(IncidentStatus.VALIDATING)

    # Skipping FIXING is not allowed.
    direct = diagnosed_incident("sm-2")
    with pytest.raises(ValueError):
        IncidentStateMachine(direct).transition_to(IncidentStatus.FIX_PROPOSED)


# ---------------------------------------------------------------------------
# context manager with the real client
# ---------------------------------------------------------------------------
def test_engineer_agent_context_manager_with_real_client(tmp_path):
    import httpx

    def handler(request):
        return httpx.Response(200, json={"data": {"search": {"searchResults": []}}, "extensions": {}})

    client = DataHubMCPClient(gms_url="http://datahub.test", transport=httpx.MockTransport(handler))
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))

    async def go():
        async with EngineerAgent(client, incident_store=store) as agent:
            return await agent.fix_open_incidents()

    assert run(go()) == []
