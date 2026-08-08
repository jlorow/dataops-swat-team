"""Unit tests for the Validator Agent (no network required).

The DataHub MCP client is replaced with an offline fake so the tests focus on
SQL parsing, schema/lineage checks, safety scoring, and the
FIX_PROPOSED -> VALIDATING -> READY_TO_DEPLOY / ESCALATED / FIX_PROPOSED
transitions.
"""

import asyncio

from src.agents.validator_agent import ValidationResult, ValidatorAgent
from src.datahub.mcp_client import (
    DatasetSchema,
    LineageEdge,
    LineageGraph,
    SchemaField,
)
from src.models import (
    AgentEventType,
    FailureType,
    FixReport,
    Incident,
    IncidentStatus,
)
from src.orchestrator import EventBus, IncidentStore

VICTIM_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"
UP1_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.raw_orders,PROD)"
DOWN1_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.order_history,PROD)"


def run(coro):
    return asyncio.run(coro)


class FakeDataHub:
    """Offline stand-in for DataHubMCPClient."""

    def __init__(self, schemas=None, lineage=None):
        self.schemas = schemas or {}
        self.lineage = lineage or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def get_dataset_schema(self, urn):
        return DatasetSchema(fields=self.schemas.get(urn, []))

    async def get_dataset_lineage(self, urn):
        data = self.lineage.get(urn, {"up": [], "down": []})
        return LineageGraph(
            upstreams=data.get("up", []), downstreams=data.get("down", [])
        )


def base_fixture():
    """Victim `orders` with upstream raw_orders and downstream order_history."""
    return {
        "schemas": {
            VICTIM_URN: [
                SchemaField(field_path="id", native_type="NUMBER", description="order id"),
                SchemaField(field_path="total", native_type="FLOAT", description="amount"),
                SchemaField(field_path="order_ts", native_type="TIMESTAMP", description="placed at"),
            ],
            UP1_URN: [
                SchemaField(field_path="id", native_type="NUMBER", description="id"),
                SchemaField(field_path="total", native_type="FLOAT", description="amount"),
            ],
            DOWN1_URN: [
                SchemaField(field_path="id", native_type="NUMBER", description="order id"),
                SchemaField(field_path="total", native_type="FLOAT", description="amount"),
            ],
        },
        "lineage": {
            VICTIM_URN: {
                "up": [LineageEdge(urn=UP1_URN, name="raw_orders", type="DATASET")],
                "down": [LineageEdge(urn=DOWN1_URN, name="order_history", type="DATASET")],
            }
        },
    }


def make_agent(fixture, store, bus=None):
    return ValidatorAgent(
        mcp_client=FakeDataHub(**fixture),
        event_bus=bus or EventBus(),
        incident_store=store,
    )


def proposed_incident(store, incident_id, sql, fix_type="SQL_PATCH"):
    incident = Incident(
        id=incident_id,
        status=IncidentStatus.FIX_PROPOSED,
        victim_urn=VICTIM_URN,
        failure_type=FailureType.SCHEMA_DRIFT,
    )
    incident.fix = FixReport(
        fix_id=f"fix-{incident_id}",
        incident_id=incident_id,
        target_dataset_urn=VICTIM_URN,
        sql_code=sql,
        fix_type=fix_type,
    )
    store.create(incident)
    return incident


# ---------------------------------------------------------------------------
# schema reference check
# ---------------------------------------------------------------------------
def test_add_column_is_exempt_from_schema_check(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-safe", "ALTER TABLE orders ADD COLUMN loyalty_rank INT;")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.schema_check["passed"] is True
    assert result.recommendation == "DEPLOY"
    assert result.safety_score == 1.0
    assert result.is_safe is True


def test_select_missing_column_fails_schema_check(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-missing", "SELECT id, mystery_col FROM orders;")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.schema_check["passed"] is False
    assert result.schema_check["missing_columns"] == ["mystery_col"]
    assert result.safety_score == 0.7  # 1.0 - 0.3 schema
    assert result.recommendation == "REVIEW"


def test_alter_missing_column_fails_schema_check(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(
        store, "inc-alter", "ALTER TABLE orders ALTER COLUMN ghost_col SET DATA TYPE TEXT;"
    )

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.schema_check["passed"] is False
    assert "ghost_col" in result.schema_check["missing_columns"]


def test_alter_column_type_incompatible_with_upstream_is_a_concern(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(
        store, "inc-type", "ALTER TABLE orders ALTER COLUMN id SET DATA TYPE TEXT;"
    )

    result = run(agent.validate_fix(incident, incident.fix))

    # Column exists, so the schema check passes; the type clash is a concern.
    assert result.schema_check["passed"] is True
    assert any("id" in c for c in result.schema_check["type_concerns"])
    assert "incompatible" in result.schema_check["type_concerns"][0]


# ---------------------------------------------------------------------------
# lineage impact check
# ---------------------------------------------------------------------------
def test_drop_column_used_downstream_is_breaking(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-drop", "ALTER TABLE orders DROP COLUMN id;")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.lineage_check["passed"] is False
    assert result.lineage_check["affected_downstream"], "must name affected datasets"
    assert "order_history" in result.lineage_check["affected_downstream"][0]
    assert result.safety_score == 0.6  # 1.0 - 0.4 lineage
    assert result.recommendation == "REVIEW"


def test_add_column_has_no_lineage_impact(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-add2", "ALTER TABLE orders ADD COLUMN x INT;")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.lineage_check["passed"] is True
    assert result.lineage_check["affected_downstream"] == []


# ---------------------------------------------------------------------------
# syntax deep check
# ---------------------------------------------------------------------------
def test_unbalanced_parentheses_fails_syntax(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-paren", "SELECT id FROM orders WHERE (id > 0;")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.syntax_check["passed"] is False
    assert any("parenthes" in e for e in result.syntax_check["errors"])
    assert result.safety_score == 0.7  # 1.0 - 0.3 syntax


def test_multi_statement_missing_semicolon_fails_syntax(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(
        store, "inc-semi", "SELECT id FROM orders; SELECT total FROM orders"
    )

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.syntax_check["passed"] is False
    assert any("semicolon" in e for e in result.syntax_check["errors"])


def test_empty_sql_fails_syntax(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-empty", "   ")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.syntax_check["passed"] is False
    # Syntax failure alone scores 1.0 - 0.3 = 0.7 -> REVIEW (back to Engineer).
    assert result.safety_score == 0.7
    assert result.recommendation == "REVIEW"


def test_reserved_keyword_as_added_column_fails_syntax(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-res", "ALTER TABLE orders ADD COLUMN order INT;")

    result = run(agent.validate_fix(incident, incident.fix))

    assert result.syntax_check["passed"] is False
    assert any("reserved" in e for e in result.syntax_check["errors"])


# ---------------------------------------------------------------------------
# scoring + recommendation
# ---------------------------------------------------------------------------
def test_recommendation_thresholds():
    assert ValidatorAgent._recommendation(0.8) == "DEPLOY"
    assert ValidatorAgent._recommendation(1.0) == "DEPLOY"
    assert ValidatorAgent._recommendation(0.5) == "REVIEW"
    assert ValidatorAgent._recommendation(0.6) == "REVIEW"
    assert ValidatorAgent._recommendation(0.49) == "ESCALATE"
    assert ValidatorAgent._recommendation(0.0) == "ESCALATE"


def test_safety_score_subtracts_per_failure(tmp_path):
    agent = make_agent(
        base_fixture(), IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    )
    score = agent._compute_safety_score(
        {"passed": False}, {"passed": True}, {"passed": True}
    )
    assert score == 0.7
    score = agent._compute_safety_score(
        {"passed": False}, {"passed": False}, {"passed": False}
    )
    assert score == 0.0  # 1.0 - 0.3 - 0.4 - 0.3, clamped at 0
    score = agent._compute_safety_score(
        {"passed": True}, {"passed": True}, {"passed": True}
    )
    assert score == 1.0


# ---------------------------------------------------------------------------
# full pipeline / transitions / events
# ---------------------------------------------------------------------------
def test_run_pipeline_applies_all_decisions(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    bus = EventBus()
    events = []
    bus.subscribe(AgentEventType.VALIDATED, events.append)
    agent = make_agent(base_fixture(), store, bus=bus)

    proposed_incident(store, "inc-safe", "ALTER TABLE orders ADD COLUMN loyalty_rank INT;")
    proposed_incident(
        store,
        "inc-escalate",
        "ALTER TABLE orders DROP COLUMN id;\nSELECT mystery_col FROM orders;",
    )
    proposed_incident(store, "inc-review", "ALTER TABLE orders DROP COLUMN id;")

    updated = run(agent.run(limit=5))

    assert {i.id for i in updated} == {"inc-safe", "inc-escalate", "inc-review"}
    assert store.get("inc-safe").status == IncidentStatus.READY_TO_DEPLOY
    assert store.get("inc-escalate").status == IncidentStatus.ESCALATED
    # REVIEW sends the fix back to the Engineer.
    assert store.get("inc-review").status == IncidentStatus.FIX_PROPOSED

    assert len(events) == 3
    deploy_event = next(e for e in events if e.payload["fix_id"] == "fix-inc-safe")
    assert deploy_event.payload["recommendation"] == "DEPLOY"
    assert deploy_event.payload["safety_score"] == 1.0
    assert "Fix fix-inc-safe validated" in deploy_event.payload["message"]
    # Spec F: validation details must be included in the event metadata.
    assert deploy_event.payload["schema_check"]["passed"] is True
    assert deploy_event.payload["lineage_check"]["passed"] is True
    assert deploy_event.payload["syntax_check"]["passed"] is True
    assert deploy_event.payload["breaking_changes"] == []
    escalate_event = next(e for e in events if e.payload["fix_id"] == "fix-inc-escalate")
    assert escalate_event.payload["recommendation"] == "ESCALATE"
    assert escalate_event.payload["schema_check"]["passed"] is False
    assert escalate_event.payload["lineage_check"]["passed"] is False
    assert store.get("inc-escalate").agent_logs[-1].action == "VALIDATED"


def test_validate_fix_requires_fix_report(tmp_path):
    store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    agent = make_agent(base_fixture(), store)
    incident = proposed_incident(store, "inc-none", "SELECT id FROM orders;")
    incident.fix = None

    results = run(agent.validate_pending_fixes())
    assert results == []


def test_validation_result_model():
    result = ValidationResult(
        fix_id="fix-1",
        incident_id="inc-1",
        is_safe=True,
        safety_score=1.0,
        schema_check={"passed": True},
        lineage_check={"passed": True},
        syntax_check={"passed": True},
        breaking_changes=[],
        recommendation="DEPLOY",
        validator_notes="ok",
    )
    assert result.safety_score == 1.0
    assert result.recommendation == "DEPLOY"
