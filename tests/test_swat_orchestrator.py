"""Unit tests for the SWATOrchestrator (no network required).

The four agent classes are replaced with offline fakes so the tests focus on
pipeline coordination: stage bookkeeping, per-stage exception isolation,
incident queries, and incident detail assembly.
"""

import asyncio

from src.models import (
    AgentEvent,
    AgentEventType,
    DiagnosisReport,
    FailureType,
    FixReport,
    Incident,
    IncidentStatus,
)
from src.orchestrator import IncidentStateMachine, IncidentStore
from src.orchestrator import swat_orchestrator as orch_mod
from src.orchestrator.swat_orchestrator import SWATOrchestrator


def run(coro):
    return asyncio.run(coro)


async def _collect(agen):
    """Drain an async generator into a list (asyncio.run needs a coroutine)."""
    return [item async for item in agen]


def run_pipeline(orch, detect_limit=5):
    """Run the full pipeline async generator to completion."""
    return asyncio.run(_collect(orch.run_full_pipeline(detect_limit=detect_limit)))


VICTIM = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"


# ---------------------------------------------------------------------------
# Fake agents — duck-type the real agents against the real store/state machine
# ---------------------------------------------------------------------------
class FakeSentry:
    def __init__(self, mcp_client, event_bus=None, incident_store=None, **kwargs):
        self.mcp, self.bus, self.store = mcp_client, event_bus, incident_store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def run(self, dataset_limit=50):
        incident = Incident(
            id="inc-1",
            status=IncidentStatus.DETECTED,
            victim_urn=VICTIM,
            failure_type=FailureType.SCHEMA_DRIFT,
        )
        self.store.create(incident)
        self.bus.publish(
            AgentEvent(
                event_type=AgentEventType.INCIDENT_CREATED,
                incident_id="inc-1",
                agent_name="SENTRY",
                payload={"victim_urn": VICTIM},
            )
        )
        return [incident]


class FakeDetective:
    def __init__(self, mcp_client, event_bus=None, incident_store=None, **kwargs):
        self.mcp, self.bus, self.store = mcp_client, event_bus, incident_store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def run(self, limit=10):
        updated = []
        for incident in self.store.list_all():
            if incident.status != IncidentStatus.DETECTED or len(updated) >= limit:
                continue
            machine = IncidentStateMachine(incident)
            machine.transition_to(IncidentStatus.DIAGNOSING)
            machine.transition_to(IncidentStatus.ROOT_CAUSE_IDENTIFIED)
            incident.diagnosis = DiagnosisReport(
                root_cause_urn=VICTIM,
                root_cause_type=FailureType.SCHEMA_DRIFT,
                lineage_path=[],
                owner_email="alice@example.com",
                summary_text="Schema drift found vs upstream",
                recommended_fix_type="SCHEMA_UPDATE",
                confidence_score=0.9,
            )
            self.store.update(incident)
            self.bus.publish(
                AgentEvent(
                    event_type=AgentEventType.DIAGNOSIS_COMPLETE,
                    incident_id=incident.id,
                    agent_name="DETECTIVE",
                )
            )
            updated.append(incident)
        return updated


class FakeEngineer:
    def __init__(self, mcp_client, llm_client=None, event_bus=None, incident_store=None, **kwargs):
        self.mcp, self.bus, self.store = mcp_client, event_bus, incident_store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def run(self, limit=10):
        reports = []
        for incident in self.store.list_all():
            if incident.status != IncidentStatus.ROOT_CAUSE_IDENTIFIED or len(reports) >= limit:
                continue
            machine = IncidentStateMachine(incident)
            machine.transition_to(IncidentStatus.FIXING)
            machine.transition_to(IncidentStatus.FIX_PROPOSED)
            report = FixReport(
                fix_id=f"fix-{incident.id}",
                incident_id=incident.id,
                target_dataset_urn=VICTIM,
                sql_code="ALTER TABLE orders ADD COLUMN total FLOAT;",
                fix_type="SCHEMA_UPDATE",
            )
            incident.fix = report
            self.store.update(incident)
            self.bus.publish(
                AgentEvent(
                    event_type=AgentEventType.FIX_GENERATED,
                    incident_id=incident.id,
                    agent_name="ENGINEER",
                    payload={"fix_id": report.fix_id},
                )
            )
            reports.append(report)
        return reports


class FakeValidator:
    def __init__(self, mcp_client, event_bus=None, incident_store=None, **kwargs):
        self.mcp, self.bus, self.store = mcp_client, event_bus, incident_store

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def run(self, limit=10):
        updated = []
        for incident in self.store.list_all():
            if incident.status != IncidentStatus.FIX_PROPOSED or len(updated) >= limit:
                continue
            machine = IncidentStateMachine(incident)
            machine.transition_to(IncidentStatus.VALIDATING)
            machine.transition_to(IncidentStatus.READY_TO_DEPLOY)
            self.store.update(incident)
            self.bus.publish(
                AgentEvent(
                    event_type=AgentEventType.VALIDATED,
                    incident_id=incident.id,
                    agent_name="VALIDATOR",
                    payload={
                        "message": f"Fix {incident.fix.fix_id} validated",
                        "fix_id": incident.fix.fix_id,
                        "safety_score": 1.0,
                        "recommendation": "DEPLOY",
                        "schema_check": {"passed": True},
                        "lineage_check": {"passed": True},
                        "syntax_check": {"passed": True},
                        "breaking_changes": [],
                    },
                )
            )
            updated.append(incident)
        return updated


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------
def make_orch(tmp_path, monkeypatch, fail_detect=False):
    monkeypatch.setattr(orch_mod, "SentryAgent", FakeSentry)
    monkeypatch.setattr(orch_mod, "DetectiveAgent", FakeDetective)
    monkeypatch.setattr(orch_mod, "EngineerAgent", FakeEngineer)
    monkeypatch.setattr(orch_mod, "ValidatorAgent", FakeValidator)
    orch = SWATOrchestrator(gms_url="http://datahub.test")
    orch.store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))
    return orch


# ---------------------------------------------------------------------------
# stage + pipeline coordination
# ---------------------------------------------------------------------------
def test_run_full_pipeline_yields_four_complete_stages(tmp_path, monkeypatch):
    orch = make_orch(tmp_path, monkeypatch)
    stages = run_pipeline(orch)
    assert [s.stage for s in stages] == ["DETECT", "DIAGNOSE", "ENGINEER", "VALIDATE"]
    assert all(s.status == "COMPLETE" for s in stages)
    assert stages[0].incident_count == 1  # detected
    assert stages[1].incident_count == 1  # diagnosed
    assert stages[2].incident_count == 1  # fixes
    assert stages[3].incident_count == 1  # validated

    incident = orch.store.get("inc-1")
    assert incident.status == IncidentStatus.READY_TO_DEPLOY


def test_run_full_pipeline_isolates_stage_failures(tmp_path, monkeypatch):
    class ExplodingSentry(FakeSentry):
        async def run(self, dataset_limit=50):
            raise RuntimeError("DataHub down")

    monkeypatch.setattr(orch_mod, "SentryAgent", ExplodingSentry)
    monkeypatch.setattr(orch_mod, "DetectiveAgent", FakeDetective)
    monkeypatch.setattr(orch_mod, "EngineerAgent", FakeEngineer)
    monkeypatch.setattr(orch_mod, "ValidatorAgent", FakeValidator)
    orch = SWATOrchestrator(gms_url="http://datahub.test")
    orch.store = IncidentStore(persist_path=str(tmp_path / "i.jsonl"))

    stages = run_pipeline(orch)

    assert stages[0].status == "FAILED"
    assert "DataHub down" in stages[0].result_summary
    # The remaining stages must still run (empty store -> 0 work, COMPLETE).
    assert all(s.status == "COMPLETE" for s in stages[1:])


# ---------------------------------------------------------------------------
# queries for the UI
# ---------------------------------------------------------------------------
def test_get_incidents_filters_by_status(tmp_path, monkeypatch):
    orch = make_orch(tmp_path, monkeypatch)
    store = orch.store
    store.create(Incident(id="a", status=IncidentStatus.DETECTED, victim_urn=VICTIM, failure_type=FailureType.SCHEMA_DRIFT))
    store.create(Incident(id="b", status=IncidentStatus.ESCALATED, victim_urn=VICTIM, failure_type=FailureType.SCHEMA_DRIFT))

    assert {i.id for i in orch.get_incidents()} == {"a", "b"}
    assert [i.id for i in orch.get_incidents(IncidentStatus.DETECTED)] == ["a"]
    assert [i.id for i in orch.get_incidents(IncidentStatus.ESCALATED)] == ["b"]


def test_get_incident_detail_assembles_reports_and_events(tmp_path, monkeypatch):
    orch = make_orch(tmp_path, monkeypatch)
    run_pipeline(orch)

    detail = orch.get_incident_detail("inc-1")
    assert detail is not None
    assert detail["incident"].status == IncidentStatus.READY_TO_DEPLOY
    assert detail["diagnosis"] is not None
    assert detail["diagnosis"].summary_text == "Schema drift found vs upstream"
    assert detail["fix_report"] is not None
    assert "ADD COLUMN" in detail["fix_report"].sql_code
    assert detail["validation_result"]["recommendation"] == "DEPLOY"
    assert detail["validation_result"]["safety_score"] == 1.0
    event_types = {e.event_type for e in detail["events"]}
    assert event_types == {
        AgentEventType.INCIDENT_CREATED,
        AgentEventType.DIAGNOSIS_COMPLETE,
        AgentEventType.FIX_GENERATED,
        AgentEventType.VALIDATED,
    }


def test_get_incident_detail_missing_incident_returns_none(tmp_path, monkeypatch):
    orch = make_orch(tmp_path, monkeypatch)
    assert orch.get_incident_detail("nope") is None


def test_pipeline_status_tracks_stages(tmp_path, monkeypatch):
    orch = make_orch(tmp_path, monkeypatch)
    assert orch.get_pipeline_status() == []
    run_pipeline(orch)
    status = orch.get_pipeline_status()
    assert [s.stage for s in status] == ["DETECT", "DIAGNOSE", "ENGINEER", "VALIDATE"]
    assert all(s.status == "COMPLETE" for s in status)
