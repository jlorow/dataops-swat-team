"""SWATOrchestrator — full DataOps SWAT agent pipeline coordinator.

Runs the four agents end-to-end against a real DataHub instance:

    DETECT    SentryAgent    scan DataHub metadata, create Incident records
    DIAGNOSE  DetectiveAgent investigate DETECTED incidents via lineage/schema
    ENGINEER  EngineerAgent  generate SQL fixes for ROOT_CAUSE_IDENTIFIED
    VALIDATE  ValidatorAgent validate fixes against schema/lineage, gate deploy

The orchestrator also:

- captures every AgentEvent published on the EventBus (for incident detail logs),
- exposes ``get_incidents`` / ``get_incident_detail`` for the Streamlit UI,
- tracks per-stage ``PipelineStage`` status for real-time progress rendering.

Usage:
    orch = SWATOrchestrator(gms_url="http://67.205.141.90:8080")
    async for stage in orch.run_full_pipeline():
        print(stage.stage, stage.status)  # real-time progress
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from pydantic import BaseModel

from src.agents import (
    DetectiveAgent,
    EngineerAgent,
    SentryAgent,
    ValidatorAgent,
)
from src.datahub.mcp_client import DataHubMCPClient
from src.llm import OpenRouterClient
from src.models import AgentEvent, AgentEventType, Incident, IncidentStatus
from src.orchestrator import EventBus, IncidentStore

logger = logging.getLogger(__name__)

# Ordered pipeline stages shown in the UI sidebar.
STAGE_ORDER = ["DETECT", "DIAGNOSE", "ENGINEER", "VALIDATE"]


class PipelineStage(BaseModel):
    """Status of a single pipeline stage."""

    stage: str  # "DETECT", "DIAGNOSE", "ENGINEER", "VALIDATE"
    status: str = "PENDING"  # "PENDING", "RUNNING", "COMPLETE", "FAILED", "SKIPPED"
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result_summary: str = ""
    incident_count: int = 0


class SWATOrchestrator:
    """
    Orchestrates the full DataOps SWAT pipeline: Detect -> Diagnose -> Engineer -> Validate.

    Usage:
        orch = SWATOrchestrator(gms_url="http://67.205.141.90:8080")
        async for update in orch.run_full_pipeline():
            print(update)  # Real-time stage updates
    """

    def __init__(
        self, gms_url: str = "http://67.205.141.90:8080", llm_api_key: Optional[str] = None
    ) -> None:
        self.gms_url = gms_url
        self.mcp = DataHubMCPClient(gms_url=gms_url)
        self.store = IncidentStore()
        self.bus = EventBus()
        self.llm = OpenRouterClient(api_key=llm_api_key) if llm_api_key else None
        self.stages: List[PipelineStage] = []

        # Capture every AgentEvent the agents publish so the UI can show logs.
        self._events: List[AgentEvent] = []
        for event_type in AgentEventType:
            self.bus.subscribe(event_type, self._events.append)

    # -- Full pipeline ---------------------------------------------------------

    async def run_full_pipeline(
        self, detect_limit: int = 20
    ) -> AsyncGenerator[PipelineStage, None]:
        """
        Run the complete 4-agent pipeline with real-time progress updates.
        Yields a PipelineStage after each stage completes.
        """
        self.stages = [PipelineStage(stage=name) for name in STAGE_ORDER]

        stage = self._begin("DETECT")
        try:
            incidents = await self.detect(limit=detect_limit)
            self._complete(
                stage, len(incidents), f"Detected {len(incidents)} new incident(s)"
            )
        except Exception as exc:  # noqa: BLE001 - one stage must not kill the run
            self._fail(stage, f"DETECT failed: {exc}")
        yield stage

        stage = self._begin("DIAGNOSE")
        try:
            updated = await self.diagnose(limit=10)
            self._complete(
                stage, len(updated), f"Diagnosed {len(updated)} incident(s)"
            )
        except Exception as exc:  # noqa: BLE001
            self._fail(stage, f"DIAGNOSE failed: {exc}")
        yield stage

        stage = self._begin("ENGINEER")
        try:
            fixes = await self.engineer(limit=10)
            self._complete(stage, len(fixes), f"Generated {len(fixes)} fix(es)")
        except Exception as exc:  # noqa: BLE001
            self._fail(stage, f"ENGINEER failed: {exc}")
        yield stage

        stage = self._begin("VALIDATE")
        try:
            updated = await self.validate(limit=10)
            self._complete(
                stage, len(updated), f"Validated {len(updated)} fix(es)"
            )
        except Exception as exc:  # noqa: BLE001
            self._fail(stage, f"VALIDATE failed: {exc}")
        yield stage

    # -- Individual agent stages ------------------------------------------------

    async def detect(self, limit: int = 20) -> List[Incident]:
        """Run SentryAgent. Return created incidents."""
        async with SentryAgent(
            self.mcp, event_bus=self.bus, incident_store=self.store
        ) as agent:
            return await agent.run(dataset_limit=limit)

    async def diagnose(self, limit: int = 10) -> List[Incident]:
        """Run DetectiveAgent on open incidents. Return updated incidents."""
        async with DetectiveAgent(
            self.mcp, event_bus=self.bus, incident_store=self.store
        ) as agent:
            return await agent.run(limit=limit)

    async def engineer(self, limit: int = 10) -> List[Any]:
        """Run EngineerAgent on diagnosed incidents. Return FixReports."""
        async with EngineerAgent(
            self.mcp,
            llm_client=self.llm,
            event_bus=self.bus,
            incident_store=self.store,
        ) as agent:
            return await agent.run(limit=limit)

    async def validate(self, limit: int = 10) -> List[Incident]:
        """Run ValidatorAgent on proposed fixes. Return updated incidents."""
        async with ValidatorAgent(
            self.mcp, event_bus=self.bus, incident_store=self.store
        ) as agent:
            return await agent.run(limit=limit)

    # -- Queries for the UI -------------------------------------------------------

    def get_incidents(self, status: Optional[IncidentStatus] = None) -> List[Incident]:
        """Fetch incidents from store, optionally filtered by status."""
        incidents = self.store.list_all()
        if status is not None:
            incidents = [incident for incident in incidents if incident.status == status]
        return sorted(incidents, key=lambda i: i.created_at, reverse=True)

    def get_incident_detail(self, incident_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full incident detail including diagnosis, fix, validation and events."""
        incident = self.store.get(incident_id)
        if incident is None:
            return None

        # Latest VALIDATED event carries the full validation result payload.
        validation_result: Optional[Dict[str, Any]] = None
        for event in reversed(self._events):
            if event.incident_id == incident_id and event.event_type == AgentEventType.VALIDATED:
                validation_result = event.payload
                break

        return {
            "incident": incident,
            "diagnosis": incident.diagnosis,
            "fix_report": incident.fix,
            "validation_result": validation_result,
            "events": [event for event in self._events if event.incident_id == incident_id],
        }

    def get_pipeline_status(self) -> List[PipelineStage]:
        """Return current pipeline stage statuses."""
        return self.stages

    # -- Stage bookkeeping ----------------------------------------------------------

    def _begin(self, stage_name: str) -> PipelineStage:
        stage = next(s for s in self.stages if s.stage == stage_name)
        stage.status = "RUNNING"
        stage.started_at = datetime.now(timezone.utc)
        return stage

    def _complete(self, stage: PipelineStage, count: int, summary: str) -> None:
        stage.status = "COMPLETE"
        stage.completed_at = datetime.now(timezone.utc)
        stage.incident_count = count
        stage.result_summary = summary

    def _fail(self, stage: PipelineStage, summary: str) -> None:
        stage.status = "FAILED"
        stage.completed_at = datetime.now(timezone.utc)
        stage.result_summary = summary
