"""Sentry Agent — data incident detection.

The Sentry Agent continuously scans DataHub metadata to detect data incidents:

- ``FRESHNESS_VIOLATION`` — dataset not updated within ``freshness_threshold_hours``
- ``OWNERSHIP_GAP``     — dataset has zero owners in DataHub
- ``LINEAGE_GAP``       — dataset has no upstream AND no downstream lineage
- ``SCHEMA_DRIFT``      — schema fields are missing descriptions

When anomalies are found, ``create_incidents`` turns them into ``Incident``
records (status ``DETECTED``), persists them via ``IncidentStore`` and
publishes an ``AgentEvent`` on the ``EventBus`` for downstream agents.

Usage:
    async with SentryAgent(DataHubMCPClient(gms_url=...)) as agent:
        incidents = await agent.run(dataset_limit=50)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from src.datahub.mcp_client import (
    DataHubMCPClient,
    DatasetInfo,
    DatasetSchema,
    LineageGraph,
)
from src.models import (
    AgentEvent,
    AgentEventType,
    AgentLogEntry,
    AgentType,
    FailureType,
    Incident,
    IncidentStatus,
    IncidentType,
)
from src.orchestrator import EventBus, IncidentStateMachine, IncidentStore

logger = logging.getLogger(__name__)


class DetectionRule(BaseModel):
    """Configuration for what the Sentry Agent checks."""

    freshness_threshold_hours: int = Field(
        default=168, description="Flag datasets not modified in N hours"
    )
    require_ownership: bool = Field(
        default=True, description="Flag datasets with zero owners"
    )
    require_lineage: bool = Field(
        default=True,
        description="Flag datasets with no upstream AND no downstream",
    )
    check_schema_descriptions: bool = Field(
        default=True, description="Flag schema fields missing descriptions"
    )


class DetectedAnomaly(BaseModel):
    """A single anomaly found during scanning."""

    dataset_urn: str
    dataset_name: str
    anomaly_type: IncidentType  # SCHEMA_DRIFT, FRESHNESS_VIOLATION, OWNERSHIP_GAP, LINEAGE_GAP
    severity: str  # "HIGH", "MEDIUM", "LOW"
    description: str
    evidence: Dict[str, Any] = Field(default_factory=dict)  # Raw metadata that triggered the anomaly


# Number of datasets scanned concurrently (each opens up to 4 GraphQL calls).
_SCAN_CONCURRENCY = 5


def _display_name_from_urn(urn: str) -> str:
    """Best-effort fallback name extracted from a dataset URN.

    Handles ``urn:li:dataset:(urn:li:dataPlatform:dbt,path.to.name,PROD)`` by
    taking the segment after the last '.' of the path portion.
    """
    if "(" in urn and "," in urn:
        inner = urn.split("(", 1)[1].rsplit(")", 1)[0]
        path = inner.split(",", 1)[-1].rsplit(",", 1)[0]
        if "." in path:
            return path.split(".")[-1]
    return urn.rsplit(":", 1)[-1]


def _incident_id(anomaly: DetectedAnomaly) -> str:
    """Deterministic incident id so re-scanning the same anomaly deduplicates."""
    digest = hashlib.sha1(anomaly.dataset_urn.encode("utf-8")).hexdigest()[:12]
    return f"inc-{anomaly.anomaly_type.value.lower()}-{digest}"


class SentryAgent:
    """
    Async agent that scans DataHub metadata and detects incidents.

    Usage:
        async with SentryAgent(mcp_client) as agent:
            anomalies = await agent.scan()
            incidents = await agent.create_incidents(anomalies)
    """

    def __init__(
        self,
        mcp_client: DataHubMCPClient,
        event_bus: Optional[EventBus] = None,
        incident_store: Optional[IncidentStore] = None,
        rules: Optional[DetectionRule] = None,
    ) -> None:
        self.mcp = mcp_client
        self.bus = event_bus or EventBus()
        self.store = incident_store or IncidentStore()
        self.rules = rules or DetectionRule()
        self.agent_type = AgentType.SENTRY

    async def __aenter__(self) -> "SentryAgent":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.mcp.__aexit__(exc_type, exc_val, exc_tb)

    # -- Detection ----------------------------------------------------------

    async def scan(self, dataset_limit: int = 50) -> List[DetectedAnomaly]:
        """
        Scan datasets in DataHub and detect anomalies based on rules.
        Returns a list of DetectedAnomaly objects (NOT Incidents yet).
        """
        datasets = (await self.mcp.search_datasets("*", count=dataset_limit))[
            :dataset_limit
        ]
        logger.info("Sentry: scanning %d datasets", len(datasets))
        semaphore = asyncio.Semaphore(_SCAN_CONCURRENCY)

        async def scan_one(dataset: DatasetInfo) -> List[DetectedAnomaly]:
            async with semaphore:
                try:
                    return await self._scan_dataset(dataset)
                except Exception as exc:
                    # One failing dataset must not abort the whole sweep.
                    logger.warning(
                        "Sentry: skipping dataset %s (%s): %s",
                        dataset.urn,
                        dataset.name,
                        exc,
                    )
                    return []

        per_dataset = await asyncio.gather(*(scan_one(d) for d in datasets))
        anomalies = [anomaly for found in per_dataset for anomaly in found]
        logger.info(
            "Sentry: found %d anomalies across %d datasets",
            len(anomalies),
            len(datasets),
        )
        return anomalies

    async def _scan_dataset(self, dataset: DatasetInfo) -> List[DetectedAnomaly]:
        """Fetch all metadata for one dataset in parallel and run the rules."""
        properties, ownership, lineage, schema = await asyncio.gather(
            self.mcp.get_dataset_properties(dataset.urn),
            self.mcp.get_dataset_ownership(dataset.urn),
            self.mcp.get_dataset_lineage(dataset.urn),
            self.mcp.get_dataset_schema(dataset.urn),
        )
        checks = [
            self._check_freshness(properties, dataset.urn, dataset.name),
            self._check_ownership(dataset.urn, ownership.owners, dataset.name),
            self._check_lineage(dataset.urn, lineage, dataset.name),
            self._check_schema_descriptions(dataset.urn, schema, dataset.name),
        ]
        return [anomaly for anomaly in checks if anomaly is not None]

    def _check_freshness(
        self,
        properties: Dict[str, Any],
        dataset_urn: str = "",
        dataset_name: str = "",
    ) -> Optional[DetectedAnomaly]:
        """Check if dataset lastModified is older than threshold."""
        threshold = self.rules.freshness_threshold_hours
        last_modified_ms = properties.get("last_modified")
        # None means no timestamp was reported; 0 (epoch) is a real timestamp
        # meaning the entity has never been updated and therefore is stale.
        if not threshold or last_modified_ms is None:
            return None

        last_modified = datetime.fromtimestamp(last_modified_ms / 1000, tz=timezone.utc)
        hours_since = (datetime.now(timezone.utc) - last_modified).total_seconds() / 3600
        if hours_since <= threshold:
            return None

        severity = "HIGH" if hours_since > threshold * 4 else "MEDIUM"
        return DetectedAnomaly(
            dataset_urn=dataset_urn,
            dataset_name=dataset_name or _display_name_from_urn(dataset_urn),
            anomaly_type=IncidentType.FRESHNESS_VIOLATION,
            severity=severity,
            description=(
                f"Dataset not updated in {hours_since:.1f}h "
                f"(threshold {threshold}h; last modified {last_modified.isoformat()})"
            ),
            evidence={
                "last_modified_ms": last_modified_ms,
                "last_modified_iso": last_modified.isoformat(),
                "hours_since_update": round(hours_since, 1),
                "threshold_hours": threshold,
            },
        )

    def _check_ownership(
        self,
        dataset_urn: str,
        owners: List[Any],
        dataset_name: str = "",
    ) -> Optional[DetectedAnomaly]:
        """Check if dataset has zero owners."""
        if not self.rules.require_ownership:
            return None
        if owners:
            return None
        return DetectedAnomaly(
            dataset_urn=dataset_urn,
            dataset_name=dataset_name or _display_name_from_urn(dataset_urn),
            anomaly_type=IncidentType.OWNERSHIP_GAP,
            severity="HIGH",
            description="Dataset has no owners assigned in DataHub",
            evidence={"owner_count": 0},
        )

    def _check_lineage(
        self,
        dataset_urn: str,
        lineage: Any,
        dataset_name: str = "",
    ) -> Optional[DetectedAnomaly]:
        """Check if dataset has no upstream and no downstream."""
        if not self.rules.require_lineage or not isinstance(lineage, LineageGraph):
            return None
        if lineage.upstreams or lineage.downstreams:
            return None
        return DetectedAnomaly(
            dataset_urn=dataset_urn,
            dataset_name=dataset_name or _display_name_from_urn(dataset_urn),
            anomaly_type=IncidentType.LINEAGE_GAP,
            severity="MEDIUM",
            description="Dataset has no upstream or downstream lineage",
            evidence={"upstream_count": 0, "downstream_count": 0},
        )

    def _check_schema_descriptions(
        self,
        dataset_urn: str,
        schema: Any,
        dataset_name: str = "",
    ) -> Optional[DetectedAnomaly]:
        """Check if schema fields are missing descriptions."""
        if not self.rules.check_schema_descriptions or not isinstance(schema, DatasetSchema):
            return None
        if not schema.fields:
            return None
        missing = [f.field_path for f in schema.fields if not (f.description or "").strip()]
        if not missing:
            return None
        total = len(schema.fields)
        missing_ratio = len(missing) / total
        severity = "MEDIUM" if missing_ratio >= 0.5 else "LOW"
        return DetectedAnomaly(
            dataset_urn=dataset_urn,
            dataset_name=dataset_name or _display_name_from_urn(dataset_urn),
            anomaly_type=IncidentType.SCHEMA_DRIFT,
            severity=severity,
            description=f"{len(missing)}/{total} schema fields missing descriptions",
            evidence={
                "fields_total": total,
                "fields_missing_descriptions": len(missing),
                "missing_ratio": round(missing_ratio, 3),
                "missing_fields": missing[:20],
            },
        )

    # -- Incident creation ---------------------------------------------------

    async def create_incidents(self, anomalies: List[DetectedAnomaly]) -> List[Incident]:
        """
        Convert anomalies into Incidents, persist them, and publish events.
        Uses IncidentStateMachine for status transitions.
        """
        incidents: List[Incident] = []
        for anomaly in anomalies:
            incident = self._build_incident(anomaly)

            # Deterministic ids mean a re-scan of the same anomaly must not
            # duplicate the incident (nor its JSONL line / published event).
            if self.store.get(incident.id) is not None:
                logger.info(
                    "Sentry: incident %s already exists, skipping", incident.id
                )
                continue

            # Preserve the detection detail on the incident record itself.
            incident.agent_logs.append(
                AgentLogEntry(
                    agent_name=self.agent_type.value,
                    action="DETECTED",
                    input_summary=(
                        f"anomaly={anomaly.anomaly_type.value} "
                        f"severity={anomaly.severity} dataset={anomaly.dataset_name}"
                    ),
                    output_summary=anomaly.description,
                    duration_ms=0,
                )
            )

            # Sanity check that the new incident can progress to DIAGNOSING so
            # the Detective Agent can pick it up.
            state_machine = IncidentStateMachine(incident)
            if not state_machine.can_transition_to(IncidentStatus.DIAGNOSING):
                logger.warning(
                    "Sentry: incident %s cannot progress past %s",
                    incident.id,
                    incident.status,
                )

            self.store.create(incident)
            self.bus.publish(
                AgentEvent(
                    event_type=AgentEventType.INCIDENT_CREATED,
                    incident_id=incident.id,
                    agent_name=self.agent_type.value,
                    payload={
                        "victim_urn": anomaly.dataset_urn,
                        "dataset_name": anomaly.dataset_name,
                        "anomaly_type": anomaly.anomaly_type.value,
                        "severity": anomaly.severity,
                        "description": anomaly.description,
                    },
                )
            )
            incidents.append(incident)
        return incidents

    def _build_incident(self, anomaly: DetectedAnomaly) -> Incident:
        """Create a DETECTED incident from an anomaly."""
        return Incident(
            id=_incident_id(anomaly),
            status=IncidentStatus.DETECTED,
            victim_urn=anomaly.dataset_urn,
            failure_type=FailureType(anomaly.anomaly_type.value),
        )

    async def run(self, dataset_limit: int = 50) -> List[Incident]:
        """Full pipeline: scan -> create_incidents. Returns created incidents."""
        anomalies = await self.scan(dataset_limit)
        return await self.create_incidents(anomalies)
