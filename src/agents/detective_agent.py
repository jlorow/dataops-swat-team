"""Detective Agent — root cause diagnosis.

The Detective Agent picks up incidents with status ``DETECTED`` (created by the
Sentry Agent), investigates the victim dataset through DataHub lineage, schema,
ownership and properties, and produces a structured root-cause diagnosis:

- **SCHEMA_DRIFT**      — compares the victim's schema against its upstreams'
  schemas to find where the drift originates (field/type mismatches).
- **FRESHNESS_VIOLATION** — traces upstream lineage (up to 3 levels) to find the
  stalest source dataset.
- **OWNERSHIP_GAP**     — checks upstream owners to recommend who should own
  the orphaned dataset.
- **LINEAGE_GAP**       — uses naming conventions (stg_, raw_, marts_, ...) to
  decide whether lineage is expected but missing.

Every investigation carries a confidence score (0.5 baseline; +0.2 lineage,
+0.2 schema comparison, +0.1 ownership info; capped at 1.0), an impact
assessment counting downstream consumers, and raw evidence.

``diagnose_and_update`` transitions the incident DETECTED -> DIAGNOSING ->
ROOT_CAUSE_IDENTIFIED via the IncidentStateMachine, attaches a DiagnosisReport,
persists it, and publishes a DIAGNOSIS_COMPLETE event.

Usage:
    async with DetectiveAgent(DataHubMCPClient(gms_url=...)) as agent:
        updated = await agent.run(limit=10)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from src.agents.sentry_agent import _display_name_from_urn
from src.datahub.mcp_client import (
    DataHubMCPClient,
    DatasetOwnership,
    DatasetSchema,
)
from src.models import (
    AgentEvent,
    AgentEventType,
    AgentLogEntry,
    AgentType,
    DiagnosisReport,
    FailureType,
    Incident,
    IncidentStatus,
)
from src.orchestrator import EventBus, IncidentStateMachine, IncidentStore

logger = logging.getLogger(__name__)

# Recommended fix type per incident failure type.
_FIX_TYPE_MAP: Dict[FailureType, str] = {
    FailureType.SCHEMA_DRIFT: "SCHEMA_UPDATE",
    FailureType.FRESHNESS_VIOLATION: "FRESHNESS_RERUN",
    FailureType.OWNERSHIP_GAP: "OWNER_ASSIGNMENT",
    FailureType.LINEAGE_GAP: "LINEAGE_REPAIR",
}

# Naming conventions that imply a dataset SHOULD have upstream lineage.
_LINEAGE_EXPECTED_PREFIXES = ("raw_", "stg_", "stage_", "dim_", "fct_", "marts_", "ods_")

# How many levels up the lineage we trace for the stalest upstream dataset.
_MAX_FRESHNESS_TRACE_DEPTH = 3


class InvestigationResult(BaseModel):
    """Findings from investigating a single incident."""

    incident_id: str
    root_cause_dataset_urn: Optional[str] = None
    root_cause_description: str
    impact_assessment: str  # e.g., "Affects 3 downstream tables and 2 dashboards"
    affected_datasets: List[str]  # URNs of all affected datasets
    recommended_fix_type: str  # "SCHEMA_UPDATE", "LINEAGE_REPAIR", "OWNER_ASSIGNMENT", "FRESHNESS_RERUN", etc.
    confidence_score: float = Field(ge=0.0, le=1.0)  # How certain is this diagnosis?
    evidence: Dict[str, Any] = Field(default_factory=dict)  # Raw metadata supporting the diagnosis


def _extract_owner_emails(ownership: DatasetOwnership) -> List[str]:
    """Pull emails embedded in corpuser URNs (urn:li:corpuser:<email>)."""
    emails: List[str] = []
    for owner in ownership.owners:
        if owner.owner_urn.startswith("urn:li:corpuser:") and "@" in owner.owner_urn:
            email = owner.owner_urn.rsplit(":", 1)[-1]
            if email not in emails:
                emails.append(email)
    return emails


def _format_timestamp(epoch_ms: Optional[int]) -> str:
    """Render an epoch-millis timestamp, tolerating epoch 0 and None."""
    if epoch_ms is None:
        return "unknown"
    try:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return str(epoch_ms)


class DetectiveAgent:
    """
    Async agent that diagnoses the root cause of detected incidents.

    Usage:
        async with DetectiveAgent(mcp_client) as agent:
            diagnoses = await agent.investigate_open_incidents()
    """

    def __init__(
        self,
        mcp_client: DataHubMCPClient,
        event_bus: Optional[EventBus] = None,
        incident_store: Optional[IncidentStore] = None,
    ) -> None:
        self.mcp = mcp_client
        self.bus = event_bus or EventBus()
        self.store = incident_store or IncidentStore()
        self.agent_type = AgentType.DETECTIVE

    async def __aenter__(self) -> "DetectiveAgent":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.mcp.__aexit__(exc_type, exc_val, exc_tb)

    # -- Investigation --------------------------------------------------------

    async def investigate_open_incidents(self, limit: int = 10) -> List[InvestigationResult]:
        """
        Fetch incidents with status DETECTED, investigate each, and return results.
        Does NOT modify incidents yet — pure investigation.
        """
        open_incidents = [
            incident
            for incident in self.store.list_all()
            if incident.status == IncidentStatus.DETECTED
        ][:limit]
        logger.info(
            "Detective: investigating %d open (DETECTED) incidents", len(open_incidents)
        )
        results: List[InvestigationResult] = []
        for incident in open_incidents:
            try:
                results.append(await self.investigate(incident))
            except Exception as exc:
                # One failing incident must not abort the batch.
                logger.warning("Detective: failed to investigate %s: %s", incident.id, exc)
        return results

    async def investigate(self, incident: Incident) -> InvestigationResult:
        """
        Deep-dive a single incident using DataHub lineage, schema, and ownership.
        """
        victim_urn = incident.victim_urn

        # All victim metadata is fetched in parallel.
        properties, ownership, lineage, schema = await asyncio.gather(
            self.mcp.get_dataset_properties(victim_urn),
            self.mcp.get_dataset_ownership(victim_urn),
            self.mcp.get_dataset_lineage(victim_urn),
            self.mcp.get_dataset_schema(victim_urn),
        )

        evidence: Dict[str, Any] = {
            "victim_urn": victim_urn,
            "lineage_upstream_count": len(lineage.upstreams),
            "lineage_downstream_count": len(lineage.downstreams),
            "schema_field_count": len(schema.fields),
            "owner_count": len(ownership.owners),
        }
        owner_emails = _extract_owner_emails(ownership)
        if owner_emails:
            evidence["owner_emails"] = owner_emails

        root_cause_urn: Optional[str] = None
        schema_compared = False

        if incident.failure_type == FailureType.SCHEMA_DRIFT:
            upstream_schemas = await self._fetch_upstream_schemas(lineage.upstreams)
            schema_compared = len(upstream_schemas) > 0
            root_cause_urn = self._find_mismatched_upstream(
                schema, lineage.upstreams, upstream_schemas
            )
            evidence["upstream_schema_count"] = len(upstream_schemas)
            root_cause_description = self._analyze_schema_drift(
                incident, schema, upstream_schemas
            )
        elif incident.failure_type == FailureType.FRESHNESS_VIOLATION:
            stalest = await self._trace_stalest_upstream(
                victim_urn, properties.get("last_modified")
            )
            root_cause_urn = stalest["urn"]
            evidence["stalest_upstream_urn"] = stalest["urn"]
            evidence["stalest_upstream_last_modified"] = stalest["last_modified_ms"]
            evidence["upstream_path"] = stalest["path"]
            root_cause_description = self._analyze_freshness(
                incident,
                properties,
                lineage,
                stalest_urn=stalest["urn"],
                stalest_ms=stalest["last_modified_ms"],
            )
        elif incident.failure_type == FailureType.OWNERSHIP_GAP:
            upstream_ownership = await self._fetch_upstream_ownership(lineage.upstreams)
            evidence["upstream_owner_types"] = sorted(
                {o.owner_type for up in upstream_ownership for o in up.owners}
            )
            evidence["upstream_owner_emails"] = [
                email for up in upstream_ownership for email in _extract_owner_emails(up)
            ]
            root_cause_description = self._analyze_ownership_gap(
                incident, ownership, upstream_ownership
            )
        elif incident.failure_type == FailureType.LINEAGE_GAP:
            root_cause_description = self._analyze_lineage_gap(incident, lineage)
        else:
            # BROKEN_JOB / MANUAL_TEST (or anything unexpected): no automated
            # analysis exists, so flag for manual review instead of guessing.
            root_cause_description = (
                f"No automated analysis available for failure type "
                f"{incident.failure_type.value}; manual review recommended."
            )

        impact_assessment = self._build_impact_assessment(victim_urn, lineage.downstreams)
        affected_datasets = [victim_urn] + [edge.urn for edge in lineage.downstreams]

        lineage_found = bool(lineage.upstreams or lineage.downstreams)
        ownership_found = bool(
            ownership.owners or evidence.get("upstream_owner_types")
        )
        confidence = self._compute_confidence(lineage_found, schema_compared, ownership_found)

        return InvestigationResult(
            incident_id=incident.id,
            root_cause_dataset_urn=root_cause_urn,
            root_cause_description=root_cause_description,
            impact_assessment=impact_assessment,
            affected_datasets=affected_datasets,
            recommended_fix_type=_FIX_TYPE_MAP.get(incident.failure_type, "MANUAL_REVIEW"),
            confidence_score=confidence,
            evidence=evidence,
        )

    # -- Helpers: schema drift -------------------------------------------------

    async def _fetch_upstream_schemas(self, upstreams: List[Any]) -> List[DatasetSchema]:
        """Fetch schemas for all first-hop upstream datasets in parallel."""
        if not upstreams:
            return []
        return list(
            await asyncio.gather(
                *[self.mcp.get_dataset_schema(edge.urn) for edge in upstreams]
            )
        )

    def _find_mismatched_upstream(
        self,
        schema: DatasetSchema,
        upstreams: List[Any],
        upstream_schemas: List[DatasetSchema],
    ) -> Optional[str]:
        """Return the URN of the upstream with the most divergent schema."""
        victim_fields = {field.field_path for field in schema.fields}
        best_urn: Optional[str] = None
        best_diff = 0
        for edge, upstream_schema in zip(upstreams, upstream_schemas):
            upstream_fields = {field.field_path for field in upstream_schema.fields}
            diff = len(victim_fields.symmetric_difference(upstream_fields))
            if diff > best_diff:
                best_urn, best_diff = edge.urn, diff
        return best_urn if best_diff > 0 else None

    def _analyze_schema_drift(
        self, incident: Incident, schema: Any, upstream_schemas: List[Any]
    ) -> str:
        """Compare schema with upstream to identify drift."""
        victim_fields = {field.field_path for field in schema.fields}
        if not upstream_schemas:
            return (
                f"Schema drift detected on {incident.victim_urn}: {len(schema.fields)} "
                f"fields present, but no upstream schema is available to compare against."
            )

        upstream_fields: set[str] = set()
        upstream_types: Dict[str, str] = {}
        for upstream_schema in upstream_schemas:
            for field in upstream_schema.fields:
                upstream_fields.add(field.field_path)
                if field.native_type:
                    upstream_types.setdefault(field.field_path, field.native_type)

        only_victim = sorted(victim_fields - upstream_fields)[:5]
        only_upstream = sorted(upstream_fields - victim_fields)[:5]
        type_mismatches = [
            f"{field.field_path} ({field.native_type} vs {upstream_types[field.field_path]})"
            for field in schema.fields
            if field.field_path in upstream_types
            and field.native_type
            and upstream_types[field.field_path] != field.native_type
        ][:3]

        parts = [f"Schema drift on {incident.victim_urn} vs upstream:"]
        if only_victim:
            parts.append(
                f"{len(only_victim)} fields present only downstream (e.g. {', '.join(only_victim)})"
            )
        if only_upstream:
            parts.append(
                f"{len(only_upstream)} upstream fields absent from victim (e.g. {', '.join(only_upstream)})"
            )
        if type_mismatches:
            parts.append(f"type mismatches on shared fields (e.g. {', '.join(type_mismatches)})")
        if len(parts) == 1:
            if victim_fields or upstream_fields:
                parts.append("shared fields match, but the field set differs upstream/downstream")
            else:
                parts.append("neither the victim nor upstream schemas have fields to compare")
        return "; ".join(parts)

    # -- Helpers: freshness -----------------------------------------------------

    async def _trace_stalest_upstream(
        self,
        dataset_urn: str,
        victim_last_modified_ms: Optional[int] = None,
        max_depth: int = _MAX_FRESHNESS_TRACE_DEPTH,
    ) -> Dict[str, Any]:
        """Walk upstream greedily toward the stalest source (up to max_depth levels).

        At each level, fetches the current dataset's upstream lineage and the
        properties of each first-hop upstream in parallel, then follows the
        branch with the oldest lastModified timestamp. Note this is a greedy
        single-branch walk: a stalest dataset nested under a younger first-hop
        upstream may be missed.
        """
        stalest_urn = dataset_urn
        stalest_ms = victim_last_modified_ms
        path: List[str] = []
        visited = {dataset_urn}
        current = dataset_urn

        for _ in range(max_depth):
            lineage = await self.mcp.get_dataset_lineage(current)
            upstreams = lineage.upstreams
            if not upstreams:
                break
            upstream_props = await asyncio.gather(
                *[self.mcp.get_dataset_properties(edge.urn) for edge in upstreams]
            )
            candidates = [
                (edge.urn, props.get("last_modified"))
                for edge, props in zip(upstreams, upstream_props)
                if edge.urn not in visited and props.get("last_modified") is not None
            ]
            if not candidates:
                break
            next_urn, next_ms = min(candidates, key=lambda pair: pair[1])
            visited.add(next_urn)
            # Only follow a branch that is strictly older; ties (e.g. all-epoch-0
            # datapack entities) mean the whole chain is equally stale.
            if stalest_ms is None or next_ms < stalest_ms:
                stalest_ms = next_ms
                stalest_urn = next_urn
                path.append(next_urn)
                current = next_urn
            else:
                break

        return {"urn": stalest_urn, "last_modified_ms": stalest_ms, "path": path}

    def _analyze_freshness(
        self,
        incident: Incident,
        properties: Dict[str, Any],
        upstream_lineage: Any,
        stalest_urn: str = "",
        stalest_ms: Optional[int] = None,
    ) -> str:
        """Trace freshness issue upstream to find the stale source."""
        victim_ms = properties.get("last_modified")
        victim_desc = _format_timestamp(victim_ms)
        if stalest_urn and stalest_urn != incident.victim_urn and stalest_ms is not None:
            return (
                f"Freshness violation traced upstream: {stalest_urn} is the stalest source "
                f"(last modified {_format_timestamp(stalest_ms)}); "
                f"{incident.victim_urn} was last modified {victim_desc}."
            )
        return (
            f"Freshness violation on {incident.victim_urn} (last modified {victim_desc}): "
            f"no upstream source is older, the whole chain is stale or lineage is missing "
            f"({len(upstream_lineage.upstreams)} upstream sources checked)."
        )

    # -- Helpers: ownership gap --------------------------------------------------

    async def _fetch_upstream_ownership(
        self, upstreams: List[Any]
    ) -> List[DatasetOwnership]:
        """Fetch ownership for all first-hop upstream datasets in parallel."""
        if not upstreams:
            return []
        return list(
            await asyncio.gather(
                *[self.mcp.get_dataset_ownership(edge.urn) for edge in upstreams]
            )
        )

    def _analyze_ownership_gap(
        self, incident: Incident, ownership: Any, upstream_ownership: List[Any]
    ) -> str:
        """Identify who should own this based on upstream owners."""
        upstream_types = sorted(
            {o.owner_type for up in upstream_ownership for o in up.owners}
        )
        upstream_emails = [
            email for up in upstream_ownership for email in _extract_owner_emails(up)
        ]
        if upstream_types:
            suggestion = (
                f"Recommend assigning similar owners"
                + (f" (e.g. {', '.join(upstream_emails[:2])})" if upstream_emails else "")
            )
            return (
                f"Dataset {incident.victim_urn} has no owners, but upstream datasets "
                f"are owned by ({', '.join(upstream_types)}). {suggestion}."
            )
        return (
            f"Dataset {incident.victim_urn} has no owners and no upstream owners to "
            f"reference — recommend assigning a business owner manually."
        )

    # -- Helpers: lineage gap -----------------------------------------------------

    def _analyze_lineage_gap(self, incident: Incident, lineage: Any) -> str:
        """Determine why lineage is missing and what should be connected."""
        name = _display_name_from_urn(incident.victim_urn)
        expected = any(name.lower().startswith(prefix) for prefix in _LINEAGE_EXPECTED_PREFIXES)
        if expected:
            return (
                f"Dataset '{name}' matches a staging/raw/marts naming convention but has "
                f"no registered lineage. Recommend LINEAGE_REPAIR: verify the source "
                f"system reports upstream lineage."
            )
        return (
            f"Dataset '{name}' has no upstream or downstream lineage. If it is a source "
            f"dataset, confirm no producers exist; otherwise register upstream lineage."
        )

    # -- Helpers: impact + confidence ----------------------------------------------

    def _build_impact_assessment(self, dataset_urn: str, downstream: Any) -> str:
        """Count downstream datasets/dashboards and summarize impact."""
        table_count = sum(1 for edge in downstream if edge.type == "DATASET")
        asset_count = sum(
            1
            for edge in downstream
            if edge.type in ("CHART", "DASHBOARD", "DATA_JOB", "DATA_FLOW")
        )
        if table_count and asset_count:
            return (
                f"Affects {table_count} downstream table(s) and {asset_count} "
                f"downstream dashboard/chart/job asset(s)"
            )
        if table_count:
            return f"Affects {table_count} downstream table(s)"
        if asset_count:
            return f"Affects {asset_count} downstream dashboard/chart/job asset(s)"
        return f"No downstream consumers registered for {dataset_urn}"

    def _compute_confidence(
        self, lineage_found: bool, schema_compared: bool, ownership_found: bool
    ) -> float:
        """Score confidence: 0.5 base, +0.2 lineage, +0.2 schema comparison, +0.1 ownership."""
        score = 0.5
        if lineage_found:
            score += 0.2
        if schema_compared:
            score += 0.2
        if ownership_found:
            score += 0.1
        # Round to keep JSON/UI output clean (0.5+0.2+0.2+0.1 != 1.0 in floats).
        return round(min(score, 1.0), 2)

    # -- Status updates -------------------------------------------------------------

    async def diagnose_and_update(self, investigation: InvestigationResult) -> Incident:
        """
        Create/update DiagnosisReport, transition incident status, publish event.
        """
        incident = self.store.get(investigation.incident_id)
        if incident is None:
            raise ValueError(f"Incident {investigation.incident_id} not found in store")

        # DETECTED -> DIAGNOSING (investigation in progress) -> ROOT_CAUSE_IDENTIFIED.
        state_machine = IncidentStateMachine(incident)
        if incident.status == IncidentStatus.DETECTED:
            state_machine.transition_to(IncidentStatus.DIAGNOSING)
        state_machine.transition_to(IncidentStatus.ROOT_CAUSE_IDENTIFIED)

        incident.diagnosis = self._build_diagnosis_report(investigation, incident)
        incident.agent_logs.append(
            AgentLogEntry(
                agent_name=self.agent_type.value,
                action="DIAGNOSED",
                input_summary=(
                    f"victim={incident.victim_urn} failure_type={incident.failure_type.value}"
                ),
                output_summary=investigation.root_cause_description,
                duration_ms=0,
            )
        )
        self.store.update(incident)

        self.bus.publish(
            AgentEvent(
                event_type=AgentEventType.DIAGNOSIS_COMPLETE,
                incident_id=incident.id,
                agent_name=self.agent_type.value,
                payload={
                    "summary": (
                        f"Root cause identified: {investigation.root_cause_description} "
                        f"(confidence: {investigation.confidence_score})"
                    ),
                    "root_cause_urn": investigation.root_cause_dataset_urn
                    or incident.victim_urn,
                    "recommended_fix_type": investigation.recommended_fix_type,
                    "confidence_score": investigation.confidence_score,
                },
            )
        )
        return incident

    def _build_diagnosis_report(
        self, investigation: InvestigationResult, incident: Incident
    ) -> DiagnosisReport:
        """Map an InvestigationResult onto the DiagnosisReport model."""
        emails = investigation.evidence.get("owner_emails", [])
        return DiagnosisReport(
            root_cause_urn=investigation.root_cause_dataset_urn or incident.victim_urn,
            root_cause_type=incident.failure_type,
            lineage_path=investigation.evidence.get("upstream_path", []),
            owner_email=emails[0] if emails else "",
            summary_text=investigation.root_cause_description,
            impact_assessment=investigation.impact_assessment,
            affected_datasets=investigation.affected_datasets,
            recommended_fix_type=investigation.recommended_fix_type,
            confidence_score=investigation.confidence_score,
            evidence=investigation.evidence,
        )

    async def run(self, limit: int = 10) -> List[Incident]:
        """
        Full pipeline: investigate_open_incidents -> diagnose_and_update for each.
        Returns updated incidents.
        """
        investigations = await self.investigate_open_incidents(limit)
        updated: List[Incident] = []
        for investigation in investigations:
            try:
                updated.append(await self.diagnose_and_update(investigation))
            except Exception as exc:
                # Log but don't fail the batch.
                logger.error(
                    "Failed to diagnose %s: %s", investigation.incident_id, exc
                )
        return updated
