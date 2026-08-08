"""Core data models for the DataOps SWAT Team.

Pydantic v2 models shared across the Event Bus, Orchestrator, and Agents.
"""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class IncidentStatus(str, Enum):
    DETECTED = "DETECTED"
    DIAGNOSING = "DIAGNOSING"
    ROOT_CAUSE_IDENTIFIED = "ROOT_CAUSE_IDENTIFIED"
    FIXING = "FIXING"
    VALIDATING = "VALIDATING"
    RESOLVED = "RESOLVED"
    ESCALATED = "ESCALATED"


class FailureType(str, Enum):
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    FRESHNESS_VIOLATION = "FRESHNESS_VIOLATION"
    BROKEN_JOB = "BROKEN_JOB"
    MANUAL_TEST = "MANUAL_TEST"
    OWNERSHIP_GAP = "OWNERSHIP_GAP"
    LINEAGE_GAP = "LINEAGE_GAP"


class IncidentType(str, Enum):
    """Anomaly categories detected by the Sentry Agent."""

    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    FRESHNESS_VIOLATION = "FRESHNESS_VIOLATION"
    OWNERSHIP_GAP = "OWNERSHIP_GAP"
    LINEAGE_GAP = "LINEAGE_GAP"


class AgentType(str, Enum):
    """The four SWAT team agents."""

    SENTRY = "SENTRY"
    DETECTIVE = "DETECTIVE"
    ENGINEER = "ENGINEER"
    VALIDATOR = "VALIDATOR"


class AgentEventType(str, Enum):
    INCIDENT_CREATED = "INCIDENT_CREATED"
    DIAGNOSIS_COMPLETE = "DIAGNOSIS_COMPLETE"
    FIX_GENERATED = "FIX_GENERATED"
    VALIDATED = "VALIDATED"
    ESCALATED = "ESCALATED"


class AgentLogEntry(BaseModel):
    agent_name: str
    action: str
    input_summary: str
    output_summary: str
    duration_ms: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DiagnosisReport(BaseModel):
    root_cause_urn: str
    root_cause_type: FailureType
    lineage_path: list[str] = Field(default_factory=list)
    owner_email: str
    summary_text: str
    # Fields populated by the Detective Agent's investigation.
    impact_assessment: str = Field(default="", description="e.g. 'Affects 3 downstream tables and 2 dashboards'")
    affected_datasets: list[str] = Field(default_factory=list)
    recommended_fix_type: str = Field(
        default="", description="e.g. SCHEMA_UPDATE, FRESHNESS_RERUN, OWNER_ASSIGNMENT, LINEAGE_REPAIR"
    )
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0)
    evidence: dict = Field(default_factory=dict)


class FixReport(BaseModel):
    fix_type: str  # e.g. "DBT_MODEL", "SQL_PATCH", "DAG_UPDATE"
    original_code: str
    fixed_code: str
    explanation: str


class AgentEvent(BaseModel):
    event_type: AgentEventType
    incident_id: str
    agent_name: str
    payload: dict = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class Incident(BaseModel):
    id: str
    status: IncidentStatus
    victim_urn: str
    failure_type: FailureType
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: datetime | None = None
    diagnosis: DiagnosisReport | None = None
    fix: FixReport | None = None
    pr_url: str | None = None
    agent_logs: list[AgentLogEntry] = Field(default_factory=list)
