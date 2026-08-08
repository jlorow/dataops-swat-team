"""Incident status transition state machine."""
import logging
from datetime import datetime
from typing import Dict, List

from src.models import Incident, IncidentStatus

logger = logging.getLogger(__name__)


class IncidentStateMachine:
    """Validates and applies Incident status transitions."""

    TRANSITIONS: Dict[IncidentStatus, List[IncidentStatus]] = {
        IncidentStatus.DETECTED: [IncidentStatus.DIAGNOSING],
        IncidentStatus.DIAGNOSING: [
            IncidentStatus.ROOT_CAUSE_IDENTIFIED,
            IncidentStatus.FIXING,
            IncidentStatus.ESCALATED,
        ],
        IncidentStatus.ROOT_CAUSE_IDENTIFIED: [
            IncidentStatus.FIXING,
            IncidentStatus.ESCALATED,
        ],
        IncidentStatus.FIXING: [IncidentStatus.FIX_PROPOSED, IncidentStatus.ESCALATED],
        IncidentStatus.FIX_PROPOSED: [IncidentStatus.VALIDATING, IncidentStatus.ESCALATED],
        IncidentStatus.VALIDATING: [
            IncidentStatus.READY_TO_DEPLOY,
            IncidentStatus.FIX_PROPOSED,
            IncidentStatus.RESOLVED,
            IncidentStatus.ESCALATED,
        ],
        IncidentStatus.READY_TO_DEPLOY: [],
        IncidentStatus.RESOLVED: [],
        IncidentStatus.ESCALATED: [],
    }

    def __init__(self, incident: Incident) -> None:
        self.incident = incident

    def can_transition_to(self, new_status: IncidentStatus) -> bool:
        """Return True if new_status is reachable from the current status."""
        return new_status in self.TRANSITIONS[self.incident.status]

    def transition_to(self, new_status: IncidentStatus) -> None:
        """Transition the incident to new_status, raising ValueError if invalid."""
        if not self.can_transition_to(new_status):
            raise ValueError(
                f"Invalid transition from {self.incident.status} to {new_status}"
            )
        self.incident.status = new_status
        if new_status is IncidentStatus.RESOLVED:
            self.incident.resolved_at = datetime.utcnow()
        logger.info(
            "Incident %s transitioned to %s", self.incident.id, self.incident.status
        )
