"""In-memory publish/subscribe event bus for agent orchestration."""
import logging
from typing import Callable, Dict, List

from src.models import AgentEvent

logger = logging.getLogger(__name__)


class EventBus:
    """Simple in-memory pub/sub. No external dependencies."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable]] = {}

    def subscribe(self, event_type: str, handler: Callable) -> None:
        """Register a handler function for a given event type."""
        self._subscribers.setdefault(event_type, []).append(handler)

    def publish(self, event: AgentEvent) -> None:
        """Dispatch an event to all handlers registered for its event type."""
        logger.info("Publishing event type=%s incident_id=%s", event.event_type, event.incident_id)
        for handler in self.get_subscribers(event.event_type):
            handler(event)

    def get_subscribers(self, event_type: str) -> List[Callable]:
        """Return all handlers registered for an event type."""
        return self._subscribers.get(event_type, [])
