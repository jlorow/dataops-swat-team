"""In-memory incident store with append-only JSONL persistence."""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

from src.models import Incident

logger = logging.getLogger(__name__)


class IncidentStore:
    """Stores Incidents in memory, persisted as JSON Lines (one JSON object per line)."""

    def __init__(self, persist_path: str = "data/incidents.jsonl") -> None:
        self._persist_path = Path(persist_path)
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        self._incidents: Dict[str, Incident] = {}
        self._load()

    def _load(self) -> None:
        """Load existing incidents from the JSONL file into memory."""
        if not self._persist_path.exists():
            return
        with self._persist_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                incident = Incident(**json.loads(line))
                self._incidents[incident.id] = incident

    def create(self, incident: Incident) -> None:
        """Add an incident to memory and append it to the JSONL file."""
        self._incidents[incident.id] = incident
        self._append_to_file(incident)

    def get(self, incident_id: str) -> Optional[Incident]:
        """Return an incident by id, or None if not found."""
        return self._incidents.get(incident_id)

    def list_all(self) -> List[Incident]:
        """Return all incidents as a list."""
        return list(self._incidents.values())

    def update(self, incident: Incident) -> None:
        """Replace an incident in memory and rewrite the entire JSONL file."""
        self._incidents[incident.id] = incident
        self._rewrite_file()

    def _append_to_file(self, incident: Incident) -> None:
        """Serialize one incident to JSON and append it as a line."""
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        with self._persist_path.open("a", encoding="utf-8") as f:
            f.write(incident.model_dump_json() + "\n")

    def _rewrite_file(self) -> None:
        """Serialize all incidents and overwrite the JSONL file completely."""
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        with self._persist_path.open("w", encoding="utf-8") as f:
            for incident in self._incidents.values():
                f.write(incident.model_dump_json() + "\n")
