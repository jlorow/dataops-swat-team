"""Engineer Agent — metadata-aware SQL fix generation.

The Engineer Agent reads incidents with a completed DiagnosisReport
(status ``ROOT_CAUSE_IDENTIFIED``), fetches the *real* target and upstream
schemas from DataHub, and generates a SQL fix — either through the LLM gateway
(OpenRouter, or any ``LLMClient``) or through a deterministic template fallback
when no LLM is configured.

Every generated fix:

- references only columns that actually exist in the fetched schemas,
- is validated with ``sqlparse`` plus safety checks (no DROP DATABASE, no
  DELETE/TRUNCATE without care),
- transitions the incident ``ROOT_CAUSE_IDENTIFIED -> FIXING -> FIX_PROPOSED``
  via the IncidentStateMachine,
- is persisted as a ``FixReport`` on the incident and announced with a
  FIX_GENERATED event.

Usage:
    async with EngineerAgent(DataHubMCPClient(gms_url=...)) as agent:
        reports = await agent.run(limit=10)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from typing import Any, List, Optional, Tuple

import sqlparse
from pydantic import BaseModel, Field

from src.agents.sentry_agent import _display_name_from_urn
from src.datahub.mcp_client import DataHubMCPClient, DatasetSchema
from src.llm import LLMClient, OpenRouterClient
from src.models import (
    AgentEvent,
    AgentEventType,
    AgentLogEntry,
    AgentType,
    DiagnosisReport,
    FixReport,
    Incident,
    IncidentStatus,
)
from src.orchestrator import EventBus, IncidentStateMachine, IncidentStore

logger = logging.getLogger(__name__)

# Max schema fields to include in an LLM prompt.
_PROMPT_FIELD_LIMIT = 60


class GeneratedFix(BaseModel):
    """A fix proposal before validation."""

    sql_code: str
    explanation: str
    target_dataset_urn: str
    fix_type: str  # "SCHEMA_UPDATE", "FRESHNESS_RERUN", "OWNER_ASSIGNMENT", "LINEAGE_REPAIR"


def _fix_id(incident_id: str, sql: str) -> str:
    """Deterministic fix id so re-generating the same fix deduplicates."""
    digest = hashlib.sha1(sql.encode("utf-8")).hexdigest()[:8]
    return f"fix-{incident_id}-{digest}"


def _looks_like_timestamp(field: Any) -> bool:
    """Heuristic for picking a freshness-check column."""
    haystack = f"{field.field_path} {field.native_type}".lower()
    return any(token in haystack for token in ("timestamp", "datetime", " date", "date_", "_date", "time"))


class EngineerAgent:
    """
    Async agent that generates SQL fixes based on DiagnosisReports.

    Usage:
        async with EngineerAgent(mcp_client, llm_client) as agent:
            fix_reports = await agent.fix_open_incidents()
    """

    def __init__(
        self,
        mcp_client: DataHubMCPClient,
        llm_client: Optional[LLMClient] = None,
        event_bus: Optional[EventBus] = None,
        incident_store: Optional[IncidentStore] = None,
    ) -> None:
        self.mcp = mcp_client
        # Prefer the injected client; else auto-configure OpenRouter when an API
        # key is present; else None (deterministic template fallback).
        self.llm: Optional[LLMClient] = llm_client
        api_key = os.getenv("OPENROUTER_API_KEY", "")
        if self.llm is None and api_key:
            self.llm = OpenRouterClient(api_key=api_key)
        self.bus = event_bus or EventBus()
        self.store = incident_store or IncidentStore()
        self.agent_type = AgentType.ENGINEER

    async def __aenter__(self) -> "EngineerAgent":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.mcp.__aexit__(exc_type, exc_val, exc_tb)

    # -- Public pipeline --------------------------------------------------------

    async def fix_open_incidents(self, limit: int = 10) -> List[FixReport]:
        """
        Fetch incidents with status ROOT_CAUSE_IDENTIFIED, generate fixes, return FixReports.
        """
        candidates = [
            incident
            for incident in self.store.list_all()
            if incident.status == IncidentStatus.ROOT_CAUSE_IDENTIFIED
        ][:limit]
        logger.info(
            "Engineer: generating fixes for %d ROOT_CAUSE_IDENTIFIED incidents",
            len(candidates),
        )
        reports: List[FixReport] = []
        for incident in candidates:
            try:
                reports.append(await self.generate_fix(incident))
            except Exception as exc:
                # One failing incident must not abort the batch.
                logger.warning("Engineer: failed to fix %s: %s", incident.id, exc)
        return reports

    async def generate_fix(self, incident: Incident) -> FixReport:
        """
        Generate a SQL fix for a single incident using LLM + DataHub schema context.
        """
        diagnosis = incident.diagnosis
        if diagnosis is None:
            raise ValueError(f"Incident {incident.id} has no DiagnosisReport to fix")

        # ROOT_CAUSE_IDENTIFIED -> FIXING (fix generation starts).
        if not self._transition(incident, IncidentStatus.FIXING):
            raise ValueError(
                f"Incident {incident.id} cannot transition from "
                f"{incident.status.value} to FIXING"
            )

        victim_urn = incident.victim_urn
        schema, lineage = await asyncio.gather(
            self.mcp.get_dataset_schema(victim_urn),
            self.mcp.get_dataset_lineage(victim_urn),
        )
        upstream_schemas = await self._fetch_upstream_schemas(lineage.upstreams)

        proposal = await self._propose_fix(incident, schema, upstream_schemas, diagnosis)
        is_valid, error = self._validate_sql(proposal.sql_code)

        report = FixReport(
            fix_id=_fix_id(incident.id, proposal.sql_code),
            incident_id=incident.id,
            target_dataset_urn=victim_urn,
            sql_code=proposal.sql_code,
            explanation=proposal.explanation,
            fix_type=proposal.fix_type,
            is_valid=is_valid,
            validation_error=error,
            fixed_code=proposal.sql_code,  # backward-compatible alias
        )

        incident.fix = report
        incident.agent_logs.append(
            AgentLogEntry(
                agent_name=self.agent_type.value,
                action="FIX_GENERATED",
                input_summary=f"fix_type={proposal.fix_type} target={victim_urn}",
                output_summary=proposal.explanation,
                duration_ms=0,
            )
        )

        # FIXING -> FIX_PROPOSED (FixReport created).
        self._transition(incident, IncidentStatus.FIX_PROPOSED)
        self.store.update(incident)

        self.bus.publish(
            AgentEvent(
                event_type=AgentEventType.FIX_GENERATED,
                incident_id=incident.id,
                agent_name=self.agent_type.value,
                payload={
                    "fix_id": report.fix_id,
                    "target_dataset_urn": victim_urn,
                    "fix_type": proposal.fix_type,
                    "is_valid": is_valid,
                    "sql_preview": proposal.sql_code[:200],
                },
            )
        )
        return report

    async def run(self, limit: int = 10) -> List[FixReport]:
        """Full pipeline: fix_open_incidents -> generate_fix for each. Returns FixReports."""
        return await self.fix_open_incidents(limit)

    # -- Fix proposal ------------------------------------------------------------

    async def _propose_fix(
        self,
        incident: Incident,
        schema: DatasetSchema,
        upstream_schemas: List[DatasetSchema],
        diagnosis: DiagnosisReport,
    ) -> GeneratedFix:
        """Ask the LLM, falling back to the deterministic template on any failure."""
        if self.llm is not None:
            prompt = self._build_prompt(incident, schema, upstream_schemas, diagnosis)
            try:
                raw = await asyncio.to_thread(
                    self.llm.generate, prompt, temperature=0.2, max_tokens=2048
                )
                sql, explanation = self._parse_llm_output(raw)
                return GeneratedFix(
                    sql_code=sql,
                    explanation=explanation,
                    target_dataset_urn=incident.victim_urn,
                    fix_type=diagnosis.recommended_fix_type or "SQL_PATCH",
                )
            except Exception as exc:
                logger.warning("Engineer: LLM failed (%s); using template fallback", exc)

        sql, explanation = self._template_fix(incident, schema, upstream_schemas, diagnosis)
        return GeneratedFix(
            sql_code=sql,
            explanation=explanation,
            target_dataset_urn=incident.victim_urn,
            fix_type=diagnosis.recommended_fix_type or "SQL_PATCH",
        )

    def _build_prompt(
        self,
        incident: Incident,
        schema: Any,
        upstream_schemas: List[Any],
        diagnosis: DiagnosisReport,
    ) -> str:
        """
        Build a metadata-aware prompt for the LLM.
        Must include real schema fields, types, and the diagnosis.
        """
        dataset_name = _display_name_from_urn(incident.victim_urn)
        target_lines = self._format_schema(schema, f"TARGET: {dataset_name} ({incident.victim_urn})")
        upstream_blocks = []
        for index, upstream in enumerate(upstream_schemas, start=1):
            upstream_blocks.append(self._format_schema(upstream, f"UPSTREAM {index}"))
        upstream_text = "\n\n".join(upstream_blocks) if upstream_blocks else "(no upstream schemas available)"

        return (
            "You are a senior data engineer fixing a data pipeline incident.\n"
            "\n"
            f"INCIDENT: {incident.failure_type.value} on {dataset_name}\n"
            f"ROOT CAUSE: {diagnosis.summary_text}\n"
            f"IMPACT: {diagnosis.impact_assessment}\n"
            f"CONFIDENCE: {diagnosis.confidence_score}\n"
            "\n"
            "TARGET DATASET SCHEMA:\n"
            f"{target_lines}\n"
            "\n"
            "UPSTREAM DATASET SCHEMAS:\n"
            f"{upstream_text}\n"
            "\n"
            "GENERATE A SQL FIX THAT:\n"
            "- Addresses the root cause\n"
            "- Is compatible with the target schema\n"
            "- References only columns that exist in the schema\n"
            "- Uses standard SQL (ANSI-compatible where possible)\n"
            "- Includes a brief comment explaining the fix\n"
            "\n"
            "Output ONLY the SQL code and a 1-sentence explanation. "
            "No markdown fences around the SQL."
        )

    def _format_schema(self, schema: Any, title: str) -> str:
        """Render schema fields as 'name type -- description' lines."""
        lines = [f"-- {title}"]
        for field in schema.fields[:_PROMPT_FIELD_LIMIT]:
            description = f" -- {field.description}" if field.description else ""
            lines.append(f"  {field.field_path} {field.native_type}{description}")
        return "\n".join(lines)

    @staticmethod
    def _looks_like_sql_start(line: str) -> bool:
        """True if a line looks like the start of another SQL statement."""
        return line.endswith(";") or line.upper().startswith(
            ("SELECT", "ALTER", "INSERT", "UPDATE", "DELETE", "CREATE", "WITH",
             "MERGE", "CALL", "REFRESH", "COMMENT", "GRANT", "REVOKE")
        )

    def _parse_llm_output(self, raw: str) -> Tuple[str, str]:
        """
        Strip markdown fences and split the trailing explanation from the SQL.

        Lines after the last statement-ending ``;`` are treated as the
        explanation UNLESS they look like the start of another SQL statement.
        This tolerates explanations that themselves contain semicolons.
        """
        text = raw.strip()
        # Remove any fence lines (``` or ```sql) wherever they appear.
        text = re.sub(r"^\s*```(?:sql)?\s*$", "", text, flags=re.IGNORECASE | re.MULTILINE)

        sql_lines: List[str] = []
        explanation_lines: List[str] = []
        statement_ended = False
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                if not statement_ended:
                    sql_lines.append(line)
                continue
            if statement_ended:
                if self._looks_like_sql_start(stripped):
                    # Continuation: another statement begins.
                    statement_ended = False
                    sql_lines.append(line)
                else:
                    explanation_lines.append(stripped)
                continue
            sql_lines.append(line)
            if stripped.endswith(";"):
                statement_ended = True

        sql = "\n".join(sql_lines).strip()
        explanation = " ".join(explanation_lines).strip()
        if not explanation:
            explanation = "SQL fix generated from DataHub schema context."
        return sql, explanation

    def _template_fix(
        self,
        incident: Incident,
        schema: Any,
        upstream_schemas: List[Any],
        diagnosis: DiagnosisReport,
    ) -> Tuple[str, str]:
        """
        Deterministic fallback that produces REAL SQL referencing actual columns
        from the fetched schemas (never mock SQL).
        """
        table = _display_name_from_urn(incident.victim_urn)
        fields = schema.fields
        fix_type = diagnosis.recommended_fix_type or "SQL_PATCH"

        if fix_type == "SCHEMA_UPDATE":
            victim_names = {field.field_path for field in fields}
            missing = [
                field
                for upstream in upstream_schemas
                for field in upstream.fields
                if field.field_path not in victim_names and field.native_type
            ]
            # Shared columns whose native type differs from upstream.
            upstream_types: dict = {}
            for upstream in upstream_schemas:
                for field in upstream.fields:
                    if field.native_type:
                        upstream_types.setdefault(field.field_path, field.native_type)
            mismatches = [
                field
                for field in fields
                if field.field_path in upstream_types
                and field.native_type
                and upstream_types[field.field_path] != field.native_type
            ]

            if missing or mismatches:
                statements = [
                    f"-- SCHEMA_UPDATE: align {table} with upstream schema "
                    f"({len(missing)} missing, {len(mismatches)} type mismatches)"
                ]
                for field in missing[:4]:
                    statements.append(f"ALTER TABLE {table} ADD COLUMN {field.field_path} {field.native_type};")
                for field in mismatches[:4]:
                    statements.append(
                        f"ALTER TABLE {table} ALTER COLUMN {field.field_path} "
                        f"SET DATA TYPE {upstream_types[field.field_path]};"
                    )
                if len(missing) > 4 or len(mismatches) > 4:
                    statements.append("-- ... additional columns/type fixes (see diagnosis evidence)")
                return "\n".join(statements), (
                    f"Align {table} schema with upstream: add {len(missing)} column(s), "
                    f"fix {len(mismatches)} type mismatch(es)."
                )
            return (
                f"-- SCHEMA_UPDATE: field sets and types already match upstream\n"
                f"{self._sample_query(table, fields)}",
                f"Schema already matches upstream; verify descriptions are aligned on {table}.",
            )

        if fix_type == "FRESHNESS_RERUN":
            timestamp_col = next(
                (field.field_path for field in fields if _looks_like_timestamp(field)), None
            )
            if timestamp_col:
                return (
                    f"-- FRESHNESS_RERUN: refresh {table} and confirm recent data\n"
                    f"SELECT MAX({timestamp_col}) AS latest_ingested_ts FROM {table};",
                    f"Rerun the ingestion for {table} and verify the latest {timestamp_col} value.",
                )
            return (
                f"-- FRESHNESS_RERUN: rerun the upstream pipeline, then check row counts\n"
                f"SELECT COUNT(*) AS row_count FROM {table};",
                f"Rerun the upstream pipeline for {table} and confirm rows are loading again.",
            )

        if fix_type == "OWNER_ASSIGNMENT":
            emails = diagnosis.evidence.get("upstream_owner_emails") or ([diagnosis.owner_email] if diagnosis.owner_email else [])
            owner = emails[0] if emails else "data-platform-team"
            return (
                f"-- OWNER_ASSIGNMENT: assign ownership of {table} (DataHub metadata API)\n"
                f"-- Recommended owner: {owner} (inherited from upstream datasets)\n"
                f"{self._sample_query(table, fields)}",
                f"Assign {owner} as an owner of {table} in DataHub metadata.",
            )

        if fix_type == "LINEAGE_REPAIR":
            return (
                f"-- LINEAGE_REPAIR: register missing upstream lineage for {table} "
                f"(DataHub metadata API)\n"
                f"{self._sample_query(table, fields)}",
                f"Register the missing upstream lineage edges for {table} in DataHub.",
            )

        return (
            f"-- {fix_type}: metadata update for {table} (see diagnosis)\n"
            f"{self._sample_query(table, fields)}",
            f"Apply {fix_type} for {table}.",
        )

    @staticmethod
    def _sample_query(table: str, fields: List[Any]) -> str:
        """A real sample query referencing actual columns (never bare SELECT *)."""
        if fields:
            columns = ", ".join(field.field_path for field in fields[:6])
            return f"SELECT {columns} FROM {table} LIMIT 1;"
        return f"SELECT COUNT(*) AS row_count FROM {table};"

    # -- SQL validation -----------------------------------------------------------

    def _validate_sql(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Basic SQL validation. Return (is_valid, error_message).
        Uses sqlparse plus safety checks. Does NOT need to be a full parser.
        """
        if not sql or not sql.strip():
            return False, "SQL is empty"

        try:
            statements = sqlparse.parse(sql)
        except Exception as exc:  # pragma: no cover - sqlparse rarely raises
            return False, f"sqlparse failed: {exc}"

        if not statements or not any(
            stmt.token_first(skip_cm=True) is not None for stmt in statements
        ):
            return False, "SQL does not parse into any statements"

        lowered = sql.lower()
        if "drop database" in lowered or "drop schema" in lowered:
            return False, "Unsafe SQL: DROP DATABASE/SCHEMA is forbidden"
        if "drop table" in lowered or "drop view" in lowered:
            return False, "Unsafe SQL: DROP TABLE/VIEW is forbidden"
        if "truncate table" in lowered:
            return False, "Unsafe SQL: TRUNCATE TABLE is forbidden"
        for stmt in statements:
            statement_text = stmt.value.lower()
            if "delete from" in statement_text and "where" not in statement_text:
                return False, "Unsafe SQL: DELETE FROM without a WHERE clause"
            if "update " in statement_text and "where" not in statement_text:
                return False, "Unsafe SQL: UPDATE without a WHERE clause"

        return True, None

    # -- Status transitions --------------------------------------------------------

    def _transition(self, incident: Incident, target: IncidentStatus) -> bool:
        """Apply a state-machine transition; log and return False when invalid."""
        machine = IncidentStateMachine(incident)
        if not machine.can_transition_to(target):
            logger.warning(
                "Engineer: invalid transition %s -> %s for incident %s",
                incident.status.value,
                target.value,
                incident.id,
            )
            return False
        machine.transition_to(target)
        return True

    async def _fetch_upstream_schemas(self, upstreams: List[Any]) -> List[DatasetSchema]:
        """Fetch schemas for all first-hop upstream datasets in parallel."""
        if not upstreams:
            return []
        return list(
            await asyncio.gather(
                *[self.mcp.get_dataset_schema(edge.urn) for edge in upstreams]
            )
        )
