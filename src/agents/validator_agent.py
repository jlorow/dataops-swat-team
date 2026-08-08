"""Validator Agent — SQL fix validation & safety gate.

The Validator Agent is the final guard in the DataOps SWAT pipeline. It picks
up incidents with a proposed FixReport (status ``FIX_PROPOSED``), re-validates
the generated SQL against *real* DataHub schema/lineage metadata and decides
whether the fix is safe to deploy:

- **Schema reference check** — every column the SQL references must exist in
  the target dataset's DataHub schema; columns being *added* (``ADD COLUMN``)
  are exempt because they are the fix itself.
- **Lineage impact check** — dropping a column that downstream consumers'
  schemas depend on is a breaking change; adding columns or touching metadata
  is not.
- **Deep syntax check** — balanced parentheses, trailing semicolons for
  multi-statement SQL, no empty statements, no reserved words used as column
  identifiers.

A safety score is computed (1.0 baseline; −0.3 schema, −0.4 lineage, −0.3
syntax) and mapped to a recommendation:

- ``>= 0.8`` → DEPLOY   (``VALIDATING -> READY_TO_DEPLOY``)
- ``>= 0.5`` → REVIEW   (``VALIDATING -> FIX_PROPOSED``, back to Engineer)
- ``< 0.5``  → ESCALATE (``VALIDATING -> ESCALATED``)

Usage:
    async with ValidatorAgent(DataHubMCPClient(gms_url=...)) as agent:
        updated = await agent.run(limit=10)
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlparse
from pydantic import BaseModel, Field

from src.agents.sentry_agent import _display_name_from_urn
from src.datahub.mcp_client import DataHubMCPClient, DatasetSchema
from src.models import (
    AgentEvent,
    AgentEventType,
    AgentLogEntry,
    AgentType,
    FixReport,
    Incident,
    IncidentStatus,
)
from src.orchestrator import EventBus, IncidentStateMachine, IncidentStore

logger = logging.getLogger(__name__)

# Words that are never column references when they appear bare in a WHERE
# clause (comparison literals, operators, SQL keywords).
_SQL_KEYWORDS = {
    "AND", "OR", "NOT", "NULL", "IS", "IN", "BETWEEN", "LIKE", "ILIKE",
    "TRUE", "FALSE", "ASC", "DESC", "DISTINCT", "ALL", "ANY", "SOME",
    "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "BY", "ON", "JOIN",
    "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "UNION", "EXCEPT",
    "INTERSECT", "EXISTS", "LIMIT", "OFFSET", "HAVING", "GROUP", "ORDER",
    "SELECT", "FROM", "WHERE", "UPDATE", "SET", "INSERT", "INTO", "VALUES",
    "DELETE", "ALTER", "ADD", "DROP", "CREATE", "TABLE", "COLUMN", "VIEW",
    "DATABASE", "SCHEMA", "TO", "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TIME",
}

# Reserved words that must not be used as column identifiers in DDL.
_RESERVED_IDENTIFIERS = _SQL_KEYWORDS | {
    "PRIMARY", "KEY", "FOREIGN", "INDEX", "DEFAULT", "CONSTRAINT",
    "REFERENCES", "MERGE", "TRUNCATE", "COMMENT", "GRANT", "REVOKE",
    "CALL", "EXEC", "TYPE", "WITH", "WITHIN", "RECURSIVE",
}

# Coarse type families used for ALTER COLUMN compatibility checks. A new type
# is compatible with the upstream type when they belong to the same family.
_TYPE_FAMILIES = {
    "NUMBER": {
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL",
        "NUMERIC", "FLOAT", "DOUBLE", "REAL", "NUMBER", "SERIAL", "BIGSERIAL",
    },
    "STRING": {
        "VARCHAR", "CHAR", "TEXT", "STRING", "NVARCHAR", "CHARACTER",
        "CLOB", "LONGVARCHAR", "VARCHAR2",
    },
    "TIMESTAMP": {
        "TIMESTAMP", "DATETIME", "DATE", "TIME", "TIMESTAMPTZ",
        "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
    },
    "BOOLEAN": {"BOOLEAN", "BOOL"},
    "BINARY": {"BINARY", "VARBINARY", "BLOB", "BYTEA"},
}

# Matches bare, quoted, backticked and bracket-quoted identifiers.
_IDENTIFIER_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_$]*|\"[^\"]+\"|`[^`]+`|\[[^\]]+\])")

# Matches quoted/bracketed/backticked string literals (for syntax stripping).
_STRING_LITERAL_RE = re.compile(r"""("[^"]*"|'[^']*'|`[^`]*`|\[[^\]]*\])""")


def _normalize_ident(token: str) -> str:
    """Lower-case and de-qualify an identifier: ``tbl.MyCol`` -> ``mycol``."""
    token = token.strip().strip('"`[]')
    if "." in token:
        token = token.rsplit(".", 1)[-1]
    return token.lower()


class ValidationResult(BaseModel):
    """Outcome of validating a single fix."""

    fix_id: str
    incident_id: str
    is_safe: bool
    safety_score: float = Field(ge=0.0, le=1.0)
    schema_check: Dict[str, Any]  # Did SQL reference non-existent columns?
    lineage_check: Dict[str, Any]  # Would this break downstream lineage?
    syntax_check: Dict[str, Any]  # sqlparse validation details
    breaking_changes: List[str]  # Human-readable list of concerns
    recommendation: str  # "DEPLOY", "ESCALATE", "REVIEW"
    validator_notes: str


class ValidatorAgent:
    """
    Async agent that validates SQL fixes before deployment.

    Usage:
        async with ValidatorAgent(mcp_client) as agent:
            validated = await agent.validate_pending_fixes()
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
        self.agent_type = AgentType.VALIDATOR

    async def __aenter__(self) -> "ValidatorAgent":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.mcp.__aexit__(exc_type, exc_val, exc_tb)

    # -- Public pipeline --------------------------------------------------------

    async def validate_pending_fixes(self, limit: int = 10) -> List[ValidationResult]:
        """
        Fetch incidents with status FIX_PROPOSED, validate each fix, return results.
        """
        candidates = [
            incident
            for incident in self.store.list_all()
            if incident.status == IncidentStatus.FIX_PROPOSED
        ][:limit]
        logger.info(
            "Validator: validating %d FIX_PROPOSED incidents", len(candidates)
        )
        results: List[ValidationResult] = []
        for incident in candidates:
            if incident.fix is None:
                logger.warning(
                    "Validator: incident %s has no FixReport, skipping", incident.id
                )
                continue
            try:
                results.append(await self.validate_fix(incident, incident.fix))
            except Exception as exc:
                # One failing incident must not abort the batch.
                logger.warning("Validator: failed to validate %s: %s", incident.id, exc)
        return results

    async def validate_fix(self, incident: Incident, fix_report: FixReport) -> ValidationResult:
        """
        Validate a single fix against DataHub metadata.

        Fetches the target schema, lineage, upstream schemas (for ALTER COLUMN
        type compatibility) and downstream schemas (for DROP COLUMN impact) from
        real DataHub, then runs the three checks and computes the safety score.
        """
        # FIX_PROPOSED -> VALIDATING (validation starts).
        if incident.status == IncidentStatus.FIX_PROPOSED:
            self._transition(incident, IncidentStatus.VALIDATING)
            self.store.update(incident)

        target_urn = fix_report.target_dataset_urn or incident.victim_urn
        schema, lineage = await asyncio.gather(
            self.mcp.get_dataset_schema(target_urn),
            self.mcp.get_dataset_lineage(target_urn),
        )
        upstream_schemas, downstream_pairs = await asyncio.gather(
            self._fetch_schemas(lineage.upstreams),
            self._fetch_downstream_pairs(lineage.downstreams),
        )

        schema_result = self._check_schema_references(
            fix_report.sql_code, schema, upstream_schemas
        )
        lineage_result = self._check_lineage_impact(
            fix_report.sql_code, lineage, downstream_pairs
        )
        syntax_result = self._check_syntax_deep(fix_report.sql_code)
        score = self._compute_safety_score(schema_result, lineage_result, syntax_result)
        recommendation = self._recommendation(score)

        breaking_changes: List[str] = []
        if not schema_result.get("passed"):
            missing = ", ".join(schema_result.get("missing_columns", []) or ["?"])
            breaking_changes.append(f"SQL references column(s) missing from schema: {missing}")
        if not lineage_result.get("passed"):
            affected = ", ".join(
                lineage_result.get("affected_downstream", []) or ["unknown downstream"]
            )
            breaking_changes.append(f"Breaking lineage impact on: {affected}")
        breaking_changes.extend(syntax_result.get("errors", []) or [])

        notes = (
            f"{len(schema.fields)} target fields, {len(lineage.upstreams)} upstream, "
            f"{len(lineage.downstreams)} downstream. "
            f"Schema: {schema_result.get('details', '')} | "
            f"Lineage: {lineage_result.get('details', '')} | "
            f"Syntax: {syntax_result.get('details', '')}"
        )

        return ValidationResult(
            fix_id=fix_report.fix_id,
            incident_id=incident.id,
            is_safe=recommendation == "DEPLOY",
            safety_score=score,
            schema_check=schema_result,
            lineage_check=lineage_result,
            syntax_check=syntax_result,
            breaking_changes=breaking_changes,
            recommendation=recommendation,
            validator_notes=notes,
        )

    # -- Schema reference check ---------------------------------------------------

    def _check_schema_references(
        self,
        sql: str,
        target_schema: Any,
        upstream_schemas: Optional[List[DatasetSchema]] = None,
    ) -> Dict[str, Any]:
        """
        Parse SQL and verify all referenced columns exist in the target schema.
        Return: {"passed": bool, "missing_columns": [...], "details": "..."}

        Columns introduced by ``ADD COLUMN`` are the fix itself and are exempt.
        Columns altered via ``ALTER COLUMN`` must exist, and their new type must
        be compatible with the upstream type of the same column (when known).
        """
        added, altered, dropped = self._parse_ddl(sql)
        referenced = self._extract_referenced_columns(sql)
        field_names = {_normalize_ident(f.field_path) for f in target_schema.fields}

        missing: Set[str] = set()
        for col in referenced - added:
            if col not in field_names:
                missing.add(col)
        for col in altered:
            if col not in field_names:
                missing.add(col)
        for col in dropped:
            if col not in field_names:
                missing.add(col)

        details = (
            f"checked {len(referenced)} referenced, {len(added)} added, "
            f"{len(altered)} altered, {len(dropped)} dropped column(s) "
            f"against {len(field_names)} schema field(s)"
        )

        # Type compatibility for ALTER COLUMN vs upstream types (concern, not failure).
        upstream_types: Dict[str, str] = {}
        for upstream in upstream_schemas or []:
            for field in upstream.fields:
                if field.native_type:
                    upstream_types.setdefault(_normalize_ident(field.field_path), field.native_type)
        type_concerns = []
        for col, new_type in altered.items():
            upstream_type = upstream_types.get(col)
            if new_type and upstream_type and not self._types_compatible(new_type, upstream_type):
                type_concerns.append(f"{col}: {new_type} incompatible with upstream {upstream_type}")
        if type_concerns:
            details += f" | type concerns: {'; '.join(type_concerns)}"

        return {
            "passed": not missing,
            "missing_columns": sorted(missing),
            "type_concerns": type_concerns,
            "details": details,
        }

    # -- Lineage impact check -----------------------------------------------------

    def _check_lineage_impact(
        self,
        sql: str,
        downstream_lineage: Any,
        downstream_schemas: Optional[List[Any]] = None,
    ) -> Dict[str, Any]:
        """
        Check if the SQL change would break downstream consumers.
        e.g., dropping a column that downstream datasets depend on.
        Return: {"passed": bool, "affected_downstream": [...], "details": "..."}

        Dropping a column (or the dataset itself) is breaking when any
        downstream dataset's schema still references that column. Adding columns
        or updating metadata is non-breaking.

        ``downstream_schemas`` is a list of ``(edge, schema)`` pairs (as
        fetched by :meth:`_fetch_downstream_pairs`) or bare schemas.
        """
        added, altered, dropped = self._parse_ddl(sql)
        affected: List[str] = []
        downstream_schemas = downstream_schemas or []

        if dropped:
            for index, entry in enumerate(downstream_schemas):
                if isinstance(entry, tuple) and len(entry) == 2:
                    edge, downstream_schema = entry
                else:
                    downstream_schema = entry
                    edge = None
                    if index < len(downstream_lineage.downstreams):
                        edge = downstream_lineage.downstreams[index]
                downstream_fields = {
                    _normalize_ident(f.field_path) for f in downstream_schema.fields
                }
                hit = sorted(dropped & downstream_fields)
                if hit:
                    label = edge.name or (
                        _display_name_from_urn(edge.urn) if edge else "unknown"
                    )
                    affected.append(f"{label} (uses {', '.join(hit)})")

        # Defensive: dropping the dataset itself breaks everything downstream.
        if re.search(r"\bdrop\s+(table|view)\b", sql, re.IGNORECASE):
            affected.append("DROP TABLE/VIEW removes the dataset itself")

        checked = len(downstream_schemas)
        details = (
            f"{len(dropped)} dropped column(s), checked {checked} downstream "
            f"dataset schema(s)"
        )
        if dropped and not downstream_schemas:
            details += " (no downstream schemas available to verify impact)"
        if not dropped:
            details += "; adding/altering columns is non-breaking"

        return {
            "passed": not affected,
            "affected_downstream": affected,
            "details": details,
        }

    # -- Deep syntax check ----------------------------------------------------------

    def _check_syntax_deep(self, sql: str) -> Dict[str, Any]:
        """
        Deep syntax validation beyond what Engineer did.
        Return: {"passed": bool, "errors": [...], "details": "..."}

        Re-parses with sqlparse and checks for unbalanced parentheses, missing
        semicolons in multi-statement SQL, empty statements, and reserved
        keywords used as column identifiers.
        """
        errors: List[str] = []
        if not sql or not sql.strip():
            return {
                "passed": False,
                "errors": ["SQL is empty"],
                "details": "no SQL text to validate",
            }

        try:
            statements = sqlparse.parse(sql)
        except Exception as exc:  # pragma: no cover - sqlparse rarely raises
            return {
                "passed": False,
                "errors": [f"sqlparse failed: {exc}"],
                "details": "parser raised while parsing SQL",
            }

        real = [
            stmt for stmt in statements if stmt.token_first(skip_cm=True) is not None
        ]
        if not real:
            errors.append("SQL contains no executable statements")

        if len(real) > 1:
            for stmt in real:
                if not stmt.value.rstrip().endswith(";"):
                    errors.append(
                        "multi-statement SQL: every statement must end with a semicolon"
                    )
                    break

        cleaned = _STRING_LITERAL_RE.sub(" ", sql)
        cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r"--[^\n]*", " ", cleaned)
        if cleaned.count("(") != cleaned.count(")"):
            errors.append(
                f"unbalanced parentheses: {cleaned.count('(')} open vs "
                f"{cleaned.count(')')} close"
            )

        added, altered, dropped = self._parse_ddl(sql)
        for col in added | set(altered) | dropped:
            if col.upper() in _RESERVED_IDENTIFIERS:
                errors.append(f"reserved keyword used as column identifier: {col}")

        return {
            "passed": not errors,
            "errors": errors,
            "details": f"{len(statements)} statement(s), {len(real)} executable",
        }

    # -- Safety score & recommendation ----------------------------------------------

    def _compute_safety_score(
        self, schema_result: Dict, lineage_result: Dict, syntax_result: Dict
    ) -> float:
        """
        Compute 0.0-1.0 safety score.
        Start at 1.0, subtract for each failure.
        """
        score = 1.0
        if not schema_result.get("passed", True):
            score -= 0.3
        if not lineage_result.get("passed", True):
            score -= 0.4
        if not syntax_result.get("passed", True):
            score -= 0.3
        return round(max(score, 0.0), 2)

    @staticmethod
    def _recommendation(score: float) -> str:
        """Map a safety score to DEPLOY / REVIEW / ESCALATE."""
        if score >= 0.8:
            return "DEPLOY"
        if score >= 0.5:
            return "REVIEW"
        return "ESCALATE"

    # -- Decision & persistence ------------------------------------------------------

    async def apply_decision(self, validation: ValidationResult, incident: Incident) -> Incident:
        """
        Transition incident based on validation result.
        FIX_PROPOSED -> VALIDATING -> READY_TO_DEPLOY (if safe) or ESCALATED (if unsafe)
        """
        target = {
            "DEPLOY": IncidentStatus.READY_TO_DEPLOY,
            "REVIEW": IncidentStatus.FIX_PROPOSED,
            "ESCALATE": IncidentStatus.ESCALATED,
        }[validation.recommendation]

        # VALIDATING -> terminal (or back to Engineer on REVIEW).
        if not self._transition(incident, target):
            raise ValueError(
                f"Incident {incident.id} cannot transition from "
                f"{incident.status.value} to {target.value}"
            )

        incident.agent_logs.append(
            AgentLogEntry(
                agent_name=self.agent_type.value,
                action="VALIDATED",
                input_summary=f"fix={validation.fix_id} recommendation={validation.recommendation}",
                output_summary=validation.validator_notes[:500],
                duration_ms=0,
            )
        )
        self.store.update(incident)

        message = (
            f"Fix {validation.fix_id} validated: "
            f"score={validation.safety_score}, recommendation={validation.recommendation}"
        )
        self.bus.publish(
            AgentEvent(
                event_type=AgentEventType.VALIDATED,
                incident_id=incident.id,
                agent_name=self.agent_type.value,
                payload={
                    "message": message,
                    "fix_id": validation.fix_id,
                    "safety_score": validation.safety_score,
                    "is_safe": validation.is_safe,
                    "recommendation": validation.recommendation,
                    "schema_check": validation.schema_check,
                    "lineage_check": validation.lineage_check,
                    "syntax_check": validation.syntax_check,
                    "breaking_changes": validation.breaking_changes,
                },
            )
        )
        return incident

    async def run(self, limit: int = 10) -> List[Incident]:
        """Full pipeline: validate_pending_fixes -> apply_decision. Returns updated incidents."""
        validations = await self.validate_pending_fixes(limit)
        updated: List[Incident] = []
        for validation in validations:
            incident = self.store.get(validation.incident_id)
            if incident is None:
                logger.warning(
                    "Validator: incident %s vanished, skipping", validation.incident_id
                )
                continue
            try:
                updated.append(await self.apply_decision(validation, incident))
            except Exception as exc:
                logger.error(
                    "Validator: failed to apply decision for %s: %s",
                    validation.incident_id,
                    exc,
                )
        return updated

    # -- SQL parsing helpers -----------------------------------------------------------

    def _parse_ddl(self, sql: str) -> Tuple[Set[str], Dict[str, str], Set[str]]:
        """Extract ADD / ALTER / DROP COLUMN operations from the SQL text.

        Returns ``(added, altered, dropped)`` where ``altered`` maps each
        altered column name to the new native type (upper-cased) when it can
        be determined.
        """
        added: Set[str] = set()
        dropped: Set[str] = set()
        altered: Dict[str, str] = {}

        for match in re.finditer(
            r"\badd\s+column\s+(?:if\s+not\s+exists\s+)?([\w.`\"\[\]]+)",
            sql,
            re.IGNORECASE,
        ):
            added.add(_normalize_ident(match.group(1)))

        for match in re.finditer(
            r"\bdrop\s+column\s+(?:if\s+exists\s+)?([\w.`\"\[\]]+)",
            sql,
            re.IGNORECASE,
        ):
            dropped.add(_normalize_ident(match.group(1)))

        for match in re.finditer(
            r"\balter\s+column\s+(?:if\s+exists\s+)?([\w.`\"\[\]]+)",
            sql,
            re.IGNORECASE,
        ):
            column = _normalize_ident(match.group(1))
            rest = sql[match.end():]
            rest = re.split(r"[,;]", rest, maxsplit=1)[0]
            type_match = re.search(r"\btype\s+([A-Za-z_][\w]*)", rest, re.IGNORECASE)
            new_type = type_match.group(1) if type_match else ""
            if not new_type:
                bare = re.search(r"([A-Za-z_][\w]*)", rest.strip(), re.IGNORECASE)
                new_type = bare.group(1) if bare else ""
            altered[column] = new_type.upper()

        return added, altered, dropped

    def _extract_referenced_columns(self, sql: str) -> Set[str]:
        """Extract column names referenced by DML (SELECT/INSERT/UPDATE/DELETE)."""
        columns: Set[str] = set()
        for statement in sqlparse.parse(sql):
            stmt_type = (statement.get_type() or "").upper()
            text = statement.value
            if stmt_type == "SELECT":
                match = re.search(
                    r"\bselect\b(.*?)\bfrom\b", text, re.IGNORECASE | re.DOTALL
                )
                if match:
                    for part in match.group(1).split(","):
                        columns |= self._expr_columns(part)
                columns |= self._where_columns(text)
            elif stmt_type == "UPDATE":
                match = re.search(
                    r"\bset\b(.*?)(?:\bwhere\b|;|$)", text, re.IGNORECASE | re.DOTALL
                )
                if match:
                    for part in match.group(1).split(","):
                        left = part.split("=", 1)[0]
                        columns |= self._expr_columns(left)
                columns |= self._where_columns(text)
            elif stmt_type == "INSERT":
                match = re.search(
                    r"\binto\b\s+[\w.`\"\[\]]+\s*\(([^)]*)\)", text, re.IGNORECASE
                )
                if match:
                    for part in match.group(1).split(","):
                        columns |= self._expr_columns(part)
            elif stmt_type == "DELETE":
                columns |= self._where_columns(text)
        return columns

    def _where_columns(self, text: str) -> Set[str]:
        """Extract column-ish identifiers from a WHERE clause.

        The clause is cut off at subqueries and trailing ORDER BY / GROUP BY /
        LIMIT / HAVING so table aliases and literals inside them are not
        mistaken for column references.
        """
        columns: Set[str] = set()
        match = re.search(r"\bwhere\b(.*)$", text, re.IGNORECASE | re.DOTALL)
        if not match:
            return columns
        clause = match.group(1)
        clause = re.split(
            r"\b(?:select|order\s+by|group\s+by|limit|offset|having)\b",
            clause,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        for token in _IDENTIFIER_RE.findall(clause):
            name = _normalize_ident(token)
            if not name or name.upper() in _SQL_KEYWORDS:
                continue
            if re.fullmatch(r"[\d.]+", name):
                continue
            columns.add(name)
        return columns

    @staticmethod
    def _expr_columns(expr: str) -> Set[str]:
        """Extract the source columns referenced by a SELECT/SET expression.

        Handles ``o.total AS amount``, ``SUM(o.total) AS s``, ``COUNT(*)``
        (skipped), arithmetic like ``a + b``, and qualified ``schema.tbl.col``.
        """
        expr = expr.strip()
        if not expr or expr == "*":
            return set()

        # Aggregate / function call: use the innermost identifier(s).
        func = re.search(
            r"\(\s*(?:distinct\s+)?([A-Za-z_\"`\[][^)]*)\)\s*(?:as\s+[\w]+)?$",
            expr,
            re.IGNORECASE,
        )
        if func:
            inner = func.group(1).strip()
            if inner == "*":
                return set()
            return {
                _normalize_ident(t)
                for t in _IDENTIFIER_RE.findall(inner)
                if _normalize_ident(t).upper() not in _SQL_KEYWORDS
            }

        # Strip an explicit alias, and a bare trailing alias only when the
        # expression has no operators (so ``a + b`` keeps both columns).
        expr = re.sub(r"\s+as\s+[A-Za-z_][\w]*\s*$", "", expr, flags=re.IGNORECASE).strip()
        if not re.search(r"[+\-*/=<>]", expr):
            expr = re.sub(r"\s+[A-Za-z_][\w]*\s*$", "", expr).strip()

        return {
            _normalize_ident(t)
            for t in _IDENTIFIER_RE.findall(expr)
            if _normalize_ident(t).upper() not in _SQL_KEYWORDS
        }

    @staticmethod
    def _types_compatible(new_type: str, upstream_type: str) -> bool:
        """True when a new column type is compatible with the upstream type."""
        new_type = new_type.upper().strip()
        upstream_type = upstream_type.upper().strip()
        if new_type == upstream_type:
            return True
        for family in _TYPE_FAMILIES.values():
            if new_type in family and upstream_type in family:
                return True
        return False

    # -- Status transitions -----------------------------------------------------------

    def _transition(self, incident: Incident, target: IncidentStatus) -> bool:
        """Apply a state-machine transition; log and return False when invalid."""
        machine = IncidentStateMachine(incident)
        if not machine.can_transition_to(target):
            logger.warning(
                "Validator: invalid transition %s -> %s for incident %s",
                incident.status.value,
                target.value,
                incident.id,
            )
            return False
        machine.transition_to(target)
        return True

    async def _fetch_downstream_pairs(self, edges: List[Any]) -> List[Tuple[Any, DatasetSchema]]:
        """Fetch downstream schemas as ``(edge, schema)`` pairs so lineage labels
        stay aligned with their schemas even when some fetches fail."""
        if not edges:
            return []
        results = await asyncio.gather(
            *[self.mcp.get_dataset_schema(edge.urn) for edge in edges],
            return_exceptions=True,
        )
        pairs: List[Tuple[Any, DatasetSchema]] = []
        for edge, result in zip(edges, results):
            if isinstance(result, Exception):
                logger.warning(
                    "Validator: could not fetch schema for %s: %s", edge.urn, result
                )
                continue
            pairs.append((edge, result))
        return pairs

    async def _fetch_schemas(self, edges: List[Any]) -> List[DatasetSchema]:
        """Fetch schemas for lineage edges in parallel, tolerating per-dataset failures."""
        return [schema for _, schema in await self._fetch_downstream_pairs(edges)]
