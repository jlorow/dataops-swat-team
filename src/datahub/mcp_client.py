"""MCP client wrapper for DataHub's GraphQL API.

This module gives the DataOps SWAT agents a typed, async interface to the
metadata stored in DataHub. Every query in this file was validated against a
live DataHub GMS instance (introspection + real requests), so the GraphQL
shapes match what DataHub actually serves:

- ``SearchInput`` takes a *singular* ``type`` field (``type: DATASET``), not
  the plural ``types`` introduced in newer releases.
- Lineage is exposed as a field on the ``Dataset`` entity
  (``dataset { lineage(input:) }``), not a root ``lineage`` query.
- The ``OwnerType`` union resolves to ``CorpUser | CorpGroup``, and the
  ownership type name (e.g. "Business Owner") lives at
  ``ownershipType.info.name``.
- The quickstart GMS uses HTTP Basic auth (``datahub``/``datahub``) unless a
  Personal Access Token is supplied.

Usage:
    client = DataHubMCPClient(gms_url="http://localhost:8080")
    datasets = await client.search_datasets("order", count=10)
    await client.aclose()
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional

import httpx
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration (overridable via env vars or constructor args)
# ---------------------------------------------------------------------------
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://localhost:8080")
DATAHUB_TOKEN = os.getenv("DATAHUB_TOKEN", "")  # optional Personal Access Token
DATAHUB_USER = os.getenv("DATAHUB_USER", "datahub")  # quickstart default creds
DATAHUB_PASSWORD = os.getenv("DATAHUB_PASSWORD", "datahub")


# ---------------------------------------------------------------------------
# Typed response models
# ---------------------------------------------------------------------------
class DatasetInfo(BaseModel):
    urn: str
    name: str
    platform: str


class SchemaField(BaseModel):
    field_path: str
    native_type: str
    description: Optional[str] = None


class DatasetSchema(BaseModel):
    fields: List[SchemaField] = Field(default_factory=list)


class LineageEdge(BaseModel):
    urn: str
    name: str = ""
    type: str = "DATASET"  # entity type, e.g. "DATASET", "DATA_JOB"


class LineageGraph(BaseModel):
    upstreams: List[LineageEdge] = Field(default_factory=list)
    downstreams: List[LineageEdge] = Field(default_factory=list)


class OwnerInfo(BaseModel):
    owner_urn: str
    owner_type: str = "NONE"  # "TECHNICAL_OWNER", "BUSINESS_OWNER", etc.


class DatasetOwnership(BaseModel):
    owners: List[OwnerInfo] = Field(default_factory=list)


class DataHubMCPError(Exception):
    """Raised when DataHub GraphQL returns errors or the HTTP call fails."""


# ---------------------------------------------------------------------------
# GraphQL queries (verified against a live DataHub GMS)
# ---------------------------------------------------------------------------
_SEARCH_DATASETS_QUERY = """
query SearchDatasets($query: String!, $count: Int!) {
  search(input: { type: DATASET, query: $query, start: 0, count: $count }) {
    start
    count
    total
    searchResults {
      entity {
        urn
        type
        ... on Dataset {
          name
          platform {
            name
          }
        }
      }
    }
  }
}
"""

_SCHEMA_QUERY = """
query DatasetSchema($urn: String!) {
  dataset(urn: $urn) {
    urn
    schemaMetadata {
      fields {
        fieldPath
        nativeDataType
        description
      }
    }
  }
}
"""

_LINEAGE_QUERY = """
query DatasetLineage($urn: String!, $direction: LineageDirection!, $count: Int!) {
  dataset(urn: $urn) {
    urn
    lineage(input: { direction: $direction, start: 0, count: $count }) {
      start
      count
      total
      relationships {
        type
        entity {
          urn
          type
          ... on Dataset {
            name
          }
        }
      }
    }
  }
}
"""

_OWNERSHIP_QUERY = """
query DatasetOwnership($urn: String!) {
  dataset(urn: $urn) {
    urn
    ownership {
      owners {
        owner {
          ... on CorpUser {
            urn
          }
          ... on CorpGroup {
            urn
          }
        }
        ownershipType {
          info {
            name
          }
        }
      }
    }
  }
}
"""

_PROPERTIES_QUERY = """
query DatasetProperties($urn: String!) {
  dataset(urn: $urn) {
    urn
    properties {
      name
      qualifiedName
      description
      customProperties {
        key
        value
      }
    }
  }
}
"""

# Default number of lineage edges to fetch per direction.
_LINEAGE_COUNT = 50


def _normalize_owner_type(name: str) -> str:
    """Normalize a display name like 'Business Owner' -> 'BUSINESS_OWNER'."""
    normalized = name.strip().upper().replace(" ", "_").replace("-", "_")
    return normalized or "NONE"


class DataHubMCPClient:
    """
    Async MCP client for DataHub GraphQL API.

    All methods return Pydantic models or raise DataHubMCPError.
    """

    def __init__(
        self,
        gms_url: str = DATAHUB_GMS_URL,
        token: str = DATAHUB_TOKEN,
        timeout: float = 30.0,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.gms_url = gms_url.rstrip("/")
        self.token = token
        self.graphql_url = f"{self.gms_url}/api/graphql"
        self.timeout = timeout
        self._transport = transport  # test hook (e.g. httpx.MockTransport)

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # DataHub quickstart uses basic auth; support a PAT too.
        self.auth: Optional[tuple[str, str]] = None
        if self.token:
            self.headers["Authorization"] = f"Bearer {self.token}"
        else:
            self.auth = (DATAHUB_USER, DATAHUB_PASSWORD)

    def _new_client(self) -> httpx.AsyncClient:
        """Build a fresh AsyncClient per request (no shared lifecycle state)."""
        return httpx.AsyncClient(
            headers=self.headers,
            auth=self.auth,
            timeout=self.timeout,
            transport=self._transport,
        )

    async def _query(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL query and return the data payload.

        Raises DataHubMCPError on transport failures, HTTP errors, or GraphQL
        errors embedded in the response body.
        """
        async with self._new_client() as client:
            try:
                resp = await client.post(
                    self.graphql_url,
                    json={"query": query, "variables": variables or {}},
                )
            except httpx.HTTPError as exc:
                raise DataHubMCPError(
                    f"DataHub GMS unreachable at {self.graphql_url}: {exc}"
                ) from exc

            if resp.status_code != 200:
                raise DataHubMCPError(
                    f"DataHub GMS returned HTTP {resp.status_code}: "
                    f"{resp.text[:500]}"
                )

            try:
                payload = resp.json()
            except ValueError as exc:
                # JSONDecodeError is a subclass of ValueError.
                raise DataHubMCPError(
                    f"DataHub GMS returned invalid JSON (HTTP {resp.status_code}): "
                    f"{resp.text[:500]}"
                ) from exc

            if payload.get("errors"):
                messages = "; ".join(
                    err.get("message", str(err)) for err in payload["errors"]
                )
                raise DataHubMCPError(f"GraphQL error: {messages}")

        return payload.get("data") or {}

    # -- Agents -------------------------------------------------------------

    async def search_datasets(self, query: str = "*", count: int = 10) -> List[DatasetInfo]:
        """Search for datasets. Used by Sentry Agent."""
        data = await self._query(
            _SEARCH_DATASETS_QUERY, {"query": query, "count": count}
        )
        results: List[DatasetInfo] = []
        for item in (data.get("search") or {}).get("searchResults") or []:
            entity = item.get("entity") or {}
            urn = entity.get("urn")
            if not urn:
                continue
            results.append(
                DatasetInfo(
                    urn=urn,
                    name=entity.get("name") or "",
                    platform=((entity.get("platform") or {}).get("name")) or "",
                )
            )
        return results

    async def get_dataset_schema(self, dataset_urn: str) -> DatasetSchema:
        """Get schema fields for a dataset. Used by Engineer Agent."""
        data = await self._query(_SCHEMA_QUERY, {"urn": dataset_urn})
        fields: List[SchemaField] = []
        for field in (
            ((data.get("dataset") or {}).get("schemaMetadata") or {}).get("fields")
            or []
        ):
            fields.append(
                SchemaField(
                    field_path=field.get("fieldPath") or "",
                    native_type=field.get("nativeDataType") or "",
                    description=field.get("description"),
                )
            )
        return DatasetSchema(fields=fields)

    async def get_dataset_lineage(self, dataset_urn: str) -> LineageGraph:
        """Get upstream/downstream lineage. Used by Detective Agent."""
        upstreams, downstreams = await asyncio.gather(
            self._fetch_lineage(dataset_urn, "UPSTREAM"),
            self._fetch_lineage(dataset_urn, "DOWNSTREAM"),
        )
        return LineageGraph(upstreams=upstreams, downstreams=downstreams)

    async def _fetch_lineage(
        self, dataset_urn: str, direction: str
    ) -> List[LineageEdge]:
        data = await self._query(
            _LINEAGE_QUERY,
            {"urn": dataset_urn, "direction": direction, "count": _LINEAGE_COUNT},
        )
        edges: List[LineageEdge] = []
        for rel in (
            ((data.get("dataset") or {}).get("lineage") or {}).get("relationships")
            or []
        ):
            entity = rel.get("entity") or {}
            urn = entity.get("urn")
            if not urn:
                continue
            edges.append(
                LineageEdge(
                    urn=urn,
                    name=entity.get("name") or "",
                    type=entity.get("type") or "DATASET",
                )
            )
        return edges

    async def get_dataset_ownership(self, dataset_urn: str) -> DatasetOwnership:
        """Get ownership info for a dataset. Used by Detective Agent."""
        data = await self._query(_OWNERSHIP_QUERY, {"urn": dataset_urn})
        owners: List[OwnerInfo] = []
        for owner in (
            ((data.get("dataset") or {}).get("ownership") or {}).get("owners") or []
        ):
            owner_urn = (owner.get("owner") or {}).get("urn")
            if not owner_urn:
                continue
            type_name = (
                ((owner.get("ownershipType") or {}).get("info") or {}).get("name")
                or "NONE"
            )
            owners.append(
                OwnerInfo(owner_urn=owner_urn, owner_type=_normalize_owner_type(type_name))
            )
        return DatasetOwnership(owners=owners)

    async def get_dataset_properties(self, dataset_urn: str) -> Dict[str, Any]:
        """Get generic properties (description, custom properties)."""
        data = await self._query(_PROPERTIES_QUERY, {"urn": dataset_urn})
        props = (data.get("dataset") or {}).get("properties") or {}
        custom_properties: Dict[str, Any] = {}
        for entry in props.get("customProperties") or []:
            if entry.get("key"):
                custom_properties[entry["key"]] = entry.get("value")
        return {
            "urn": dataset_urn,
            "name": props.get("name"),
            "qualified_name": props.get("qualifiedName"),
            "description": props.get("description"),
            "custom_properties": custom_properties,
        }


__all__ = [
    "DataHubMCPClient",
    "DataHubMCPError",
    "DatasetInfo",
    "DatasetSchema",
    "DatasetOwnership",
    "LineageEdge",
    "LineageGraph",
    "OwnerInfo",
    "SchemaField",
    "DATAHUB_GMS_URL",
    "DATAHUB_TOKEN",
]
