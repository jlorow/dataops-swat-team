"""DataHub integration package — typed GraphQL access for the SWAT agents."""

from .mcp_client import (
    DataHubMCPClient,
    DataHubMCPError,
    DatasetInfo,
    DatasetOwnership,
    DatasetSchema,
    LineageEdge,
    LineageGraph,
    OwnerInfo,
    SchemaField,
)

__all__ = [
    "DataHubMCPClient",
    "DataHubMCPError",
    "DatasetInfo",
    "DatasetOwnership",
    "DatasetSchema",
    "LineageEdge",
    "LineageGraph",
    "OwnerInfo",
    "SchemaField",
]
