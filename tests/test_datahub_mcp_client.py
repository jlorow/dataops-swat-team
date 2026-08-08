"""Unit tests for the DataHub MCP client (no network required).

The DataHub GMS is stubbed with httpx.MockTransport so the tests run offline
and are deterministic. Sync test functions drive the async client with
asyncio.run() -- no pytest-asyncio dependency needed.
"""

import asyncio
import base64
import json

import httpx
import pytest

from src.datahub.mcp_client import (
    DataHubMCPClient,
    DataHubMCPError,
    DatasetOwnership,
    DatasetSchema,
    LineageGraph,
)


def run(coro):
    """Run an async coroutine from a sync test."""
    return asyncio.run(coro)


def client_with(handler):
    return DataHubMCPClient(
        gms_url="http://datahub.test",
        transport=httpx.MockTransport(handler),
    )


def graphql_response(data):
    return httpx.Response(200, json={"data": data, "extensions": {}})


# ---------------------------------------------------------------------------
# search_datasets
# ---------------------------------------------------------------------------
def test_search_datasets_parses_results():
    def handler(request):
        body = json.loads(request.content)
        assert body["variables"] == {"query": "order", "count": 5}
        assert "search(input:" in body["query"]
        return graphql_response(
            {
                "search": {
                    "start": 0,
                    "count": 5,
                    "total": 1,
                    "searchResults": [
                        {
                            "entity": {
                                "urn": "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)",
                                "type": "DATASET",
                                "name": "orders",
                                "platform": {"name": "dbt"},
                            }
                        }
                    ],
                }
            }
        )

    client = client_with(handler)
    results = run(client.search_datasets("order", count=5))

    assert len(results) == 1
    assert results[0].urn == "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"
    assert results[0].name == "orders"
    assert results[0].platform == "dbt"


def test_search_datasets_empty_results():
    def handler(request):
        return graphql_response({"search": {"searchResults": []}})

    client = client_with(handler)
    assert run(client.search_datasets()) == []


# ---------------------------------------------------------------------------
# get_dataset_schema
# ---------------------------------------------------------------------------
def test_get_dataset_schema_parses_fields():
    def handler(request):
        assert "schemaMetadata" in request.content.decode()
        return graphql_response(
            {
                "dataset": {
                    "urn": "urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)",
                    "schemaMetadata": {
                        "fields": [
                            {
                                "fieldPath": "order_id",
                                "nativeDataType": "NUMBER",
                                "description": "Unique id",
                            },
                            {
                                "fieldPath": "amount",
                                "nativeDataType": "FLOAT",
                                "description": None,
                            },
                        ]
                    },
                }
            }
        )

    client = client_with(handler)
    schema = run(client.get_dataset_schema("urn:li:dataset:(urn:li:dataPlatform:dbt,db.orders,PROD)"))

    assert isinstance(schema, DatasetSchema)
    assert len(schema.fields) == 2
    assert schema.fields[0].field_path == "order_id"
    assert schema.fields[0].native_type == "NUMBER"
    assert schema.fields[0].description == "Unique id"
    assert schema.fields[1].description is None


def test_get_dataset_schema_missing_schema_metadata():
    def handler(request):
        return graphql_response({"dataset": {"urn": "urn:x", "schemaMetadata": None}})

    client = client_with(handler)
    schema = run(client.get_dataset_schema("urn:x"))
    assert isinstance(schema, DatasetSchema)
    assert schema.fields == []


# ---------------------------------------------------------------------------
# get_dataset_lineage
# ---------------------------------------------------------------------------
def test_get_dataset_lineage_fetches_both_directions():
    directions_seen = []

    def handler(request):
        variables = json.loads(request.content)["variables"]
        directions_seen.append(variables["direction"])
        return graphql_response(
            {
                "dataset": {
                    "urn": variables["urn"],
                    "lineage": {
                        "start": 0,
                        "count": 10,
                        "total": 1,
                        "relationships": [
                            {
                                "type": "DownstreamOf",
                                "entity": {
                                    "urn": f"urn:edge:{variables['direction']}",
                                    "type": "DATASET",
                                    "name": f"tbl_{variables['direction'].lower()}",
                                },
                            }
                        ],
                    },
                }
            }
        )

    client = client_with(handler)
    graph = run(client.get_dataset_lineage("urn:dataset:orders"))

    assert isinstance(graph, LineageGraph)
    assert sorted(directions_seen) == ["DOWNSTREAM", "UPSTREAM"]
    assert len(graph.upstreams) == 1
    assert graph.upstreams[0].name == "tbl_upstream"
    assert len(graph.downstreams) == 1
    assert graph.downstreams[0].urn == "urn:edge:DOWNSTREAM"
    assert graph.downstreams[0].type == "DATASET"


# ---------------------------------------------------------------------------
# get_dataset_ownership
# ---------------------------------------------------------------------------
def test_get_dataset_ownership_normalizes_types():
    def handler(request):
        return graphql_response(
            {
                "dataset": {
                    "urn": "urn:1",
                    "ownership": {
                        "owners": [
                            {
                                "owner": {"urn": "urn:li:corpuser:alice"},
                                "ownershipType": {"info": {"name": "Business Owner"}},
                            },
                            {
                                "owner": {"urn": "urn:li:corpgroup:data-platform"},
                                "ownershipType": {"info": {"name": "Data Steward"}},
                            },
                            {
                                "owner": {"urn": "urn:li:corpuser:bob"},
                                "ownershipType": {"info": {"name": None}},
                            },
                        ]
                    },
                }
            }
        )

    client = client_with(handler)
    ownership = run(client.get_dataset_ownership("urn:1"))

    assert isinstance(ownership, DatasetOwnership)
    assert ownership.owners[0].owner_type == "BUSINESS_OWNER"
    assert ownership.owners[0].owner_urn == "urn:li:corpuser:alice"
    assert ownership.owners[1].owner_type == "DATA_STEWARD"
    assert ownership.owners[2].owner_type == "NONE"


# ---------------------------------------------------------------------------
# get_dataset_properties
# ---------------------------------------------------------------------------
def test_get_dataset_properties_parses_custom_props():
    def handler(request):
        return graphql_response(
            {
                "dataset": {
                    "urn": "urn:1",
                    "properties": {
                        "name": "orders",
                        "qualifiedName": "db.orders",
                        "description": "the orders table",
                        "customProperties": [
                            {"key": "owner_team", "value": "data-platform"},
                            {"key": "tier", "value": "gold"},
                        ],
                    },
                }
            }
        )

    client = client_with(handler)
    props = run(client.get_dataset_properties("urn:1"))

    assert props["name"] == "orders"
    assert props["qualified_name"] == "db.orders"
    assert props["custom_properties"] == {
        "owner_team": "data-platform",
        "tier": "gold",
    }


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------
def test_query_raises_on_graphql_errors():
    def handler(request):
        return httpx.Response(
            200,
            json={"errors": [{"message": "Failed to find dataset with urn foo"}]},
        )

    client = client_with(handler)
    with pytest.raises(DataHubMCPError, match="Failed to find dataset"):
        run(client.get_dataset_schema("urn:foo"))


def test_query_raises_on_http_error():
    def handler(request):
        return httpx.Response(503, text="service unavailable")

    client = client_with(handler)
    with pytest.raises(DataHubMCPError, match="HTTP 503"):
        run(client.search_datasets())


def test_query_raises_on_invalid_json():
    def handler(request):
        return httpx.Response(200, text="<html>this is not json</html>")

    client = client_with(handler)
    with pytest.raises(DataHubMCPError, match="invalid JSON"):
        run(client.search_datasets())


def test_query_raises_on_connection_error():
    def handler(request):
        raise httpx.ConnectError("connection refused", request=request)

    client = client_with(handler)
    with pytest.raises(DataHubMCPError, match="unreachable"):
        run(client.search_datasets())


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
def test_default_basic_auth_used():
    auth_header = {}

    def handler(request):
        auth_header["value"] = request.headers.get("authorization")
        return graphql_response({"search": {"searchResults": []}})

    client = client_with(handler)
    run(client.search_datasets())

    expected = "Basic " + base64.b64encode(b"datahub:datahub").decode()
    assert auth_header["value"] == expected


def test_token_uses_bearer_auth():
    client = DataHubMCPClient(
        gms_url="http://datahub.test",
        token="pat-123",
        transport=httpx.MockTransport(lambda req: graphql_response({"search": {"searchResults": []}})),
    )
    assert client.headers["Authorization"] == "Bearer pat-123"
    assert client.auth is None

    results = run(client.search_datasets())
    assert results == []
