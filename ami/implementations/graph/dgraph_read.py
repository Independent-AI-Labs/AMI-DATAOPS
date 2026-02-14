"""Read operations for Dgraph DAO."""

import json
import logging
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.graph.dgraph_util import (
    build_count_query,
    build_dql_query,
    from_dgraph_format,
)
from ami.models.base_model import StorageModel

logger = logging.getLogger(__name__)


async def find_by_id(dao: Any, item_id: str) -> StorageModel | None:
    """Find node by UID or regular ID."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Check if it's a Dgraph UID or regular ID
    if item_id.startswith("0x"):
        # Build query for UID - don't use @filter with uid()
        query = f"""
        {{
            node(func: uid({item_id})) {{
                uid
                expand(_all_)
                dgraph.type
            }}
        }}
        """
    else:
        # Build query for application UID field first (most likely case)
        coll = dao.collection_name
        query = (
            "{\n"
            f'    node(func: eq({coll}.app_uid, "{item_id}"))'
            f" @filter(type({coll})) {{\n"
            "        uid\n"
            "        expand(_all_)\n"
            "        dgraph.type\n"
            "    }\n"
            "}"
        )

    txn = dao.client.txn(read_only=True)
    try:
        response = txn.query(query)
        data = json.loads(response.json)
        logger.debug("Query response: %s", data)

        if data.get("node") and len(data["node"]) > 0:
            node_data = data["node"][0]
            logger.debug("Retrieved node data from Dgraph: %s", node_data)

            # Check if we have the expected type
            if item_id.startswith("0x"):
                # For UID queries, verify the type matches
                dgraph_type = node_data.get("dgraph.type", [])
                if (
                    isinstance(dgraph_type, list)
                    and dao.collection_name not in dgraph_type
                ):
                    logger.debug(
                        "Node type %s does not match expected %s",
                        dgraph_type,
                        dao.collection_name,
                    )
                    return None

            return from_dgraph_format(node_data, dao.model_cls, dao.collection_name)

        return None

    finally:
        txn.discard()


async def find_one(dao: Any, query: dict[str, Any]) -> StorageModel | None:
    """Find single node matching query."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Build DQL query from dict
    dql = build_dql_query(query, dao.collection_name, limit=1)

    txn = dao.client.txn(read_only=True)
    try:
        response = txn.query(dql)
        data = json.loads(response.json)

        result_key = f"{dao.collection_name}_results"
        if data.get(result_key) and len(data[result_key]) > 0:
            node_data = data[result_key][0]
            return from_dgraph_format(node_data, dao.model_cls, dao.collection_name)

        return None

    finally:
        txn.discard()


async def find(
    dao: Any,
    query: dict[str, Any],
    limit: int | None = None,
    skip: int = 0,
) -> list[StorageModel]:
    """Find multiple nodes matching query."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Build DQL query
    dql = build_dql_query(query, dao.collection_name, limit=limit, offset=skip)

    # Debug logging
    logger.debug("DQL Query: %s", dql)

    txn = dao.client.txn(read_only=True)
    try:
        response = txn.query(dql)
        data = json.loads(response.json)

        # Debug the response
        logger.debug("Query response: %s", data)

        result_key = f"{dao.collection_name}_results"
        results = [
            instance
            for node_data in data.get(result_key, [])
            if (
                instance := from_dgraph_format(
                    node_data,
                    dao.model_cls,
                    dao.collection_name,
                )
            )
        ]

        return results

    finally:
        txn.discard()


async def count(dao: Any, query: dict[str, Any]) -> int:
    """Count nodes matching query."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Build count query
    dql = build_count_query(query, dao.collection_name)

    txn = dao.client.txn(read_only=True)
    try:
        response = txn.query(dql)
        data = json.loads(response.json)

        # Extract count
        count_result = data.get("count", [{}])[0]
        total = count_result.get("total", 0)
        return int(total) if total is not None else 0

    finally:
        txn.discard()


async def exists(dao: Any, item_id: str) -> bool:
    """Check if node exists.

    Raises:
        StorageError: If not connected or query execution fails
    """
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    try:
        # Check both by UID and by ID field
        if item_id.startswith("0x"):
            # It's a Dgraph UID
            node = await find_by_id(dao, item_id)
            return node is not None
        # It's a regular ID, query by field
        coll = dao.collection_name
        query = (
            "{\n"
            f'    node(func: eq({coll}.uid, "{item_id}"))'
            f" @filter(type({coll})) {{\n"
            "        uid\n"
            "    }\n"
            "}"
        )

        txn = dao.client.txn(read_only=True)
        try:
            response = txn.query(query)
            result = json.loads(response.json)
            return result.get("node") and len(result["node"]) > 0
        finally:
            txn.discard()
    except Exception as e:
        msg = f"Failed to check existence for item {item_id}"
        raise StorageError(msg) from e


async def raw_read_query(
    dao: Any, query: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    """Execute raw DQL read query."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    txn = dao.client.txn(read_only=True)
    try:
        # Add variables if provided
        response = txn.query(query, variables=params) if params else txn.query(query)

        result = json.loads(response.json)
        return (
            result
            if isinstance(result, list)
            else [result]
            if isinstance(result, dict)
            else []
        )

    finally:
        txn.discard()


async def list_databases(_dao: Any) -> list[str]:
    """List namespaces in Dgraph."""
    # Dgraph doesn't have traditional databases, return default
    return ["default"]


async def list_schemas(dao: Any, _database: str | None = None) -> list[str]:
    """List types in Dgraph."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    query = """
    {
        types(func: has(dgraph.type)) @groupby(dgraph.type) {
            count(uid)
        }
    }
    """

    result = await raw_read_query(dao, query)
    types_list = []

    result_dict = result[0] if isinstance(result, list) and result else result
    for group in result_dict.get("types", []) if isinstance(result_dict, dict) else []:
        type_name = group.get("@groupby", [{}])[0].get("dgraph.type")
        if type_name:
            types_list.append(type_name)

    return types_list


async def list_models(
    dao: Any, database: str | None = None, _schema: str | None = None
) -> list[str]:
    """List types (models) in Dgraph."""
    return await list_schemas(dao, database)


async def get_model_info(
    dao: Any,
    path: str,
    _database: str | None = None,
    _schema: str | None = None,
) -> dict[str, Any]:
    """Get information about a type."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Query for type info
    query = f"""
    {{
        type_info(func: type({path})) {{
            count(uid)
        }}
    }}
    """

    result = await raw_read_query(dao, query)

    result_dict = result[0] if isinstance(result, list) and result else result
    type_info = (
        result_dict.get("type_info", [{}]) if isinstance(result_dict, dict) else [{}]
    )
    count_val = type_info[0].get("count(uid)", 0) if type_info else 0
    return {"type": path, "count": count_val}


async def get_model_schema(
    dao: Any,
    path: str,
    _database: str | None = None,
    _schema: str | None = None,
) -> dict[str, Any]:
    """Get schema for a type."""
    if not dao.client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    # Query schema
    query = "{schema {}}"
    result = await raw_read_query(dao, query)

    # Filter for this type
    type_schema = {}
    result_dict = result[0] if isinstance(result, list) and result else result
    for pred in result_dict.get("schema", []) if isinstance(result_dict, dict) else []:
        if pred.get("predicate", "").startswith(f"{path}."):
            type_schema[pred["predicate"]] = pred

    return type_schema


async def get_model_fields(
    dao: Any,
    path: str,
    _database: str | None = None,
    _schema: str | None = None,
) -> list[dict[str, Any]]:
    """Get fields for a type."""
    schema_info = await get_model_schema(dao, path)

    fields = []
    for pred_name, pred_info in schema_info.items():
        field_name = pred_name.replace(f"{path}.", "")
        fields.append(
            {
                "name": field_name,
                "type": pred_info.get("type"),
                "index": pred_info.get("index"),
                "list": pred_info.get("list", False),
            }
        )

    return fields


async def get_model_indexes(
    dao: Any,
    path: str,
    _database: str | None = None,
    _schema: str | None = None,
) -> list[dict[str, Any]]:
    """Get indexes for a type."""
    fields = await get_model_fields(dao, path)

    indexes = [
        {"field": field["name"], "type": field["index"]}
        for field in fields
        if field.get("index")
    ]

    return indexes


async def test_connection(dao: Any) -> bool:
    """Test if connection is valid.

    Raises:
        StorageError: If health check fails or connection is invalid
    """
    if not dao.client:
        msg = "Client not initialized"
        raise StorageError(msg)

    try:
        # Simple health check query - just query schema
        query = "{schema {}}"
        await raw_read_query(dao, query)
    except Exception as e:
        msg = f"Health check failed: {e}"
        raise StorageError(msg) from e
    else:
        return True
