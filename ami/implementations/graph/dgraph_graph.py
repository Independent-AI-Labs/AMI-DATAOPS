"""Graph-specific operations for Dgraph DAO."""

import json
import logging
import re
from typing import Any, cast

from ami.core.exceptions import StorageError

logger = logging.getLogger(__name__)


def _validate_uid(uid: str) -> str:
    """Validate and sanitize UID format.

    Args:
        uid: UID to validate (hex format 0x... or alphanumeric)

    Returns:
        Sanitized UID

    Raises:
        ValueError: If UID format is invalid
    """
    if not uid:
        msg = "UID cannot be empty"
        raise ValueError(msg)

    # Allow hex UIDs (0x...) or alphanumeric UIDs
    if not re.match(r"^0x[0-9a-fA-F]+$|^[a-zA-Z0-9_-]+$", uid):
        msg = f"Invalid UID format: {uid}"
        raise ValueError(msg)

    return uid


def _validate_identifier(identifier: str) -> str:
    """Validate and sanitize identifiers (collection names, type names).

    Args:
        identifier: Identifier to validate

    Returns:
        Sanitized identifier

    Raises:
        ValueError: If identifier format is invalid
    """
    if not identifier:
        msg = "Identifier cannot be empty"
        raise ValueError(msg)

    # Allow alphanumeric, underscores, hyphens, and dots
    if not re.match(r"^[a-zA-Z0-9_.-]+$", identifier):
        msg = f"Invalid identifier format: {identifier}"
        raise ValueError(msg)

    return identifier


def _validate_positive_int(value: int) -> int:
    """Validate positive integer.

    Args:
        value: Integer to validate

    Returns:
        Validated integer

    Raises:
        ValueError: If value is not positive
    """
    if not isinstance(value, int) or value < 1:
        msg = f"Value must be a positive integer: {value}"
        raise ValueError(msg)

    return value


async def k_hop_query(
    dao: Any,
    start_id: str,
    _k: int,
    _edge_types: list[str] | None = None,
) -> dict[str, Any]:
    """Perform k-hop graph traversal from a starting node.

    Args:
        start_id: UID of the starting node
        _k: Number of hops to traverse (currently unused, defaults to 1-hop)
        _edge_types: Optional list of edge types to follow (currently unused)

    Returns:
        Dict containing nodes and edges found in traversal
    """
    if not dao.client:
        await dao.connect()

    try:
        # Validate inputs BEFORE using them
        validated_start_id = _validate_uid(start_id)
        collection_name = _validate_identifier(dao.collection_name)

        txn = dao.client.txn(read_only=True)
        try:
            # First, find the internal Dgraph UID from the application UID
            if validated_start_id.startswith("0x"):
                # It's already a Dgraph UID
                dgraph_uid = validated_start_id
            else:
                # Search for the node by application UID
                # INJECTION SAFETY: collection_name is validated via
                # _validate_identifier() to match ^[a-zA-Z0-9_.-]+$
                # User input (validated_start_id) is passed as parameterized
                # variable ($app_uid) - NOT via string interpolation
                find_query = (
                    "query find_node($app_uid: string) {\n"
                    f"    find_node(func: eq({collection_name}.app_uid, $app_uid))"
                    f" @filter(type({collection_name})) {{\n"
                    "        uid\n"
                    "    }\n"
                    "}"
                )
                find_response = txn.query(
                    find_query,
                    variables={"$app_uid": validated_start_id},
                )
                find_result = json.loads(find_response.json)

                if (
                    not find_result.get("find_node")
                    or len(find_result["find_node"]) == 0
                ):
                    msg = f"Node not found: {validated_start_id}"
                    raise StorageError(msg)

                dgraph_uid = find_result["find_node"][0]["uid"]

            # Validate the retrieved UID
            validated_dgraph_uid = _validate_uid(dgraph_uid)

            # Now perform k-hop query with the internal Dgraph UID
            # Use parameterized query for UID value
            query = """
            query k_hop($dgraph_uid: string) {
                path(func: uid($dgraph_uid)) {
                    uid
                    dgraph.type
                    expand(_all_) {
                        uid
                        dgraph.type
                    }
                }
            }
            """

            response = txn.query(query, variables={"$dgraph_uid": validated_dgraph_uid})
            result = json.loads(response.json)
            return cast(dict[str, Any], result.get("path", []))
        finally:
            txn.discard()

    except ValueError as e:
        logger.exception("Invalid input for k-hop query")
        msg = f"Invalid input: {e}"
        raise StorageError(msg) from e
    except Exception as e:
        logger.exception("K-hop query failed")
        msg = f"K-hop traversal failed: {e}"
        raise StorageError(msg) from e


async def shortest_path(
    dao: Any, start_id: str, end_id: str, max_depth: int = 10
) -> list[str]:
    """Find shortest path between two nodes.

    Args:
        start_id: Starting node UID
        end_id: Target node UID
        max_depth: Maximum depth to search

    Returns:
        List of UIDs representing the shortest path
    """
    if not dao.client:
        await dao.connect()

    try:
        # Validate inputs BEFORE using them
        validated_start_id = _validate_uid(start_id)
        validated_end_id = _validate_uid(end_id)
        validated_max_depth = _validate_positive_int(max_depth)

        # Dgraph shortest path query - use parameterized query for values
        query = """
        query shortest_path($start: string, $end: string, $depth: int) {
            path as shortest(from: $start, to: $end, depth: $depth) {
                uid
            }

            path_nodes(func: uid(path)) {
                uid
                dgraph.type
                expand(_all_)
            }
        }
        """

        txn = dao.client.txn(read_only=True)
        try:
            response = txn.query(
                query,
                variables={
                    "$start": validated_start_id,
                    "$end": validated_end_id,
                    "$depth": validated_max_depth,
                },
            )
            result = json.loads(response.json)
        finally:
            txn.discard()

        # Extract path UIDs
        path_nodes = result.get("path_nodes", [])
        return [node["uid"] for node in path_nodes]

    except ValueError as e:
        logger.exception("Invalid input for shortest path query")
        msg = f"Invalid input: {e}"
        raise StorageError(msg) from e
    except Exception as e:
        logger.exception("Shortest path query failed")
        msg = f"Shortest path search failed: {e}"
        raise StorageError(msg) from e


async def _get_all_nodes(dao: Any, node_type: str | None) -> list[dict[str, Any]]:
    """Get all nodes of specified type."""
    if node_type:
        # Validate node type BEFORE using it in structural position
        # INJECTION SAFETY: validated_node_type restricted to
        # ^[a-zA-Z0-9_.-]+$ via _validate_identifier()
        validated_node_type = _validate_identifier(node_type)
        query = (
            "{\n"
            f"    nodes(func: type({validated_node_type})) {{\n"
            "        uid\n"
            "        dgraph.type\n"
            "    }\n"
            "}"
        )
    else:
        query = """
        {
            nodes(func: has(dgraph.type)) {
                uid
                dgraph.type
            }
        }
        """

    txn = dao.client.txn(read_only=True)
    try:
        response = txn.query(query)
        result = json.loads(response.json)
        return cast(list[dict[str, Any]], result.get("nodes", []))
    finally:
        txn.discard()


async def _get_node_neighbors(dao: Any, node_uid: str) -> list[str]:
    """Get all neighbor UIDs for a given node."""
    # Validate node UID BEFORE using it
    validated_node_uid = _validate_uid(node_uid)

    # Use parameterized query for UID value
    neighbor_query = """
    query get_neighbors($node_uid: string) {
        node(func: uid($node_uid)) {
            expand(_all_) {
                uid
            }
        }
    }
    """

    txn = dao.client.txn(read_only=True)
    try:
        response = txn.query(
            neighbor_query, variables={"$node_uid": validated_node_uid}
        )
        result = json.loads(response.json)

        neighbors: list[str] = []
        if result.get("node"):
            for value in result["node"][0].values():
                if isinstance(value, list):
                    neighbors.extend(
                        item["uid"]
                        for item in value
                        if isinstance(item, dict) and "uid" in item
                    )
        return neighbors
    finally:
        txn.discard()


async def _find_component_dfs(dao: Any, start_uid: str, visited: set[str]) -> list[str]:
    """Find a connected component using DFS from start_uid."""
    component = []
    stack = [start_uid]

    while stack:
        current = stack.pop()
        if current not in visited:
            visited.add(current)
            component.append(current)

            # Get neighbors and add unvisited ones to stack
            neighbors = await _get_node_neighbors(dao, current)
            stack.extend(uid for uid in neighbors if uid not in visited)

    return component


async def find_connected_components(
    dao: Any, node_type: str | None = None
) -> list[list[str]]:
    """Find all connected components in the graph.

    Args:
        node_type: Optional type filter for nodes

    Returns:
        List of connected components (each component is a list of UIDs)
    """
    if not dao.client:
        await dao.connect()

    try:
        nodes = await _get_all_nodes(dao, node_type)

        # Track visited nodes
        visited: set[str] = set()
        components = []

        # DFS to find components
        for node in nodes:
            uid = node["uid"]
            if uid not in visited:
                component = await _find_component_dfs(dao, uid, visited)
                if component:
                    components.append(component)

    except Exception as e:
        logger.exception("Connected components query failed")
        msg = f"Connected components search failed: {e}"
        raise StorageError(msg) from e
    else:
        return components


def _validate_direction(direction: str) -> None:
    """Validate direction parameter for degree queries."""
    valid = ("in", "out", "all")
    if direction not in valid:
        msg = f"Invalid direction: {direction}. Must be 'in', 'out', or 'all'"
        raise ValueError(msg)


def _count_degrees(
    node_data: dict[str, Any],
) -> tuple[int, int]:
    """Count in-degree and out-degree from node data."""
    out_degree = 0
    in_degree = 0
    skip_keys = {"uid", "dgraph.type"}
    for key, value in node_data.items():
        if key.startswith("~"):
            if isinstance(value, list):
                in_degree += len(value)
        elif key not in skip_keys and isinstance(value, list):
            out_degree += len(value)
    return in_degree, out_degree


def _format_degree_result(
    direction: str,
    in_degree: int,
    out_degree: int,
) -> dict[str, int]:
    """Format the degree result based on direction."""
    if direction == "in":
        return {"in": in_degree}
    if direction == "out":
        return {"out": out_degree}
    return {
        "in": in_degree,
        "out": out_degree,
        "total": in_degree + out_degree,
    }


async def get_node_degree(
    dao: Any, node_id: str, direction: str = "all"
) -> dict[str, int]:
    """Get degree of a node (in-degree, out-degree, or total).

    Args:
        node_id: Node UID
        direction: "in", "out", or "all"

    Returns:
        Dict with degree counts
    """
    if not dao.client:
        await dao.connect()

    try:
        validated_node_id = _validate_uid(node_id)
        _validate_direction(direction)

        query = """
        query get_degree($node_id: string) {
            node(func: uid($node_id)) {
                uid
                dgraph.type
                expand(_all_) {
                    count(uid)
                }
                ~expand(_all_) {
                    count(uid)
                }
            }
        }
        """

        txn = dao.client.txn(read_only=True)
        try:
            response = txn.query(
                query,
                variables={"$node_id": validated_node_id},
            )
            result = json.loads(response.json)
        finally:
            txn.discard()

        if not result.get("node"):
            return {"in": 0, "out": 0, "total": 0}

        in_deg, out_deg = _count_degrees(result["node"][0])
        return _format_degree_result(direction, in_deg, out_deg)

    except ValueError as e:
        logger.exception("Invalid input for node degree query")
        msg = f"Invalid input: {e}"
        raise StorageError(msg) from e
    except Exception as e:
        logger.exception("Node degree query failed")
        msg = f"Node degree calculation failed: {e}"
        raise StorageError(msg) from e
