"""Traversal operations for Dgraph DAO relational graph management."""

import json
import logging
from typing import Any, Protocol, cast

import pydgraph

from ami.core.exceptions import StorageError
from ami.core.graph_relations import GraphSchemaAnalyzer
from ami.implementations.graph.dgraph_graph import (
    _validate_identifier,
    _validate_uid,
)
from ami.models.base_model import StorageModel

logger = logging.getLogger(__name__)


class DgraphLikeDAO(Protocol):
    """Protocol for DAO-like objects that work with DgraphTraversalMixin."""

    client: pydgraph.DgraphClient | None
    collection_name: str
    model_cls: type[StorageModel]


class DgraphTraversalMixin:
    """Mixin for DgraphDAO to add traversal and query capabilities."""

    async def get_edges(
        self,
        node_uid: str,
        edge_name: str | None = None,
        direction: str = "out",
    ) -> list[dict[str, Any]]:
        """Get edges for a node.

        Args:
            node_uid: Node UID
            edge_name: Specific edge name or None for all
            direction: "out", "in", or "both"

        Returns:
            List of edge information
        """
        dao = cast(DgraphLikeDAO, self)
        if not dao.client:
            msg = "Not connected to Dgraph"
            raise StorageError(msg)

        # Validate node_uid and edge_name before interpolation
        validated_uid = _validate_uid(node_uid)

        txn = dao.client.txn(read_only=True)
        try:
            if edge_name:
                validated_edge = _validate_identifier(edge_name)

                if direction == "out":
                    query = (
                        "{\n"
                        f"    node(func: uid({validated_uid})) {{\n"
                        "        uid\n"
                        f"        {validated_edge} {{\n"
                        "            uid\n"
                        "            expand(_all_)\n"
                        "        }\n"
                        "    }\n"
                        "}"
                    )
                elif direction == "in":
                    query = (
                        "{\n"
                        f"    node(func: uid({validated_uid})) {{\n"
                        "        uid\n"
                        f"        ~{validated_edge} {{\n"
                        "            uid\n"
                        "            expand(_all_)\n"
                        "        }\n"
                        "    }\n"
                        "}"
                    )
                else:  # both
                    query = (
                        "{\n"
                        f"    node(func: uid({validated_uid})) {{\n"
                        "        uid\n"
                        f"        {validated_edge} {{\n"
                        "            uid\n"
                        "            expand(_all_)\n"
                        "        }\n"
                        f"        ~{validated_edge} {{\n"
                        "            uid\n"
                        "            expand(_all_)\n"
                        "        }\n"
                        "    }\n"
                        "}"
                    )
            else:
                # Get all edges
                query = (
                    "{\n"
                    f"    node(func: uid({validated_uid})) {{\n"
                    "        uid\n"
                    "        expand(_all_)\n"
                    "    }\n"
                    "}"
                )

            response = txn.query(query)
            try:
                result = json.loads(response.json)
            except json.JSONDecodeError as e:
                msg = f"Failed to parse Dgraph response for node {node_uid}"
                raise StorageError(msg) from e

            if "node" not in result:
                msg = (
                    f"Node {node_uid} not found in Dgraph: "
                    "'node' key missing from response"
                )
                raise StorageError(msg)

            nodes = result["node"]
            if not isinstance(nodes, list):
                msg = (
                    "Unexpected node data structure for "
                    f"{node_uid}: expected list, "
                    f"got {type(nodes)}"
                )
                raise StorageError(msg)
            return list(nodes)

        finally:
            txn.discard()

    async def load_with_relations(
        self,
        item_id: str,
        relations: list[str] | None = None,
        depth: int = 1,
    ) -> StorageModel:
        """Load an instance with its related objects.

        Args:
            item_id: ID of the item to load
            relations: List of relation names to load (None for all)
            depth: How deep to load relations (1 = direct only)

        Returns:
            Instance with loaded relations

        Raises:
            StorageError: If the item is not found or query fails
        """
        dao = cast(DgraphLikeDAO, self)
        if not dao.client:
            msg = "Not connected to Dgraph"
            raise StorageError(msg)

        # First find the node by ID
        txn = dao.client.txn(read_only=True)
        try:
            result = self._find_node_by_id(txn, dao, item_id)
            uid = result["items"][0]["uid"]

            # Now load with relations
            schema = GraphSchemaAnalyzer.analyze_model(dao.model_cls)

            # Build query with relations
            query_parts = ["uid", "expand(_all_)"]

            self._add_relation_query_parts(query_parts, relations, schema, depth)

            # Build final query
            query = "{{\n    node(func: uid({})) {{\n        {}\n    }}\n}}".format(
                uid, "\n        ".join(query_parts)
            )

            response = txn.query(query)
            try:
                result = json.loads(response.json)
            except json.JSONDecodeError as e:
                msg = (
                    "Failed to parse Dgraph response when "
                    f"loading relations for item {item_id}"
                )
                raise StorageError(msg) from e

            self._validate_node_result(result, item_id)

            node_data = result["node"][0]
            # Convert to model instance
            return self._dgraph_to_model(node_data, schema, dao)

        finally:
            txn.discard()

    def _find_node_by_id(
        self,
        txn: Any,
        dao: DgraphLikeDAO,
        item_id: str,
    ) -> dict[str, Any]:
        """Find a node by ID and return the parsed result."""
        id_query = (
            "{\n"
            f"    items(func: eq({dao.collection_name}.uid,"
            f' "{item_id}")) {{\n'
            "        uid\n"
            "    }\n"
            "}"
        )

        response = txn.query(id_query)
        try:
            result = json.loads(response.json)
        except json.JSONDecodeError as e:
            msg = f"Failed to parse Dgraph response for item {item_id}"
            raise StorageError(msg) from e

        if "items" not in result:
            msg = (
                f"Item with ID {item_id} not found in Dgraph: "
                "'items' key missing from response"
            )
            raise StorageError(msg)
        if not isinstance(result["items"], list):
            msg = (
                f"Item with ID {item_id} query failed: "
                "'items' is not a list in response"
            )
            raise StorageError(msg)
        if not result["items"]:
            msg = f"Item with ID {item_id} not found in Dgraph"
            raise StorageError(msg)

        parsed: dict[str, Any] = result
        return parsed

    def _add_relation_query_parts(
        self,
        query_parts: list[str],
        relations: list[str] | None,
        schema: dict[str, Any],
        depth: int,
    ) -> None:
        """Add relation query parts to the query."""
        if relations:
            # Load specific relations
            for rel_name in relations:
                if rel_name in schema["edges"]:
                    edge_config = schema["edges"][rel_name]
                    edge_nm = edge_config["edge_name"]

                    if depth > 1:
                        query_parts.append(
                            f"{edge_nm} @recurse(depth: {depth})"
                            " {\n"
                            "    uid\n"
                            "    expand(_all_)\n"
                            "}"
                        )
                    else:
                        query_parts.append(
                            f"{edge_nm} {{\n    uid\n    expand(_all_)\n}}"
                        )
        elif depth > 1:
            # Load all relations to specified depth
            for edge_config in schema["edges"].values():
                edge_nm = edge_config["edge_name"]
                query_parts.append(f"{edge_nm} {{\n    uid\n    expand(_all_)\n}}")

    def _validate_node_result(
        self,
        result: dict[str, Any],
        item_id: str,
    ) -> None:
        """Validate the node result from a Dgraph query."""
        if "node" not in result:
            msg = (
                f"Item with ID {item_id} not found in Dgraph "
                "after loading relations: "
                "'node' key missing from response"
            )
            raise StorageError(msg)
        if not isinstance(result["node"], list):
            msg = (
                f"Item with ID {item_id} query failed: 'node' is not a list in response"
            )
            raise StorageError(msg)
        if not result["node"]:
            msg = f"Item with ID {item_id} not found in Dgraph after loading relations"
            raise StorageError(msg)

    def _dgraph_to_model(
        self,
        dgraph_data: dict[str, Any],
        schema: dict[str, Any],
        dao: DgraphLikeDAO,
    ) -> StorageModel:
        """Convert Dgraph data to model instance with relations."""
        # Extract regular properties
        model_data = {}
        prefix = f"{dao.collection_name}."

        for key, value in dgraph_data.items():
            if key == "uid":
                model_data["graph_id"] = value
            elif key.startswith(prefix):
                field_name = key[len(prefix) :]
                model_data[field_name] = value

        # Handle edges - store as IDs for now
        for field_name, edge_config in schema["edges"].items():
            edge_name = edge_config["edge_name"]
            if edge_name in dgraph_data:
                edge_data = dgraph_data[edge_name]
                if edge_config["is_list"]:
                    # Extract UIDs from list
                    model_data[field_name] = [
                        item["uid"]
                        for item in edge_data
                        if isinstance(item, dict) and "uid" in item
                    ]
                # Single edge
                elif isinstance(edge_data, dict) and "uid" in edge_data:
                    model_data[field_name] = edge_data["uid"]
                elif isinstance(edge_data, list) and edge_data:
                    # Take first if it's a list
                    model_data[field_name] = edge_data[0]["uid"]

        # Create model instance
        return dao.model_cls.from_storage_dict(model_data)

    def _build_traverse_query(self, start_uid: str, edge_path: list[str]) -> str:
        """Build a nested query for graph traversal."""
        query_parts = []

        for i, edge in enumerate(edge_path):
            indent = "  " * i
            query_parts.append(f"{indent}{edge} {{")

            if i == len(edge_path) - 1:
                query_parts.append(f"{indent}  uid")
                query_parts.append(f"{indent}  expand(_all_)")
            else:
                query_parts.append(f"{indent}  uid")

        # Close all brackets
        for i in range(len(edge_path) - 1, -1, -1):
            indent = "  " * i
            query_parts.append(f"{indent}}}")

        return (
            "{{\n"
            "    path(func: uid({})) {{\n"
            "        uid\n"
            "        {}\n"
            "    }}\n"
            "}}".format(start_uid, "\n        ".join(query_parts))
        )

    def _extract_traverse_nodes(
        self,
        result: dict[str, Any],
        edge_path: list[str],
    ) -> list[dict[str, Any]]:
        """Extract nodes from traversal result."""
        if "path" not in result:
            msg = "Path not found in traversal result: 'path' key missing from response"
            raise StorageError(msg)
        if not isinstance(result["path"], list):
            msg = "Path query failed: 'path' is not a list in response"
            raise StorageError(msg)

        nodes = result["path"]
        for edge in edge_path:
            nodes = self._follow_edge(nodes, edge)

        if not isinstance(nodes, list):
            msg = (
                "Unexpected node data structure after traversal: "
                f"expected list, got {type(nodes)}"
            )
            raise StorageError(msg)
        return list(nodes)

    def _follow_edge(
        self,
        nodes: list[dict[str, Any]],
        edge: str,
    ) -> list[dict[str, Any]]:
        """Follow a single edge in the node list."""
        new_nodes = []
        for node in nodes:
            if edge in node:
                if isinstance(node[edge], list):
                    new_nodes.extend(node[edge])
                else:
                    new_nodes.append(node[edge])
        return new_nodes

    async def traverse(
        self,
        start_uid: str,
        edge_path: list[str],
        _filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Traverse the graph following a path of edges.

        Args:
            start_uid: Starting node UID
            edge_path: List of edge names to follow
            _filters: Optional filters to apply at each step

        Returns:
            List of nodes at the end of the path
        """
        dao = cast(DgraphLikeDAO, self)
        if not dao.client:
            msg = "Not connected to Dgraph"
            raise StorageError(msg)

        if not edge_path:
            msg = "edge_path cannot be empty"
            raise ValueError(msg)

        txn = dao.client.txn(read_only=True)
        try:
            query = self._build_traverse_query(start_uid, edge_path)
            response = txn.query(query)
            try:
                result = json.loads(response.json)
            except json.JSONDecodeError as e:
                msg = (
                    f"Failed to parse Dgraph response during traversal from {start_uid}"
                )
                raise StorageError(msg) from e
            return self._extract_traverse_nodes(result, edge_path)
        finally:
            txn.discard()
