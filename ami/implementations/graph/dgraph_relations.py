"""Enhanced Dgraph DAO operations for relational graph management."""

import json
import logging
from typing import TYPE_CHECKING, Any, Protocol, cast

import pydgraph

from ami.core.exceptions import StorageError
from ami.core.graph_relations import GraphSchemaAnalyzer
from ami.models.base_model import StorageModel

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DgraphLikeDAO(Protocol):
    """Protocol for DAO-like objects that work with DgraphRelationalMixin."""

    client: pydgraph.DgraphClient | None
    collection_name: str
    model_cls: type[StorageModel]


class DgraphRelationalMixin:
    """Mixin for DgraphDAO to add relational capabilities."""

    def _prepare_node_data(
        self,
        instance_dict: dict[str, Any],
        schema: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Prepare node data and edges from instance."""
        node_data = {}
        edges_data = {}
        dao = cast(DgraphLikeDAO, self)

        for field_name, value in instance_dict.items():
            if value is None:
                continue

            if field_name in schema["edges"]:
                edge_config = schema["edges"][field_name]
                edges_data[field_name] = {
                    "value": value,
                    "config": edge_config,
                }
            elif field_name == "uid":
                node_data[f"{dao.collection_name}.uid"] = value
            else:
                node_data[f"{dao.collection_name}.{field_name}"] = value

        return node_data, edges_data

    def _add_relation_edges(
        self,
        relations: dict[str, Any] | None,
        schema: dict[str, Any],
        edges_data: dict[str, Any],
    ) -> None:
        """Add additional relation edges."""
        if not relations:
            return

        for field_name, related in relations.items():
            if field_name in schema["edges"]:
                edge_config = schema["edges"][field_name]
                edges_data[field_name] = {
                    "value": related,
                    "config": edge_config,
                }

    def _process_edge_value(
        self,
        value: Any,
        edge_name: str,
        node_data: dict[str, Any],
    ) -> None:
        """Process a single edge value."""
        if isinstance(value, list):
            node_data[edge_name] = [self._get_uid_reference(item) for item in value]
        elif value:
            node_data[edge_name] = self._get_uid_reference(value)

    def _get_uid_reference(self, item: Any) -> dict[str, str]:
        """Get UID reference for an item."""
        if isinstance(item, str):
            return {"uid": item}
        if hasattr(item, "graph_id"):
            return {"uid": item.graph_id}
        if hasattr(item, "uid"):
            return {"uid": f"_{item.uid}"}
        return {"uid": "_:unknown"}

    async def create_with_relations(
        self,
        instance: StorageModel,
        relations: dict[str, Any] | None = None,
    ) -> str:
        """Create a node with its relationships.

        Args:
            instance: The model instance to create
            relations: Dict of field_name -> related objects/IDs

        Returns:
            The UID of the created node
        """
        dao = cast(DgraphLikeDAO, self)
        if not dao.client:
            msg = "Not connected to Dgraph"
            raise StorageError(msg)

        # Analyze model for graph semantics
        schema = GraphSchemaAnalyzer.analyze_model(dao.model_cls)

        # Prepare node data
        instance_dict = instance.to_storage_dict()
        node_data, edges_data = self._prepare_node_data(instance_dict, schema)

        # Add type
        node_data["dgraph.type"] = dao.collection_name

        # Process additional relations
        self._add_relation_edges(relations, schema, edges_data)

        # Build mutation
        txn = dao.client.txn()
        try:
            # Create main node first
            node_data["uid"] = "_:newnode"

            # Add edges to the mutation
            for edge_info in edges_data.values():
                edge_name = edge_info["config"]["edge_name"]
                value = edge_info["value"]
                self._process_edge_value(value, edge_name, node_data)

            # Execute mutation
            mutation = pydgraph.Mutation(
                set_json=json.dumps(node_data, default=str).encode()
            )
            response = txn.mutate(mutation)
            txn.commit()

            # Get the created UID
            uid = response.uids.get("newnode")

        except Exception as e:
            msg = f"Failed to create with relations: {e}"
            raise StorageError(msg) from e
        else:
            if not uid:
                msg = "Failed to get UID from Dgraph"
                raise StorageError(msg)

            logger.debug(
                "Created node %s with %d edges",
                uid,
                len(edges_data),
            )
            return str(uid)
        finally:
            txn.discard()

    async def add_edge(
        self,
        from_uid: str,
        to_uid: str,
        edge_name: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Add an edge between two nodes.

        Args:
            from_uid: Source node UID
            to_uid: Target node UID
            edge_name: Name of the edge
            properties: Edge properties (requires intermediate node)

        Raises:
            StorageError: If the operation fails
        """
        dao = cast(DgraphLikeDAO, self)
        if not dao.client:
            msg = "Not connected to Dgraph"
            raise StorageError(msg)

        txn = dao.client.txn()
        try:
            if properties:
                # Create intermediate node for edge with properties
                edge_node = {
                    "uid": "_:edge",
                    "dgraph.type": f"{edge_name}_edge",
                }
                for k, v in properties.items():
                    edge_node[f"{edge_name}.{k}"] = v

                # Connect source -> edge_node -> target
                mutation_data: list[dict[str, Any]] = [
                    edge_node,
                    {
                        "uid": from_uid,
                        f"{edge_name}_via": {"uid": "_:edge"},
                    },
                    {
                        "uid": "_:edge",
                        f"{edge_name}_to": {"uid": to_uid},
                    },
                ]
            else:
                # Simple edge without properties
                mutation_data = [{"uid": from_uid, edge_name: {"uid": to_uid}}]

            mutation = pydgraph.Mutation(
                set_json=json.dumps(mutation_data, default=str).encode()
            )
            txn.mutate(mutation)
            txn.commit()

            logger.debug("Added edge %s from %s to %s", edge_name, from_uid, to_uid)

        except Exception as e:
            msg = f"Failed to add edge {edge_name} from {from_uid} to {to_uid}: {e}"
            raise StorageError(msg) from e
        finally:
            txn.discard()

    async def remove_edge(self, from_uid: str, to_uid: str, edge_name: str) -> None:
        """Remove an edge between two nodes.

        Args:
            from_uid: Source node UID
            to_uid: Target node UID
            edge_name: Name of the edge

        Raises:
            StorageError: If the operation fails
        """
        dao = cast(DgraphLikeDAO, self)
        if not dao.client:
            msg = "Not connected to Dgraph"
            raise StorageError(msg)

        txn = dao.client.txn()
        try:
            # Delete the edge
            delete_data = {"uid": from_uid, edge_name: {"uid": to_uid}}

            mutation = pydgraph.Mutation(delete_json=json.dumps(delete_data).encode())
            txn.mutate(mutation)
            txn.commit()

            logger.debug(
                "Removed edge %s from %s to %s",
                edge_name,
                from_uid,
                to_uid,
            )

        except Exception as e:
            msg = f"Failed to remove edge {edge_name} from {from_uid} to {to_uid}: {e}"
            raise StorageError(msg) from e
        finally:
            txn.discard()
