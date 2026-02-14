"""Dgraph DAO implementation combining all modules."""

import logging
from typing import Any

import pydgraph

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageError
from ami.implementations.graph import (
    dgraph_create,
    dgraph_delete,
    dgraph_graph,
    dgraph_read,
    dgraph_update,
)
from ami.implementations.graph.dgraph_relations import (
    DgraphRelationalMixin,
)
from ami.implementations.graph.dgraph_traversal import (
    DgraphTraversalMixin,
)
from ami.implementations.graph.dgraph_util import ensure_schema
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig

logger = logging.getLogger(__name__)


class DgraphDAO(BaseDAO, DgraphRelationalMixin, DgraphTraversalMixin):
    """DAO implementation for Dgraph graph database."""

    def __init__(self, model_cls: type[StorageModel], config: StorageConfig):
        super().__init__(model_cls, config)
        self.client: pydgraph.DgraphClient | None = None
        self._grpc_client_conn: pydgraph.DgraphClientStub | None = None

    async def connect(self) -> None:
        """Establish connection to Dgraph."""
        if not self.config:
            msg = "No configuration provided for Dgraph connection"
            raise StorageError(msg)
        if not self.config.host:
            msg = "Dgraph host not configured"
            raise StorageError(msg)
        if not self.config.port:
            msg = "Dgraph port not configured"
            raise StorageError(msg)
        try:
            # Create gRPC client connection
            host = self.config.host
            port = self.config.port
            self._grpc_client_conn = pydgraph.DgraphClientStub(f"{host}:{port}")

            # Create client
            self.client = pydgraph.DgraphClient(self._grpc_client_conn)

            # Apply schema derived from the model metadata so index
            # definitions are honoured
            metadata = self.model_cls.get_metadata()
            ensure_schema(
                self.client,
                self.model_cls,
                metadata,
                self.collection_name,
            )

            logger.info("Connected to Dgraph at %s:%s", host, port)
        except Exception as e:
            msg = f"Failed to connect to Dgraph: {e}"
            raise StorageError(msg) from e

    async def disconnect(self) -> None:
        """Close connection to Dgraph."""
        if self._grpc_client_conn:
            self._grpc_client_conn.close()
            self.client = None
            logger.info("Disconnected from Dgraph")

    # CREATE operations
    async def create(self, instance: StorageModel) -> str:
        """Create new record, return ID."""
        return await dgraph_create.create(self, instance)

    async def bulk_create(self, instances: list[StorageModel]) -> list[str]:
        """Bulk insert multiple records."""
        return await dgraph_create.bulk_create(self, instances)

    async def create_indexes(self) -> None:
        """Create indexes defined in metadata."""
        await dgraph_create.create_indexes(self)

    # READ operations
    async def find_by_id(self, item_id: str) -> StorageModel | None:
        """Find record by ID."""
        return await dgraph_read.find_by_id(self, item_id)

    async def find_one(self, query: dict[str, Any]) -> StorageModel | None:
        """Find single record matching query."""
        return await dgraph_read.find_one(self, query)

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[StorageModel]:
        """Find multiple records matching query."""
        return await dgraph_read.find(self, query, limit, skip)

    async def count(self, query: dict[str, Any]) -> int:
        """Count records matching query."""
        return await dgraph_read.count(self, query)

    async def exists(self, item_id: str) -> bool:
        """Check if record exists."""
        return await dgraph_read.exists(self, item_id)

    async def raw_read_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute raw read query and return results as list of dicts."""
        return await dgraph_read.raw_read_query(self, query, params)

    async def list_databases(self) -> list[str]:
        """List all databases/namespaces/buckets in storage."""
        return await dgraph_read.list_databases(self)

    async def list_schemas(self, database: str | None = None) -> list[str]:
        """List all schemas/collections/directories in a database."""
        return await dgraph_read.list_schemas(self, database)

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List all models in current storage."""
        return await dgraph_read.list_models(self, database, schema)

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get information about a model."""
        return await dgraph_read.get_model_info(self, path, database, schema)

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get schema/structure information for a model."""
        return await dgraph_read.get_model_schema(self, path, database, schema)

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get field/property/attribute information for a model."""
        return await dgraph_read.get_model_fields(self, path, database, schema)

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get index information for a model."""
        return await dgraph_read.get_model_indexes(self, path, database, schema)

    async def test_connection(self) -> bool:
        """Test if connection is valid."""
        return await dgraph_read.test_connection(self)

    # UPDATE operations
    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """Update record by ID."""
        return await dgraph_update.update(self, item_id, data)

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Bulk update multiple records."""
        return await dgraph_update.bulk_update(self, updates)

    async def raw_write_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> int:
        """Execute raw write query and return affected rows."""
        return await dgraph_update.raw_write_query(self, query, params)

    # DELETE operations
    async def delete(self, item_id: str) -> bool:
        """Delete record by ID."""
        return await dgraph_delete.delete(self, item_id)

    async def bulk_delete(self, ids: list[str]) -> dict[str, Any]:
        """Bulk delete multiple records."""
        return await dgraph_delete.bulk_delete(self, ids)

    # Graph-specific operations
    async def k_hop_query(
        self,
        start_id: str,
        k: int,
        edge_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """Perform k-hop graph traversal from a starting node."""
        return await dgraph_graph.k_hop_query(self, start_id, k, edge_types)

    async def shortest_path(
        self,
        start_id: str,
        end_id: str,
        max_depth: int = 10,
    ) -> list[str]:
        """Find shortest path between two nodes."""
        return await dgraph_graph.shortest_path(self, start_id, end_id, max_depth)

    async def find_connected_components(
        self, node_type: str | None = None
    ) -> list[list[str]]:
        """Find all connected components in the graph."""
        return await dgraph_graph.find_connected_components(self, node_type)

    async def get_node_degree(
        self, node_id: str, direction: str = "all"
    ) -> dict[str, int]:
        """Get degree of a node (in-degree, out-degree, or total)."""
        return await dgraph_graph.get_node_degree(self, node_id, direction)
