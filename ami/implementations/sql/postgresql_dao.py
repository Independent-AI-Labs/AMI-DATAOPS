"""PostgreSQL Data Access Object implementation."""

import logging
from typing import TYPE_CHECKING, Any

import asyncpg

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageError
from ami.implementations.sql import (
    postgresql_create,
    postgresql_delete,
    postgresql_read,
    postgresql_update,
)
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

MIN_RESULT_PARTS = 2


class PostgreSQLDAO(BaseDAO):
    """PostgreSQL implementation of the BaseDAO interface.

    Delegates CRUD operations to the dedicated postgresql_* modules
    and manages the asyncpg connection pool lifecycle.
    """

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        super().__init__(model_cls, config)
        self.pool: asyncpg.Pool | None = None
        self._table_created: bool = False
        self._omit_columns: set[str] = {"storage_configs", "path"}

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Establish connection pool to PostgreSQL."""
        if self.pool is not None:
            return

        if self.config is None:
            msg = "StorageConfig is required to connect"
            raise StorageError(msg)

        try:
            dsn = self.config.get_connection_string()
            self.pool = await asyncpg.create_pool(
                dsn=dsn,
                min_size=1,
                max_size=10,
            )
            logger.info(
                "Connected to PostgreSQL for collection %s",
                self.collection_name,
            )
        except Exception as e:
            msg = "Failed to connect to PostgreSQL"
            raise StorageError(msg) from e

    async def disconnect(self) -> None:
        """Close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            logger.info(
                "Disconnected from PostgreSQL for collection %s",
                self.collection_name,
            )

    async def test_connection(self) -> bool:
        """Test if the connection is valid."""
        try:
            if not self.pool:
                await self.connect()
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return bool(result == 1)
        except Exception:
            logger.exception("PostgreSQL connection test failed")
            return False

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create(self, instance: Any) -> str:
        """Create a new record from a model instance."""
        if isinstance(instance, StorageModel):
            data = instance.to_storage_dict()
        elif isinstance(instance, dict):
            data = instance
        else:
            msg = "Instance must be a StorageModel or dict"
            raise StorageError(msg)

        return await postgresql_create.create(self, data)

    async def find_by_id(self, item_id: str) -> Any | None:
        """Find a record by its ID."""
        row = await postgresql_read.read(self, item_id)
        if row is None:
            return None
        return self._row_to_model(row)

    async def find_one(self, query: dict[str, Any]) -> Any | None:
        """Find a single record matching the query."""
        results = await postgresql_read.query(self, query)
        if not results:
            return None
        return self._row_to_model(results[0])

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        """Find multiple records matching the query."""
        results = await postgresql_read.query(self, query)
        if skip:
            results = results[skip:]
        if limit is not None:
            results = results[:limit]
        return [self._row_to_model(row) for row in results]

    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """Update a record by ID."""
        await postgresql_update.update(self, item_id, data)

    async def delete(self, item_id: str) -> bool:
        """Delete a record by ID."""
        return await postgresql_delete.delete(self, item_id)

    async def count(self, query: dict[str, Any]) -> int:
        """Count records matching the query."""
        return await postgresql_read.count(self, query or None)

    async def exists(self, item_id: str) -> bool:
        """Check if a record exists."""
        row = await postgresql_read.read(self, item_id)
        return row is not None

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    async def bulk_create(self, instances: list[Any]) -> list[str]:
        """Bulk insert multiple records."""
        ids: list[str] = []
        for instance in instances:
            record_id = await self.create(instance)
            ids.append(record_id)
        return ids

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Bulk update multiple records.

        Each dict must contain an ``id`` key and the fields to update.
        """
        for update_data in updates:
            item_id = update_data.get("id") or update_data.get("uid")
            if not item_id:
                msg = "Each update dict must contain an 'id' or 'uid' key"
                raise StorageError(msg)
            fields = {k: v for k, v in update_data.items() if k not in ("id", "uid")}
            await postgresql_update.update(self, str(item_id), fields)

    async def bulk_delete(self, ids: list[str]) -> dict[str, Any] | int:
        """Bulk delete multiple records."""
        deleted = 0
        for item_id in ids:
            if await postgresql_delete.delete(self, item_id):
                deleted += 1
        return deleted

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    async def create_indexes(self) -> None:
        """Create indexes defined in model metadata."""
        await postgresql_create.ensure_table_exists(self)

    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a raw read query."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                if params:
                    param_values = list(params.values())
                    rows = await conn.fetch(query, *param_values)
                else:
                    rows = await conn.fetch(query)
                return [dict(row) for row in rows]
            except Exception as e:
                msg = "Failed to execute raw read query"
                raise StorageError(msg) from e

    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a raw write query."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                if params:
                    param_values = list(params.values())
                    result = await conn.execute(query, *param_values)
                else:
                    result = await conn.execute(query)
            except Exception as e:
                msg = "Failed to execute raw write query"
                raise StorageError(msg) from e
            else:
                # asyncpg returns e.g. "DELETE 3" or "UPDATE 1"
                parts = result.split() if result else []
                if len(parts) >= MIN_RESULT_PARTS:
                    return int(parts[-1])
                return 0

    async def list_databases(self) -> list[str]:
        """List all databases."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(
                    "SELECT datname FROM pg_database "
                    "WHERE datistemplate = false ORDER BY datname",
                )
                return [row["datname"] for row in rows]
            except Exception as e:
                msg = "Failed to list databases"
                raise StorageError(msg) from e

    async def list_schemas(
        self,
        database: str | None = None,
    ) -> list[str]:
        """List all schemas."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT IN "
                    "('pg_catalog', 'information_schema', 'pg_toast') "
                    "ORDER BY schema_name",
                )
                return [row["schema_name"] for row in rows]
            except Exception as e:
                msg = "Failed to list schemas"
                raise StorageError(msg) from e

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List all tables in the given schema."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        target_schema = schema or "public"

        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = $1 ORDER BY table_name",
                    target_schema,
                )
                return [row["table_name"] for row in rows]
            except Exception as e:
                msg = "Failed to list tables"
                raise StorageError(msg) from e

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get information about a table."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                row_count = await conn.fetchval(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = $1",
                    path,
                )
                schema_info = await postgresql_read.get_model_schema(
                    self,
                    path,
                )
            except StorageError:
                raise
            except Exception as e:
                msg = f"Failed to get model info for {path}"
                raise StorageError(msg) from e
            else:
                return {
                    "name": path,
                    "estimated_rows": row_count or 0,
                    "schema": schema_info,
                }

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get schema information for a table."""
        return await postgresql_read.get_model_schema(self, path)

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get field information for a table."""
        schema_info = await postgresql_read.get_model_schema(self, path)
        fields: list[dict[str, Any]] = schema_info.get("fields", [])
        return fields

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get index information for a table."""
        if not self.pool:
            await self.connect()
        assert self.pool is not None

        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(
                    """
                    SELECT
                        i.relname AS index_name,
                        a.attname AS column_name,
                        ix.indisunique AS is_unique,
                        am.amname AS index_type
                    FROM pg_index ix
                    JOIN pg_class t ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_attribute a ON a.attrelid = t.oid
                        AND a.attnum = ANY(ix.indkey)
                    JOIN pg_am am ON am.oid = i.relam
                    WHERE t.relname = $1
                    ORDER BY i.relname
                    """,
                    path,
                )
                return [
                    {
                        "name": row["index_name"],
                        "column": row["column_name"],
                        "unique": row["is_unique"],
                        "type": row["index_type"],
                    }
                    for row in rows
                ]
            except Exception as e:
                msg = f"Failed to get indexes for {path}"
                raise StorageError(msg) from e

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _row_to_model(self, row: dict[str, Any]) -> Any:
        """Convert a database row dict to a model instance."""
        try:
            return self.model_cls.from_storage_dict(row)
        except Exception:
            logger.debug(
                "Could not hydrate row into %s, returning raw dict",
                self.model_cls.__name__,
            )
            return row
