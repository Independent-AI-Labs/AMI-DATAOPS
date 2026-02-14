"""PgVectorDAO -- Data Access Object for pgvector-backed models.

Extends the relational PostgreSQL DAO with vector-embedding support
(storage, retrieval, and similarity search).
"""

from __future__ import annotations

import logging
from typing import Any

import asyncpg

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageConnectionError, StorageError
from ami.implementations.embedding_service import get_embedding_service
from ami.implementations.vec import (
    pgvector_create,
    pgvector_delete,
    pgvector_read,
    pgvector_update,
    pgvector_vector,
)
from ami.implementations.vec.pgvector_util import (
    create_model_indexes,
    get_safe_table_name,
)
from ami.models.storage_config import StorageConfig

logger = logging.getLogger(__name__)

# Default embedding dimension (matches all-MiniLM-L6-v2)
_DEFAULT_EMBEDDING_DIM = 384

# Default fields to use for embedding text extraction
_DEFAULT_EMBEDDING_FIELDS: list[str] = [
    "title",
    "content",
    "description",
    "text",
    "name",
]


class PgVectorDAO(BaseDAO):
    """DAO implementation for PostgreSQL with pgvector extension.

    Supports full CRUD, vector similarity search, and automatic
    embedding generation on create / update.
    """

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        super().__init__(model_cls, config)
        self.pool: asyncpg.Pool | None = None
        self._ensured_tables: set[str] = set()

        # Embedding service setup
        self._embedding_service = get_embedding_service()
        self.embedding_dim: int = self._embedding_service.embedding_dim

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Establish a connection pool to PostgreSQL."""
        if self.pool is not None:
            return
        try:
            dsn = self._build_dsn()
            self.pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=10,
            )
            logger.info("Connected to pgvector at %s", self._safe_dsn())
        except Exception as exc:
            msg = f"Failed to connect to pgvector: {exc}"
            raise StorageConnectionError(msg) from exc

    async def disconnect(self) -> None:
        """Close the connection pool."""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            self._ensured_tables.clear()
            logger.info("Disconnected from pgvector")

    async def test_connection(self) -> bool:
        """Return *True* if a simple query succeeds."""
        try:
            await self._ensure_pool()
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
        except Exception:
            logger.warning("pgvector connection test failed")
            return False
        else:
            return True

    # ------------------------------------------------------------------
    # CRUD — delegated to sub-modules
    # ------------------------------------------------------------------

    async def create(self, instance: Any) -> str:
        await self._ensure_pool()
        return await pgvector_create.create(self, instance)

    async def find_by_id(self, item_id: str) -> Any | None:
        await self._ensure_pool()
        return await pgvector_read.find_by_id(self, item_id)

    async def find_one(self, query: dict[str, Any]) -> Any | None:
        await self._ensure_pool()
        return await pgvector_read.find_one(self, query)

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        await self._ensure_pool()
        return await pgvector_read.find(self, query, limit=limit, skip=skip)

    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        await self._ensure_pool()
        await pgvector_update.update(self, item_id, data)

    async def delete(self, item_id: str) -> bool:
        await self._ensure_pool()
        return await pgvector_delete.delete(self, item_id)

    async def count(self, query: dict[str, Any]) -> int:
        await self._ensure_pool()
        return await pgvector_read.count(self, query)

    async def exists(self, item_id: str) -> bool:
        await self._ensure_pool()
        return await pgvector_read.exists(self, item_id)

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    async def bulk_create(self, instances: list[Any]) -> list[str]:
        await self._ensure_pool()
        return await pgvector_create.bulk_create(self, instances)

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        await self._ensure_pool()
        await pgvector_update.bulk_update(self, updates)

    async def bulk_delete(self, ids: list[str]) -> dict[str, Any] | int:
        await self._ensure_pool()
        return await pgvector_delete.bulk_delete(self, ids)

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------

    async def create_indexes(self) -> None:
        """Create indexes defined in model metadata and auto-detected ones."""
        await self._ensure_pool()
        table = get_safe_table_name(self.collection_name)

        assert self.pool is not None
        async with self.pool.acquire() as conn:
            # Model-level indexes from metadata
            await create_model_indexes(conn, table, self.model_cls)

            # Create HNSW index on embedding column if it exists
            try:
                idx_name = f"idx_{table}_embedding_hnsw"
                await conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {idx_name} "
                    f"ON {table} USING hnsw "
                    f"(embedding vector_cosine_ops)",
                )
            except Exception:
                logger.debug(
                    "HNSW index creation skipped for %s "
                    "(table or column may not exist)",
                    table,
                )

    # ------------------------------------------------------------------
    # Raw queries
    # ------------------------------------------------------------------

    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_pool()
        return await pgvector_read.raw_read_query(self, query, params)

    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        await self._ensure_pool()
        return await pgvector_update.raw_write_query(self, query, params)

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[str]:
        await self._ensure_pool()
        return await pgvector_read.list_databases(self)

    async def list_schemas(
        self,
        database: str | None = None,
    ) -> list[str]:
        await self._ensure_pool()
        return await pgvector_read.list_schemas(self, database)

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        await self._ensure_pool()
        return await pgvector_read.list_models(self, database, schema)

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_pool()
        return await pgvector_read.get_model_info(self, path, database, schema)

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_pool()
        return await pgvector_read.get_model_schema(self, path, database, schema)

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_pool()
        return await pgvector_read.get_model_fields(self, path, database, schema)

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        await self._ensure_pool()
        return await pgvector_read.get_model_indexes(self, path, database, schema)

    # ------------------------------------------------------------------
    # Vector-specific operations
    # ------------------------------------------------------------------

    async def similarity_search(
        self,
        query_text: str,
        *,
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        """Search by semantic similarity to *query_text*."""
        await self._ensure_pool()
        return await pgvector_vector.similarity_search(
            self,
            query_text,
            limit=limit,
            filters=filters,
            metric=metric,
        )

    async def similarity_search_by_vector(
        self,
        embedding: list[float],
        *,
        limit: int = 10,
        filters: dict[str, Any] | None = None,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        """Search by a pre-computed embedding vector."""
        await self._ensure_pool()
        return await pgvector_vector.similarity_search_by_vector(
            self,
            embedding,
            limit=limit,
            filters=filters,
            metric=metric,
        )

    async def fetch_embedding(self, item_id: str) -> list[float] | None:
        """Retrieve the stored embedding for a record."""
        await self._ensure_pool()
        return await pgvector_vector.fetch_embedding(self, item_id)

    # ------------------------------------------------------------------
    # Embedding helpers
    # ------------------------------------------------------------------

    async def _get_query_embedding(self, text: str) -> list[float]:
        """Generate an embedding vector for a search query string."""
        return await self._embedding_service.generate_embedding(text)

    async def _generate_embedding_for_record(
        self,
        data: dict[str, Any],
    ) -> list[float] | None:
        """Generate an embedding from a record's text fields.

        Returns ``None`` if no embeddable text is found.
        """
        text = self._extract_text_for_embedding(data)
        if not text or not text.strip():
            return None
        try:
            return await self._embedding_service.generate_embedding(text)
        except Exception:
            logger.warning(
                "Embedding generation failed for record in %s",
                self.collection_name,
            )
            return None

    def _extract_text_for_embedding(self, data: dict[str, Any]) -> str:
        """Extract text from *data* using the default embedding fields.

        Uses a fixed list of well-known text field names since
        ``ModelMetadata`` does not carry an ``embedding_fields``
        attribute.
        """
        embedding_fields: list[str] = _DEFAULT_EMBEDDING_FIELDS

        parts: list[str] = []
        for field in embedding_fields:
            value = data.get(field)
            if value and isinstance(value, str):
                parts.append(value)

        return " ".join(parts)

    async def _fetch_embedding(self, item_id: str) -> list[float] | None:
        """Retrieve and parse the stored embedding for *item_id*.

        Handles the various types that asyncpg / pgvector may return:
        ``list``, ``tuple``, ``memoryview``, or ``str``.
        """
        table = get_safe_table_name(self.collection_name)
        sql = f"SELECT embedding FROM {table} WHERE uid = $1"

        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(sql, item_id)

        if row is None or row["embedding"] is None:
            return None

        raw: Any = row["embedding"]

        # Branch 1: already a Python sequence
        if isinstance(raw, list | tuple):
            result: list[float] = [float(v) for v in raw]
            return result

        # Branch 2: memoryview (some asyncpg builds)
        if isinstance(raw, memoryview):
            mv_result: list[float] = [float(v) for v in bytes(raw)]
            return mv_result

        # Branch 3: string literal e.g. "[0.1,0.2,0.3]"
        if isinstance(raw, str):
            import json

            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    str_result: list[float] = [float(v) for v in parsed]
                    return str_result
            except (json.JSONDecodeError, TypeError):
                pass

        logger.warning(
            "Unable to parse embedding of type %s for record %s",
            type(raw).__name__,
            item_id,
        )
        return None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    async def _ensure_pool(self) -> None:
        """Ensure the connection pool is available."""
        if self.pool is None:
            await self.connect()

    def _build_dsn(self) -> str:
        """Build a DSN string from the storage config."""
        if self.config is None:
            msg = "No storage config provided for PgVectorDAO"
            raise StorageError(msg)

        if self.config.connection_string:
            return self.config.connection_string

        return "postgresql://{}:{}@{}:{}/{}".format(
            self.config.username or "postgres",
            self.config.password or "",
            self.config.host or "localhost",
            self.config.port or 5432,
            self.config.database or "postgres",
        )

    def _safe_dsn(self) -> str:
        """Return a DSN with the password redacted for logging."""
        if self.config is None:
            return "<no config>"
        return "postgresql://{}:***@{}:{}/{}".format(
            self.config.username or "postgres",
            self.config.host or "localhost",
            self.config.port or 5432,
            self.config.database or "postgres",
        )
