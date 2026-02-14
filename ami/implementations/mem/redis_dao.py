"""Redis DAO implementation for in-memory data and work queues."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

import redis.asyncio as redis
from redis.asyncio import Redis

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageError, StorageValidationError
from ami.implementations.mem import (
    redis_create,
    redis_delete,
    redis_inmem,
    redis_read,
    redis_update,
)
from ami.models.storage_config import StorageConfig

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class RedisDAO(BaseDAO):
    """Redis implementation for in-memory data and work queue storage."""

    DEFAULT_TTL: ClassVar[int] = 86400  # 24 hours

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        """Initialize Redis DAO."""
        super().__init__(model_cls, config)
        self.client: Redis | None = None
        self._key_prefix = f"{self.collection_name}:"

    async def connect(self) -> None:
        """Connect to Redis server."""
        if self.client:
            return
        if not self.config:
            msg = "Invalid Redis configuration"
            raise StorageError(msg)
        if not self.config.host:
            msg = "Redis host not configured"
            raise StorageError(msg)
        if not self.config.port:
            msg = "Redis port not configured"
            raise StorageError(msg)
        try:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=int(self.config.database or 0),
                decode_responses=True,
                max_connections=50,
            )
            ping_result = self.client.ping()
            if isinstance(ping_result, bool):
                pass  # Synchronous ping
            else:
                await ping_result
            logger.info(
                "Connected to Redis at %s:%s",
                self.config.host,
                self.config.port,
            )
        except Exception as e:
            logger.exception("Failed to connect to Redis")
            msg = f"Redis connection failed: {e}"
            raise StorageError(msg) from e

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self.client:
            try:
                await self.client.aclose()
                self.client = None
                logger.info("Disconnected from Redis")
            except Exception as e:
                logger.exception("Failed to disconnect from Redis")
                msg = f"Redis disconnection failed: {e}"
                raise StorageError(msg) from e

    # ---- CREATE ----

    async def create(self, instance: Any) -> str:
        """Create a new in-memory entry."""
        return await redis_create.create(self, instance)

    # ---- READ ----

    async def read(self, item_id: str) -> Any | None:
        """Read an in-memory entry by ID."""
        data = await redis_read.read(self, item_id)
        if data and self.model_cls:
            return self.model_cls(**data)
        return None

    async def query(
        self,
        filters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Query in-memory entries with filters."""
        return await redis_read.query(self, filters)

    async def list_all(self, limit: int = 100, offset: int = 0) -> list[Any]:
        """List all in-memory entries with pagination."""
        results = await redis_read.list_all(self, limit, offset)
        if self.model_cls:
            return [self.model_cls(**item) for item in results]
        return results

    async def count(self, query: dict[str, Any] | None = None) -> int:
        """Count in-memory entries matching filters."""
        return await redis_read.count(self, query)

    async def get_metadata(self, item_id: str) -> dict[str, Any] | None:
        """Get metadata for an in-memory entry."""
        return await redis_read.get_metadata(self, item_id)

    async def find_by_field(self, field: str, value: Any) -> list[Any]:
        """Find records by field value."""
        results = await self.query({field: value})
        if self.model_cls:
            return [self.model_cls(**item) for item in results]
        return results

    # ---- UPDATE ----

    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """Update an in-memory entry."""
        await redis_update.update(self, item_id, data)

    # ---- DELETE ----

    async def delete(self, item_id: str) -> bool:
        """Delete an in-memory entry."""
        return await redis_delete.delete(self, item_id)

    async def clear_collection(self) -> int:
        """Clear all entries in this collection."""
        return await redis_delete.clear_collection(self)

    async def find_by_id(self, item_id: str) -> Any | None:
        """Find in-memory entry by ID (alias for read)."""
        return await self.read(item_id)

    # ---- In-memory specific ----

    async def expire(self, item_id: str, ttl: int) -> bool:
        """Set TTL for an in-memory entry."""
        return await redis_inmem.expire(self, item_id, ttl)

    async def touch(self, item_id: str) -> bool:
        """Reset TTL for an in-memory entry."""
        return await redis_inmem.touch(self, item_id)

    # ---- Abstract method implementations ----

    async def find_one(self, query: dict[str, Any]) -> Any | None:
        """Find single record matching query."""
        results = await self.query(query)
        if results and self.model_cls:
            return self.model_cls(**results[0])
        return None

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        """Find multiple records matching query."""
        results = await self.query(query)
        if self.model_cls:
            return [self.model_cls(**item) for item in results]
        return []

    async def exists(self, item_id: str) -> bool:
        """Check if record exists."""
        result = await self.read(item_id)
        return result is not None

    async def bulk_create(self, instances: list[Any]) -> list[str]:
        """Bulk insert multiple records."""
        results = []
        failed = []
        for i, instance in enumerate(instances):
            try:
                result = await self.create(instance)
                results.append(result)
            except Exception as e:
                failed.append((i, str(e)))
        if failed:
            details = "; ".join(f"index {i}: {err}" for i, err in failed)
            msg = (
                f"Bulk create failed for {len(failed)}"
                f"/{len(instances)} instances: {details}"
            )
            raise StorageError(msg)
        return results

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Bulk update multiple records."""
        for i, update_item in enumerate(updates):
            if "id" not in update_item:
                msg = f"Update at index {i} missing required 'id' field"
                raise StorageValidationError(msg)
            item_id = update_item.pop("id")
            await self.update(item_id, update_item)

    async def bulk_delete(self, ids: list[str]) -> int:
        """Bulk delete multiple records."""
        deleted_count = 0
        failed = []
        for i, item_id in enumerate(ids):
            try:
                success = await self.delete(item_id)
                if success:
                    deleted_count += 1
                else:
                    failed.append((i, f"Delete returned False for id={item_id}"))
            except Exception as e:
                failed.append((i, str(e)))
        if failed:
            details = "; ".join(f"index {i}: {err}" for i, err in failed)
            msg = (
                f"Bulk delete failed for {len(failed)}/{len(ids)} instances: {details}"
            )
            raise StorageError(msg)
        return deleted_count

    async def create_indexes(self) -> None:
        """Create indexes defined in metadata (no-op for Redis)."""

    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute raw read command and return results as list of dicts."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        parts = query.split()
        command = parts[0].upper() if parts else ""
        if not command:
            return []
        if command not in ("GET", "KEYS", "INFO"):
            msg = (
                f"Unsupported Redis command for raw queries: "
                f"{command}. Use typed methods instead."
            )
            raise StorageError(msg)
        try:
            if command == "GET":
                if len(parts) > 1:
                    get_result = await self.client.get(parts[1])
                    return [{"value": get_result}] if get_result else []
                return []
            if command == "KEYS":
                pattern = parts[1] if len(parts) > 1 else "*"
                keys = await self.client.keys(pattern)
                return [{"key": key} for key in keys]
            # INFO
            info = await self.client.info()
            return [{"info": str(info)}]
        except StorageError:
            raise
        except Exception as e:
            msg = f"Raw read query failed: {e}"
            raise StorageError(msg) from e

    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute raw write command and return affected count."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        parts = query.split()
        command = parts[0].upper() if parts else ""
        set_min_args = 3  # SET key value
        del_min_args = 2  # DEL key
        if command not in ("SET", "DEL"):
            msg = (
                f"Unsupported Redis write command: {command}. "
                "Use typed methods instead."
            )
            raise StorageError(msg)
        try:
            if command == "SET" and len(parts) >= set_min_args:
                await self.client.set(parts[1], parts[2])
                result = 1
            elif command == "DEL" and len(parts) >= del_min_args:
                deleted = await self.client.delete(*parts[1:])
                result = int(deleted)
            else:
                result = 0
        except StorageError:
            raise
        except Exception as e:
            msg = f"Raw write query failed: {e}"
            raise StorageError(msg) from e
        else:
            return result

    async def list_databases(self) -> list[str]:
        """List all databases in Redis server."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        try:
            info = await self.client.info("keyspace")
            databases = []
            for line in str(info).split("\n"):
                if line.startswith("db"):
                    db_num = line.split(":")[0]
                    databases.append(db_num)
        except Exception as e:
            msg = f"Failed to list databases: {e}"
            raise StorageError(msg) from e
        else:
            return databases or ["db0"]

    async def list_schemas(self, database: str | None = None) -> list[str]:
        """List all namespaces (key prefixes) in current database."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        try:
            keys = await self.client.keys("*")
            prefixes = set()
            for key in keys:
                if ":" in key:
                    prefixes.add(key.split(":")[0])
        except Exception as e:
            msg = f"Failed to list schemas: {e}"
            raise StorageError(msg) from e
        else:
            return list(prefixes)

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List all key patterns in current database."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        try:
            pattern = f"{schema}:*" if schema else "*"
            keys = await self.client.keys(pattern)
            models = set()
            for key in keys:
                if ":" in key:
                    models.add(key.split(":")[0])
        except Exception as e:
            msg = f"Failed to list models: {e}"
            raise StorageError(msg) from e
        else:
            return list(models)

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get information about a key pattern."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        try:
            keys = await self.client.keys(f"{path}:*")
            key_type = None
            sample_key = keys[0] if keys else None
            if sample_key:
                key_type = await self.client.type(sample_key)
        except Exception as e:
            msg = f"Failed to get model info: {e}"
            raise StorageError(msg) from e
        else:
            return {
                "path": path,
                "key_count": len(keys),
                "key_type": key_type,
                "sample_keys": keys[:5],
            }

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get schema information for a key pattern."""
        return await self.get_model_info(path, database, schema)

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get field information for a key pattern."""
        if not self.client:
            msg = "Not connected to Redis"
            raise StorageError(msg)
        try:
            keys = await self.client.keys(f"{path}:*")
            fields: list[dict[str, str]] = []
            for key in keys[:5]:  # Sample first 5 keys
                key_type = await self.client.type(key)
                if key_type == "hash":
                    hkeys_result = self.client.hkeys(key)
                    hash_fields: list[Any] = (
                        await hkeys_result
                        if hasattr(hkeys_result, "__await__")
                        else hkeys_result
                    )
                    for field in hash_fields:
                        if not any(f["name"] == field for f in fields):
                            fields.append(
                                {
                                    "name": field,
                                    "type": "string",
                                    "key_example": key,
                                }
                            )
        except Exception as e:
            msg = f"Failed to get model fields: {e}"
            raise StorageError(msg) from e
        else:
            return fields

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get index information for a key pattern."""
        return []

    async def test_connection(self) -> bool:
        """Test if connection is valid."""
        if not self.client:
            msg = "Client not initialized"
            raise StorageError(msg)
        try:
            ping_result = self.client.ping()
            if isinstance(ping_result, bool):
                return ping_result
            await ping_result
        except Exception as e:
            msg = f"Redis connection test failed: {e}"
            raise StorageError(msg) from e
        else:
            return True
