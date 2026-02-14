"""Create operations for Redis DAO."""

import logging
from datetime import UTC, datetime
from typing import Any

from uuid_utils import uuid7

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_util import (
    create_indexes,
    make_key,
    make_metadata_key,
    serialize_data,
)
from ami.models.base_model import StorageModel

logger = logging.getLogger(__name__)


def _normalize_data(data: dict[str, Any] | Any) -> dict[str, Any]:
    """Normalize data to dict format."""
    if isinstance(data, StorageModel):
        return data.model_dump(mode="json", exclude_none=True)
    if not isinstance(data, dict):
        # Try to convert to dict if it's a Pydantic model
        if hasattr(data, "model_dump"):
            result = data.model_dump(mode="json", exclude_none=True)
            if isinstance(result, dict):
                return result
        msg = f"Cannot create from type {type(data)}"
        raise ValueError(msg)
    return data


def _ensure_id_and_timestamps(data: dict[str, Any]) -> tuple[str, str]:
    """Ensure data has ID and timestamps, return item_id and key."""
    # Generate UID if not provided (StorageModel uses uid, not id)
    if "uid" not in data and "id" not in data:
        data["uid"] = uuid7()

    # Get the ID field (prefer uid over id)
    item_id_raw = data.get("uid") or data.get("id")
    if not item_id_raw:
        msg = "Instance must have uid or id"
        raise ValueError(msg)

    item_id = str(item_id_raw)

    # Add timestamps
    now = datetime.now(UTC)
    data["created_at"] = now.isoformat()
    data["updated_at"] = now.isoformat()

    return item_id, item_id


async def _store_data_and_metadata(
    dao: Any,
    data: dict[str, Any],
    item_id: str,
) -> None:
    """Store main data and metadata in Redis."""
    key = make_key(dao._key_prefix, item_id)
    serialized = serialize_data(data)
    if "_ttl" not in data:
        msg = "Redis DAO cache entries must define a TTL (_ttl field required)"
        raise StorageError(msg)
    ttl = int(data["_ttl"])
    if ttl <= 0:
        msg = "Redis DAO cache entries must define a positive TTL"
        raise StorageError(msg)

    # Store main data
    try:
        await dao.client.setex(key, ttl, serialized)
    except Exception as e:
        msg = f"Failed to store data in Redis for key {key}"
        raise StorageError(msg) from e

    # Store metadata - if this fails, clean up main data
    metadata = {
        "created_at": data["created_at"],
        "updated_at": data["updated_at"],
        "ttl": ttl,
        "size": len(serialized),
    }
    meta_key = make_metadata_key(dao._key_prefix, item_id)
    try:
        await dao.client.hset(meta_key, mapping=metadata)
        await dao.client.expire(meta_key, ttl)
    except Exception as e:
        # Clean up main data if metadata storage fails
        cleanup_error = None
        try:
            await dao.client.delete(key)
        except Exception as cleanup_e:
            cleanup_error = cleanup_e

        # Raise with cleanup failure context if cleanup also failed
        if cleanup_error:
            msg = (
                f"Failed to store metadata in Redis for key {meta_key}, "
                f"and cleanup of key {key} also failed: {cleanup_error}"
            )
            raise StorageError(msg) from e
        msg = f"Failed to store metadata in Redis for key {meta_key}"
        raise StorageError(msg) from e


async def _create_indexes(
    dao: Any,
    data: dict[str, Any],
    item_id: str,
) -> None:
    """Create indexes for the data."""
    try:
        if "_index_fields" in data:
            await create_indexes(
                dao.client,
                dao._key_prefix,
                item_id,
                data,
                data["_index_fields"],
            )
        else:
            # Index all fields except special ones
            index_fields = [
                field
                for field in data
                if not field.startswith("_")
                and field not in ["created_at", "updated_at", "uid", "id"]
            ]
            if index_fields:
                await create_indexes(
                    dao.client,
                    dao._key_prefix,
                    item_id,
                    data,
                    index_fields,
                )
    except Exception as e:
        msg = f"Failed to create indexes for item {item_id}"
        raise StorageError(msg) from e


async def create(dao: Any, data: dict[str, Any] | Any) -> str:
    """Create a new in-memory entry."""
    if not dao.client:
        await dao.connect()

    data = _normalize_data(data)
    # Explicit TTL check - must be provided, no implicit defaults
    if "ttl" in data:
        ttl = data["ttl"]
    elif "_ttl" in data:
        ttl = data["_ttl"]
    else:
        msg = (
            "Redis DAO cache entries must explicitly define "
            "a TTL (use 'ttl' or '_ttl' field)"
        )
        raise StorageError(msg)
    if not isinstance(ttl, int | float) or ttl <= 0:
        msg = f"Redis DAO requires a positive TTL for cache entries, got: {ttl}"
        raise StorageError(msg)
    data["_ttl"] = int(ttl)
    item_id, _key_id = _ensure_id_and_timestamps(data)

    await _store_data_and_metadata(dao, data, item_id)
    await _create_indexes(dao, data, item_id)

    logger.debug("Created in-memory entry %s", item_id)
    return item_id
