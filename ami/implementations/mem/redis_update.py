"""Update operations for Redis DAO."""

import logging
from datetime import UTC, datetime
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_read import read
from ami.implementations.mem.redis_util import (
    make_key,
    make_metadata_key,
    serialize_data,
    update_indexes,
)

logger = logging.getLogger(__name__)


def _prepare_data_with_ttl(
    data: dict[str, Any],
    item_id: str,
    default_ttl: int,
) -> dict[str, Any]:
    """Prepare data with required fields and TTL.

    Args:
        data: Data dictionary to prepare
        item_id: Item identifier
        default_ttl: Default TTL if not specified

    Returns:
        Updated data dictionary

    Raises:
        StorageError: If TTL is invalid
    """
    # Ensure ID and timestamps
    data["updated_at"] = datetime.now(UTC).isoformat()
    if "created_at" not in data:
        data["created_at"] = data["updated_at"]
    if "uid" not in data:
        data["uid"] = item_id

    # Explicit TTL resolution: data override, then existing, then default
    ttl = data.get("_ttl")
    if ttl is None:
        ttl = default_ttl

    if not isinstance(ttl, int | float) or ttl <= 0:
        msg = "Redis cache entries require a positive TTL"
        raise StorageError(msg)
    data["_ttl"] = int(ttl)

    return data


def _get_index_fields(data: dict[str, Any]) -> list[str]:
    """Determine which fields to index.

    Args:
        data: Data dictionary

    Returns:
        List of field names to index
    """
    if "_index_fields" in data:
        index_fields = data["_index_fields"]
        if isinstance(index_fields, list):
            return index_fields
        return []

    # Index all fields except special ones
    return [
        field
        for field in data
        if not field.startswith("_")
        and field not in ["created_at", "updated_at", "uid", "id"]
    ]


async def update(dao: Any, item_id: str, data: dict[str, Any]) -> None:
    """Update or insert (upsert) in-memory cache entry.

    Cache semantics: SET operations work whether entry exists or not.
    Cache misses due to TTL expiry are normal and should not raise errors.

    Raises:
        StorageError: If upsert operation fails
    """
    if not dao.client:
        await dao.connect()

    key = make_key(dao._key_prefix, item_id)

    try:
        # Try to read existing data for merge, but don't fail if missing
        try:
            existing_data = await read(dao, item_id)
        except Exception:
            # Cache miss - this is normal in cache-aside pattern
            existing_data = None

        if existing_data:
            # Merge with existing (preserve fields not in update)
            existing_data.update(data)
            data = existing_data
        else:
            # Cache miss - treat as new entry
            logger.debug(
                "Cache miss on update for %s - creating new cache entry",
                item_id,
            )

        # Prepare data with required fields and TTL
        data = _prepare_data_with_ttl(data, item_id, dao.DEFAULT_TTL)
        ttl = data["_ttl"]

        # Store (upsert) - works whether exists or not
        serialized = serialize_data(data)
        await dao.client.setex(key, int(ttl), serialized)

        # Update metadata
        meta_key = make_metadata_key(dao._key_prefix, item_id)
        await dao.client.hset(
            meta_key,
            mapping={
                "updated_at": data["updated_at"],
                "size": len(serialized),
                "ttl": int(ttl),
            },
        )
        await dao.client.expire(meta_key, int(ttl))

        # Update indexes
        index_fields = _get_index_fields(data)
        if index_fields:
            await update_indexes(
                dao.client,
                dao._key_prefix,
                item_id,
                data,
                index_fields,
            )

        logger.debug("Upserted cache entry %s", item_id)
    except Exception as e:
        logger.exception("Failed to upsert cache entry %s", item_id)
        msg = f"Failed to update cache: {e}"
        raise StorageError(msg) from e
