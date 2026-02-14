"""Utility functions for Redis operations."""

import json
import logging
from datetime import datetime
from enum import Enum
from typing import Any, cast

logger = logging.getLogger(__name__)


def make_key(key_prefix: str, item_id: str) -> str:
    """Create Redis key with collection prefix."""
    return f"{key_prefix}{item_id}"


def make_metadata_key(key_prefix: str, item_id: str) -> str:
    """Create metadata key for an item."""
    return f"{key_prefix}meta:{item_id}"


def make_index_key(key_prefix: str, field: str, value: Any) -> str:
    """Create index key for field lookups."""
    return f"{key_prefix}idx:{field}:{value}"


def _json_serializer(obj: Any) -> str:
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return str(obj.value)
    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)


def serialize_data(data: dict[str, Any]) -> str:
    """Serialize data for Redis storage."""
    try:
        return json.dumps(data, default=_json_serializer)
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.exception("Failed to serialize data")
        msg = f"Data serialization failed: {e}"
        raise ValueError(msg) from e


def deserialize_data(data: str) -> dict[str, Any]:
    """Deserialize data from Redis."""
    try:
        return cast(dict[str, Any], json.loads(data))
    except (TypeError, ValueError, json.JSONDecodeError) as e:
        logger.exception("Failed to deserialize data")
        msg = f"Data deserialization failed: {e}"
        raise ValueError(msg) from e


async def create_indexes(
    client: Any,
    key_prefix: str,
    item_id: str,
    data: dict[str, Any],
    fields: list[str],
) -> None:
    """Create indexes for specified fields."""
    for field in fields:
        if field in data:
            index_key = make_index_key(key_prefix, field, data[field])
            await client.sadd(index_key, item_id)


async def update_indexes(
    client: Any,
    key_prefix: str,
    item_id: str,
    data: dict[str, Any],
    fields: list[str],
) -> None:
    """Update indexes for specified fields."""
    data_key = make_key(key_prefix, item_id)
    existing_data_str = await client.get(data_key)

    if existing_data_str:
        try:
            existing_data = deserialize_data(existing_data_str)
        except ValueError:
            logger.exception(
                "Failed to deserialize existing metadata for %s",
                item_id,
            )
            raise

        for field in fields:
            if field in existing_data and existing_data[field] != data.get(field):
                old_index_key = make_index_key(
                    key_prefix,
                    field,
                    existing_data[field],
                )
                await client.srem(old_index_key, item_id)

    for field in fields:
        if field in data:
            index_key = make_index_key(key_prefix, field, data[field])
            await client.sadd(index_key, item_id)


async def delete_indexes(
    client: Any,
    key_prefix: str,
    item_id: str,
) -> None:
    """Delete all index entries for an item."""
    pattern = f"{key_prefix}idx:*"
    async for key in client.scan_iter(match=pattern, count=100):
        await client.srem(key, item_id)
