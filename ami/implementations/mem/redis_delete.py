"""Delete operations for Redis DAO."""

import logging
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_util import (
    delete_indexes,
    make_key,
    make_metadata_key,
)

logger = logging.getLogger(__name__)


async def delete(dao: Any, item_id: str) -> bool:
    """Delete an in-memory entry."""
    if not dao.client:
        await dao.connect()

    key = make_key(dao._key_prefix, item_id)
    meta_key = make_metadata_key(dao._key_prefix, item_id)

    try:
        # Delete main key and metadata
        deleted = await dao.client.delete(key, meta_key)

        # Clean up indexes
        await delete_indexes(dao.client, dao._key_prefix, item_id)

        if deleted:
            logger.debug("Deleted in-memory entry %s", item_id)
        return bool(deleted)
    except Exception as e:
        logger.exception("Failed to delete in-memory entry %s", item_id)
        msg = f"Failed to delete in-memory entry: {e}"
        raise StorageError(msg) from e


async def clear_collection(dao: Any) -> int:
    """Clear all entries in this collection."""
    if not dao.client:
        await dao.connect()

    try:
        pattern = f"{dao._key_prefix}*"
        count = 0

        # Collect all keys to delete
        keys_to_delete = [
            key async for key in dao.client.scan_iter(match=pattern, count=100)
        ]

        # Delete in batches
        if keys_to_delete:
            count = await dao.client.delete(*keys_to_delete)
            logger.info(
                "Cleared %d entries from collection %s",
                count,
                dao.collection_name,
            )
    except Exception as e:
        logger.exception(
            "Failed to clear collection %s",
            dao.collection_name,
        )
        msg = f"Failed to clear collection: {e}"
        raise StorageError(msg) from e
    else:
        return count
