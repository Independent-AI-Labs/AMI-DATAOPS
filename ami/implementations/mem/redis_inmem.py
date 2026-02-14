"""In-memory specific operations for Redis DAO."""

import logging
from datetime import UTC, datetime
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_util import make_key, make_metadata_key

logger = logging.getLogger(__name__)


async def expire(dao: Any, item_id: str, ttl: int) -> bool:
    """Set TTL for an in-memory entry."""
    if not dao.client:
        await dao.connect()

    key = make_key(dao._key_prefix, item_id)

    try:
        result = await dao.client.expire(key, ttl)
        if result:
            # Update metadata
            meta_key = make_metadata_key(dao._key_prefix, item_id)
            await dao.client.hset(meta_key, "ttl", str(ttl))
            logger.debug("Set TTL %ds for in-memory entry %s", ttl, item_id)
        return bool(result)
    except Exception as e:
        logger.exception("Failed to set TTL for %s", item_id)
        msg = f"Failed to set TTL: {e}"
        raise StorageError(msg) from e


async def touch(dao: Any, item_id: str) -> bool:
    """Reset TTL for an in-memory entry."""
    if not dao.client:
        await dao.connect()

    key = make_key(dao._key_prefix, item_id)
    meta_key = make_metadata_key(dao._key_prefix, item_id)

    try:
        # Get original TTL from metadata
        ttl_str = await dao.client.hget(meta_key, "ttl")
        if ttl_str:
            ttl = int(ttl_str)
            result = await dao.client.expire(key, ttl)
            if result:
                await dao.client.hset(
                    meta_key,
                    "last_touched",
                    datetime.now(UTC).isoformat(),
                )
                logger.debug("Reset TTL for in-memory entry %s", item_id)
            touch_result = bool(result)
        else:
            touch_result = False
    except Exception as e:
        logger.exception("Failed to touch in-memory entry %s", item_id)
        msg = f"Failed to touch in-memory entry: {e}"
        raise StorageError(msg) from e
    else:
        return touch_result
