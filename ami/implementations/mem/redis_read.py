"""Read operations for Redis DAO."""

import logging
from datetime import UTC, datetime
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.mem.redis_util import (
    deserialize_data,
    make_index_key,
    make_key,
    make_metadata_key,
)

logger = logging.getLogger(__name__)


async def read(dao: Any, item_id: str) -> dict[str, Any] | None:
    """Read an in-memory entry by ID.

    Returns:
        dict[str, Any]: The entry data if found
        None: Only when entry does not exist (not found in Redis)

    Raises:
        StorageError: On any Redis operation failure or data corruption
    """
    if not dao.client:
        await dao.connect()

    key = make_key(dao._key_prefix, item_id)

    try:
        data = await dao.client.get(key)
    except Exception as e:
        logger.exception("Failed to read key %s from Redis", key)
        msg = f"Failed to read in-memory entry {item_id}: {e}"
        raise StorageError(msg) from e

    if not data:
        return None

    try:
        result = deserialize_data(data)
    except ValueError as e:
        logger.exception("Failed to deserialize data for %s", item_id)
        msg = f"Data corruption in entry {item_id}: {e}"
        raise StorageError(msg) from e

    # Update access metadata (non-critical operation)
    meta_key = make_metadata_key(dao._key_prefix, item_id)
    try:
        await dao.client.hset(
            meta_key,
            "last_accessed",
            datetime.now(UTC).isoformat(),
        )
    except Exception as meta_err:
        logger.warning(
            "Failed to update access metadata for %s: %s",
            item_id,
            meta_err,
        )
        # Non-critical failure, continue with read result

    return result


async def _query_with_filters(
    dao: Any,
    filters: dict[str, Any],
) -> list[dict[str, Any]]:
    """Query entries using index filters.

    Args:
        dao: DAO instance with Redis client
        filters: Field-value pairs to filter by

    Returns:
        List of matching entries (may be partial if individual reads fail)

    Raises:
        StorageError: If index lookup fails (individual entry read failures
            are logged but don't fail the query)
    """
    matching_ids: set[Any] = set()
    first_filter = True

    for field, value in filters.items():
        index_key = make_index_key(dao._key_prefix, field, value)
        try:
            field_ids = await dao.client.smembers(index_key)
        except Exception as e:
            logger.exception("Failed to read index %s", index_key)
            msg = f"Failed to read index for {field}={value}: {e}"
            raise StorageError(msg) from e

        if first_filter:
            matching_ids = set(field_ids)
            first_filter = False
        else:
            matching_ids &= set(field_ids)

    # Read matching entries (failures in individual reads are logged
    # but don't fail the entire query)
    results = []
    for item_id in matching_ids:
        try:
            data = await read(dao, item_id)
            if data:
                results.append(data)
        except StorageError:
            logger.exception(
                "Failed to read entry %s during query",
                item_id,
            )
            # Skip failed reads and continue - partial results are acceptable

    return results


async def _query_all(dao: Any) -> list[dict[str, Any]]:
    """Query all entries in collection.

    Args:
        dao: DAO instance with Redis client

    Returns:
        List of all entries (may be partial if individual reads fail)

    Note:
        Individual read failures are logged but don't fail the entire query.
        Returns partial results if some entries can't be read.
    """
    pattern = f"{dao._key_prefix}*"
    keys = [
        key
        async for key in dao.client.scan_iter(match=pattern, count=100)
        if ":meta:" not in key and ":idx:" not in key
    ]

    results = []
    for key in keys:
        try:
            data = await dao.client.get(key)
        except Exception:
            logger.exception("Failed to get data for key %s", key)
            # Skip failed reads and continue with other entries
            continue

        if not data:
            continue

        try:
            results.append(deserialize_data(data))
        except ValueError:
            logger.exception(
                "Failed to deserialize data for key %s",
                key,
            )
            # Skip corrupted entries and continue

    return results


async def query(
    dao: Any,
    filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Query in-memory entries with filters."""
    if not dao.client:
        await dao.connect()

    try:
        if filters:
            return await _query_with_filters(dao, filters)
        return await _query_all(dao)
    except Exception as e:
        logger.exception("Failed to query in-memory entries")
        msg = f"Failed to query in-memory entries: {e}"
        raise StorageError(msg) from e


async def list_all(
    dao: Any,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List all in-memory entries with pagination.

    Args:
        dao: DAO instance with Redis client
        limit: Maximum number of entries to return
        offset: Number of entries to skip

    Returns:
        List of entries (may be partial if individual reads fail)

    Raises:
        StorageError: If key scanning fails (individual entry read failures
            are logged but don't fail the list)
    """
    if not dao.client:
        await dao.connect()

    try:
        # Get all keys matching pattern
        pattern = f"{dao._key_prefix}*"
        all_keys = [
            key
            async for key in dao.client.scan_iter(match=pattern, count=100)
            if ":meta:" not in key and ":idx:" not in key
        ]

        # Apply pagination
        paginated_keys = all_keys[offset : offset + limit]

        # Read entries (individual failures are logged but don't fail
        # the entire list operation)
        results = []
        for key in paginated_keys:
            try:
                data = await dao.client.get(key)
            except Exception:
                logger.exception(
                    "Failed to get data for key %s",
                    key,
                )
                # Skip failed reads and continue with other entries
                continue

            if not data:
                continue

            try:
                results.append(deserialize_data(data))
            except ValueError:
                logger.exception(
                    "Failed to deserialize data for key %s",
                    key,
                )
                # Skip corrupted entries and continue
    except Exception as e:
        logger.exception("Failed to list in-memory entries")
        msg = f"Failed to list in-memory entries: {e}"
        raise StorageError(msg) from e
    else:
        return results


async def count(
    dao: Any,
    filters: dict[str, Any] | None = None,
) -> int:
    """Count in-memory entries matching filters."""
    if not dao.client:
        await dao.connect()

    try:
        if filters:
            # Use indexes to count matching entries
            matching_ids: set[Any] = set()
            first_filter = True

            for field, value in filters.items():
                index_key = make_index_key(dao._key_prefix, field, value)
                try:
                    field_ids = await dao.client.smembers(index_key)
                except Exception as idx_err:
                    logger.exception(
                        "Failed to read index %s",
                        index_key,
                    )
                    msg = f"Failed to read index for {field}={value}: {idx_err}"
                    raise StorageError(msg) from idx_err

                if first_filter:
                    matching_ids = set(field_ids)
                    first_filter = False
                else:
                    matching_ids &= set(field_ids)

            count_result = len(matching_ids)
        else:
            # Count all entries in collection
            pattern = f"{dao._key_prefix}*"
            count_result = 0
            async for key in dao.client.scan_iter(match=pattern, count=100):
                if ":meta:" not in key and ":idx:" not in key:
                    count_result += 1
    except Exception as e:
        logger.exception("Failed to count in-memory entries")
        msg = f"Failed to count in-memory entries: {e}"
        raise StorageError(msg) from e
    else:
        return count_result


async def get_metadata(dao: Any, item_id: str) -> dict[str, Any] | None:
    """Get metadata for an in-memory entry."""
    if not dao.client:
        await dao.connect()

    meta_key = make_metadata_key(dao._key_prefix, item_id)

    try:
        metadata = await dao.client.hgetall(meta_key)
        return dict(metadata) if metadata else None
    except Exception as e:
        logger.exception("Failed to get metadata for %s", item_id)
        msg = f"Failed to get metadata: {e}"
        raise StorageError(msg) from e
