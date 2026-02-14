"""High-level REST operation helpers.

Provides convenience wrappers around ``RestDAO`` for common patterns such
as paginated fetches, batch operations with progress, and authenticated
request helpers.
"""

from __future__ import annotations

import logging
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.rest.rest_dao import RestDAO
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig

logger = logging.getLogger(__name__)

# Sensible page-size defaults
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000


async def create_rest_dao(
    model_cls: type[StorageModel],
    config: StorageConfig,
    *,
    auto_connect: bool = True,
) -> RestDAO:
    """Factory helper: create and optionally connect a ``RestDAO``.

    Args:
        model_cls: The model class this DAO manages.
        config: Storage configuration for the REST endpoint.
        auto_connect: Whether to open the HTTP session immediately.

    Returns:
        A ready-to-use ``RestDAO`` instance.
    """
    dao = RestDAO(model_cls, config)
    if auto_connect:
        await dao.connect()
    return dao


async def paginated_fetch(
    dao: RestDAO,
    query: dict[str, Any] | None = None,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_records: int | None = None,
) -> list[Any]:
    """Fetch all matching records with automatic pagination.

    Args:
        dao: An active ``RestDAO`` instance.
        query: Filter query (passed as query-string parameters).
        page_size: Number of records per page.
        max_records: Stop after retrieving this many total records.

    Returns:
        A flat list of model instances.
    """
    if page_size > MAX_PAGE_SIZE:
        logger.warning(
            "Clamping page_size from %d to %d",
            page_size,
            MAX_PAGE_SIZE,
        )
        page_size = MAX_PAGE_SIZE

    query = query or {}
    all_results: list[Any] = []
    skip = 0

    while True:
        batch = await dao.find(query, limit=page_size, skip=skip)
        if not batch:
            break
        all_results.extend(batch)
        if max_records is not None and len(all_results) >= max_records:
            all_results = all_results[:max_records]
            break
        if len(batch) < page_size:
            break
        skip += page_size

    logger.debug("paginated_fetch returned %d records", len(all_results))
    return all_results


async def batch_create(
    dao: RestDAO,
    instances: list[Any],
    *,
    batch_size: int = DEFAULT_PAGE_SIZE,
    stop_on_error: bool = False,
) -> list[str]:
    """Create records in batches with optional error handling.

    Args:
        dao: An active ``RestDAO`` instance.
        instances: Model instances (or dicts) to create.
        batch_size: How many to send per batch.
        stop_on_error: If *True*, raise on first failure; otherwise skip.

    Returns:
        List of successfully created IDs.
    """
    created_ids: list[str] = []
    for i in range(0, len(instances), batch_size):
        batch = instances[i : i + batch_size]
        for inst in batch:
            try:
                uid = await dao.create(inst)
                created_ids.append(uid)
            except StorageError:
                if stop_on_error:
                    raise
                logger.warning(
                    "batch_create: skipping failed instance at index %d",
                    i + batch.index(inst),
                )
    logger.info(
        "batch_create completed: %d/%d created", len(created_ids), len(instances)
    )
    return created_ids


async def batch_update(
    dao: RestDAO,
    updates: list[dict[str, Any]],
    *,
    stop_on_error: bool = False,
) -> int:
    """Update records one-by-one with optional error tolerance.

    Each dict in *updates* must contain ``uid`` or ``id``.

    Returns:
        Number of successfully updated records.
    """
    success = 0
    for upd in updates:
        item_id = str(upd.get("uid") or upd.get("id", ""))
        if not item_id:
            logger.warning("batch_update: skipping entry without id")
            continue
        data = {k: v for k, v in upd.items() if k not in ("uid", "id")}
        try:
            await dao.update(item_id, data)
            success += 1
        except StorageError:
            if stop_on_error:
                raise
            logger.warning("batch_update: failed for id %s", item_id)
    logger.info("batch_update completed: %d/%d updated", success, len(updates))
    return success


async def batch_delete(
    dao: RestDAO,
    ids: list[str],
    *,
    stop_on_error: bool = False,
) -> int:
    """Delete records one-by-one with optional error tolerance.

    Returns:
        Number of successfully deleted records.
    """
    deleted = 0
    for item_id in ids:
        try:
            if await dao.delete(item_id):
                deleted += 1
        except StorageError:
            if stop_on_error:
                raise
            logger.warning("batch_delete: failed for id %s", item_id)
    logger.info("batch_delete completed: %d/%d deleted", deleted, len(ids))
    return deleted


async def upsert(
    dao: RestDAO,
    instance: Any,
) -> tuple[str, bool]:
    """Create or update a resource.

    Returns:
        Tuple of ``(uid, created)`` where *created* is ``True`` if the
        record was newly created.
    """
    if isinstance(instance, StorageModel):
        uid = instance.uid
    elif isinstance(instance, dict):
        uid = instance.get("uid") or instance.get("id")
    else:
        uid = getattr(instance, "uid", None) or getattr(instance, "id", None)

    if uid and await dao.exists(str(uid)):
        data = (
            instance.to_storage_dict()
            if isinstance(instance, StorageModel)
            else instance
            if isinstance(instance, dict)
            else instance.model_dump(mode="json")
        )
        await dao.update(str(uid), data)
        return str(uid), False

    new_id = await dao.create(instance)
    return new_id, True
