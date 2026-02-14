"""PgVector UPDATE operations."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ami.implementations.vec.pgvector_util import (
    get_safe_table_name,
    is_valid_identifier,
    serialize_value,
)

if TYPE_CHECKING:
    from ami.implementations.vec.pgvector_dao import PgVectorDAO

logger = logging.getLogger(__name__)


async def update(
    dao: PgVectorDAO,
    item_id: str,
    data: dict[str, Any],
) -> None:
    """Update the record identified by *item_id*."""
    if not data:
        return

    table = get_safe_table_name(dao.collection_name)

    # Inject updated_at timestamp
    data.setdefault("updated_at", datetime.now(UTC))

    # Re-generate embedding when text fields change
    embedding = await dao._generate_embedding_for_record(data)
    if embedding is not None:
        data["embedding"] = embedding

    set_parts: list[str] = []
    values: list[Any] = []
    param_idx = 1

    for key, value in data.items():
        if key == "uid" or not is_valid_identifier(key):
            continue
        set_parts.append(f"{key} = ${param_idx}")
        values.append(serialize_value(value))
        param_idx += 1

    if not set_parts:
        return

    values.append(item_id)
    set_clause = ", ".join(set_parts)
    sql = f"UPDATE {table} SET {set_clause} WHERE uid = ${param_idx}"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        await conn.execute(sql, *values)

    logger.debug("Updated record %s in %s", item_id, table)


async def bulk_update(
    dao: PgVectorDAO,
    updates: list[dict[str, Any]],
) -> None:
    """Apply multiple update operations.

    Each entry in *updates* must contain a ``uid`` key.
    """
    for entry in updates:
        uid = entry.get("uid")
        if not uid:
            logger.warning("Skipping update entry without uid")
            continue
        payload = {k: v for k, v in entry.items() if k != "uid"}
        await update(dao, uid, payload)


async def raw_write_query(
    dao: PgVectorDAO,
    query: str,
    params: dict[str, Any] | None = None,
) -> int:
    """Execute an arbitrary write query and return affected-row count."""
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        if params:
            result = await conn.execute(query, *params.values())
        else:
            result = await conn.execute(query)

    # asyncpg returns e.g. "UPDATE 3" — extract the count
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0
