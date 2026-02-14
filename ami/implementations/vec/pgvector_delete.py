"""PgVector DELETE operations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ami.implementations.vec.pgvector_util import get_safe_table_name

if TYPE_CHECKING:
    from ami.implementations.vec.pgvector_dao import PgVectorDAO

logger = logging.getLogger(__name__)


async def delete(dao: PgVectorDAO, item_id: str) -> bool:
    """Delete the record identified by *item_id*. Return success flag."""
    table = get_safe_table_name(dao.collection_name)
    sql = f"DELETE FROM {table} WHERE uid = $1"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        result = await conn.execute(sql, item_id)

    deleted = result and result.endswith("1")
    if deleted:
        logger.debug("Deleted record %s from %s", item_id, table)
    else:
        logger.debug("Record %s not found in %s for deletion", item_id, table)
    return bool(deleted)


async def bulk_delete(
    dao: PgVectorDAO,
    ids: list[str],
) -> dict[str, Any] | int:
    """Delete multiple records by *ids*. Return count of deleted rows."""
    if not ids:
        return 0

    table = get_safe_table_name(dao.collection_name)
    params = ", ".join(f"${i + 1}" for i in range(len(ids)))
    sql = f"DELETE FROM {table} WHERE uid IN ({params})"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        result = await conn.execute(sql, *ids)

    try:
        return int(result.split()[-1])
    except (ValueError, IndexError, AttributeError):
        return 0
