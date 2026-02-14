"""Delete operations for PostgreSQL DAO."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ami.core.exceptions import StorageError
from ami.implementations.sql.postgresql_create import (
    ensure_table_exists,
)
from ami.implementations.sql.postgresql_util import (
    get_safe_table_name,
)

if TYPE_CHECKING:
    from ami.implementations.sql.postgresql_dao import PostgreSQLDAO

logger = logging.getLogger(__name__)


async def delete(dao: PostgreSQLDAO, item_id: str) -> bool:
    """Delete a record."""
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()
    assert dao.pool is not None

    table_name = get_safe_table_name(dao.collection_name)

    async with dao.pool.acquire() as conn:
        try:
            # table_name validated by get_safe_table_name()
            result = await conn.execute(
                f"DELETE FROM {table_name} WHERE id = $1",
                item_id,
            )
            deleted = result.split()[-1] == "1" if result else False
            return bool(deleted)
        except Exception as e:
            msg = f"Failed to delete record: {e}"
            raise StorageError(msg) from e
