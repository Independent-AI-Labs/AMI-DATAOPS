"""Update operations for PostgreSQL DAO."""

import logging
from datetime import UTC, datetime
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.sql.postgresql_create import (
    convert_datetime_strings_for_timestamps,
    ensure_table_exists,
)
from ami.implementations.sql.postgresql_util import (
    get_safe_table_name,
    is_valid_identifier,
    serialize_value,
)

logger = logging.getLogger(__name__)


async def update(dao: Any, item_id: str, data: dict[str, Any]) -> None:
    """Update a record."""
    data = convert_datetime_strings_for_timestamps(data)
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)
    data["updated_at"] = datetime.now(UTC)

    async with dao.pool.acquire() as conn:
        set_clauses = []
        values = []
        param_count = 1

        for key, value in data.items():
            if key != "id" and is_valid_identifier(key):
                set_clauses.append(f"{key} = ${param_count + 1}")
                values.append(serialize_value(value))
                param_count += 1

        values.insert(0, item_id)

        # table_name is validated by get_safe_table_name()
        update_sql = f"""
            UPDATE {table_name}
            SET {", ".join(set_clauses)}
            WHERE id = $1
        """

        try:
            result = await conn.execute(update_sql, *values)
        except StorageError:
            raise
        except Exception as e:
            msg = f"Failed to update record: {e}"
            raise StorageError(msg) from e
        else:
            updated = result.split()[-1] == "1" if result else False
            if not updated:
                msg = f"Record not found: {item_id}"
                raise StorageError(msg)
