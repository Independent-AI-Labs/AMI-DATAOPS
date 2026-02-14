"""Read operations for PostgreSQL DAO."""

import logging
from typing import Any

from asyncpg.exceptions import UndefinedTableError

from ami.core.exceptions import StorageError
from ami.implementations.sql.postgresql_create import (
    ensure_table_exists,
)
from ami.implementations.sql.postgresql_util import (
    build_where_clause,
    deserialize_row,
    get_safe_table_name,
)

logger = logging.getLogger(__name__)


async def read(dao: Any, item_id: str) -> dict[str, Any] | None:
    """Read a record by ID."""
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)

    async with dao.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {table_name} WHERE id = $1",
                item_id,
            )
        except UndefinedTableError as e:
            msg = f"Table does not exist: {table_name}"
            raise StorageError(msg) from e
        except Exception as e:
            msg = f"Failed to read record: {e}"
            raise StorageError(msg) from e
        else:
            if row:
                result = deserialize_row(dict(row))
                if "uid" not in result and "id" in result:
                    result["uid"] = result["id"]
                return result
            return None


async def query(
    dao: Any,
    filters: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Query records with filters."""
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)

    async with dao.pool.acquire() as conn:
        try:
            if filters:
                where_clause, values = build_where_clause(filters)
                if where_clause:
                    query_sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
                    rows = await conn.fetch(query_sql, *values)
                else:
                    rows = await conn.fetch(f"SELECT * FROM {table_name}")
            else:
                rows = await conn.fetch(f"SELECT * FROM {table_name}")

            return [deserialize_row(dict(row)) for row in rows]
        except UndefinedTableError as e:
            msg = f"Table does not exist: {table_name}"
            raise StorageError(msg) from e
        except Exception as e:
            msg = f"Failed to query records: {e}"
            raise StorageError(msg) from e


async def list_all(
    dao: Any,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List all records with pagination."""
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)

    async with dao.pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                f"SELECT * FROM {table_name} "
                "ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                limit,
                offset,
            )
            return [deserialize_row(dict(row)) for row in rows]
        except UndefinedTableError as e:
            msg = f"Table does not exist: {table_name}"
            raise StorageError(msg) from e
        except Exception as e:
            msg = f"Failed to list records: {e}"
            raise StorageError(msg) from e


async def count(
    dao: Any,
    filters: dict[str, Any] | None = None,
) -> int:
    """Count records matching filters."""
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)

    async with dao.pool.acquire() as conn:
        try:
            if filters:
                where_clause, values = build_where_clause(filters)
                if where_clause:
                    count_sql = (
                        f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"
                    )
                    result = await conn.fetchval(count_sql, *values)
                else:
                    result = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table_name}",
                    )
            else:
                result = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table_name}",
                )
        except UndefinedTableError as e:
            msg = f"Table does not exist: {table_name}"
            raise StorageError(msg) from e
        except (TypeError, ValueError) as e:
            msg = f"Invalid count value: {e}"
            raise StorageError(msg) from e
        except StorageError:
            raise
        except Exception as e:
            msg = f"Failed to count records: {e}"
            raise StorageError(msg) from e
        else:
            if result is None:
                msg = f"COUNT query returned None for table {table_name}"
                raise StorageError(msg)
            return int(result)


async def get_model_schema(
    dao: Any,
    table_name: str,
) -> dict[str, Any]:
    """Get table schema information."""
    if not dao.pool:
        await dao.connect()

    try:
        async with dao.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
                """,
                table_name,
            )
    except StorageError:
        raise
    except Exception as e:
        msg = f"Failed to get table schema: {e}"
        raise StorageError(msg) from e

    if not rows:
        msg = f"Table schema not found: {table_name}"
        raise StorageError(msg)

    fields = [
        {
            "name": row["column_name"],
            "type": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
            "default": row["column_default"],
        }
        for row in rows
    ]
    return {"fields": fields, "table_name": table_name}
