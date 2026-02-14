"""PgVector READ operations.

Supports find-by-id, find-one, filtered find with pagination,
count, exists checks, and raw read queries.
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any

from ami.implementations.vec.pgvector_util import (
    build_where_clause,
    deserialize_row,
    get_safe_table_name,
    is_valid_identifier,
)

if TYPE_CHECKING:
    from ami.implementations.vec.pgvector_dao import PgVectorDAO

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Single-record lookups
# ------------------------------------------------------------------


async def find_by_id(dao: PgVectorDAO, item_id: str) -> Any | None:
    """Return the record with *item_id*, or ``None``."""
    table = get_safe_table_name(dao.collection_name)
    sql = f"SELECT * FROM {table} WHERE uid = $1"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        row = await conn.fetchrow(sql, item_id)

    if row is None:
        return None
    return _row_to_model(dao, dict(row))


async def find_one(
    dao: PgVectorDAO,
    query: dict[str, Any],
) -> Any | None:
    """Return the first record matching *query*, or ``None``."""
    table = get_safe_table_name(dao.collection_name)

    if not query:
        sql = f"SELECT * FROM {table} LIMIT 1"
        params: list[Any] = []
    else:
        where, params = build_where_clause(query)
        sql = f"SELECT * FROM {table} WHERE {where} LIMIT 1"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)

    if row is None:
        return None
    return _row_to_model(dao, dict(row))


# ------------------------------------------------------------------
# Multi-record queries
# ------------------------------------------------------------------


async def find(
    dao: PgVectorDAO,
    query: dict[str, Any],
    limit: int | None = None,
    skip: int = 0,
) -> list[Any]:
    """Return records matching *query* with pagination."""
    table = get_safe_table_name(dao.collection_name)

    if query:
        where, params = build_where_clause(query)
        sql = f"SELECT * FROM {table} WHERE {where}"
    else:
        sql = f"SELECT * FROM {table}"
        params = []

    sql += " ORDER BY uid"

    if limit is not None:
        sql += f" LIMIT {limit}"
    if skip:
        sql += f" OFFSET {skip}"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    return [_row_to_model(dao, dict(r)) for r in rows]


# ------------------------------------------------------------------
# Aggregation helpers
# ------------------------------------------------------------------


async def count(
    dao: PgVectorDAO,
    query: dict[str, Any],
) -> int:
    """Return the number of records matching *query*."""
    table = get_safe_table_name(dao.collection_name)

    if query:
        where, params = build_where_clause(query)
        sql = f"SELECT COUNT(*) FROM {table} WHERE {where}"
    else:
        sql = f"SELECT COUNT(*) FROM {table}"
        params = []

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        result = await conn.fetchval(sql, *params)
    return int(result) if result else 0


async def exists(dao: PgVectorDAO, item_id: str) -> bool:
    """Return *True* if *item_id* exists in the table."""
    table = get_safe_table_name(dao.collection_name)
    sql = f"SELECT 1 FROM {table} WHERE uid = $1 LIMIT 1"

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        row = await conn.fetchval(sql, item_id)
    return row is not None


# ------------------------------------------------------------------
# Raw queries
# ------------------------------------------------------------------


async def raw_read_query(
    dao: PgVectorDAO,
    query: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Execute an arbitrary read query and return rows as dicts."""
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        if params:
            rows = await conn.fetch(query, *params.values())
        else:
            rows = await conn.fetch(query)
    return [dict(r) for r in rows]


# ------------------------------------------------------------------
# Schema introspection
# ------------------------------------------------------------------


async def list_databases(dao: PgVectorDAO) -> list[str]:
    """List all databases visible to the connection."""
    sql = "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [r["datname"] for r in rows]


async def list_schemas(
    dao: PgVectorDAO,
    database: str | None = None,
) -> list[str]:
    """List schemas in the current database."""
    sql = (
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name NOT IN ('pg_catalog', 'information_schema') "
        "ORDER BY schema_name"
    )
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [r["schema_name"] for r in rows]


async def list_models(
    dao: PgVectorDAO,
    database: str | None = None,
    schema: str | None = None,
) -> list[str]:
    """List tables in *schema* (default ``public``)."""
    target_schema = schema or "public"
    sql = (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = $1 ORDER BY table_name"
    )
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql, target_schema)
    return [r["table_name"] for r in rows]


async def get_model_info(
    dao: PgVectorDAO,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Return metadata about a table."""
    target_schema = schema or "public"
    if not is_valid_identifier(path):
        return {"error": "Invalid table name"}

    sql = (
        "SELECT table_name, table_type "
        "FROM information_schema.tables "
        "WHERE table_schema = $1 AND table_name = $2"
    )
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        row = await conn.fetchrow(sql, target_schema, path)

    if row is None:
        return {"error": "Table not found"}
    return {
        "name": row["table_name"],
        "type": row["table_type"],
        "schema": target_schema,
    }


async def get_model_schema(
    dao: PgVectorDAO,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Return column definitions for a table."""
    fields = await get_model_fields(dao, path, database, schema)
    return {"table": path, "fields": fields}


async def get_model_fields(
    dao: PgVectorDAO,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Return field information for a table."""
    target_schema = schema or "public"
    if not is_valid_identifier(path):
        return []

    sql = (
        "SELECT column_name, data_type, is_nullable, column_default "
        "FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "ORDER BY ordinal_position"
    )
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql, target_schema, path)

    return [
        {
            "name": r["column_name"],
            "type": r["data_type"],
            "nullable": r["is_nullable"] == "YES",
            "default": r["column_default"],
        }
        for r in rows
    ]


async def get_model_indexes(
    dao: PgVectorDAO,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Return index information for a table."""
    target_schema = schema or "public"
    if not is_valid_identifier(path):
        return []

    sql = (
        "SELECT indexname, indexdef "
        "FROM pg_indexes "
        "WHERE schemaname = $1 AND tablename = $2 "
        "ORDER BY indexname"
    )
    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        rows = await conn.fetch(sql, target_schema, path)

    return [{"name": r["indexname"], "definition": r["indexdef"]} for r in rows]


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _row_to_model(dao: PgVectorDAO, row: dict[str, Any]) -> Any:
    """Convert a database row dict into a model instance."""
    data = deserialize_row(row)

    # Strip embedding column from model hydration
    data.pop("embedding", None)

    # Attempt JSON-decode for any remaining string columns that look like JSON
    for key, value in list(data.items()):
        if isinstance(value, str) and value.startswith(("{", "[")):
            with contextlib.suppress(json.JSONDecodeError, TypeError):
                data[key] = json.loads(value)

    try:
        return dao.model_cls.from_storage_dict(data)
    except Exception:
        logger.debug("Falling back to raw dict for table %s", dao.collection_name)
        return data
