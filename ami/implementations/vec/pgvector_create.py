"""PgVector CREATE operations."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from ami.implementations.vec.pgvector_util import (
    get_safe_table_name,
    infer_column_type,
    serialize_value,
)
from ami.models.base_model import StorageModel

if TYPE_CHECKING:
    from ami.implementations.vec.pgvector_dao import PgVectorDAO

logger = logging.getLogger(__name__)


async def create(dao: PgVectorDAO, instance: Any) -> str:
    """Insert a single record and return its UID."""
    data = _prepare_data(dao, instance)
    uid = data.get("uid") or str(uuid7())
    data["uid"] = uid

    table = get_safe_table_name(dao.collection_name)
    await _ensure_table(dao, table, data)

    # Generate embedding if applicable
    embedding = await dao._generate_embedding_for_record(data)
    if embedding is not None:
        data["embedding"] = embedding

    columns = list(data.keys())
    params = ", ".join(f"${i + 1}" for i in range(len(columns)))
    col_list = ", ".join(columns)

    sql = f"INSERT INTO {table} ({col_list}) VALUES ({params})"
    values = [serialize_value(data[c]) for c in columns]

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        await conn.execute(sql, *values)

    logger.debug("Created record %s in %s", uid, table)
    return uid


async def bulk_create(dao: PgVectorDAO, instances: list[Any]) -> list[str]:
    """Insert multiple records and return their UIDs."""
    if not instances:
        return []

    uids: list[str] = []
    for inst in instances:
        uid = await create(dao, inst)
        uids.append(uid)
    return uids


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _prepare_data(dao: PgVectorDAO, instance: Any) -> dict[str, Any]:
    """Convert an instance to a storage dict."""
    if isinstance(instance, StorageModel):
        data = instance.to_storage_dict()
    elif isinstance(instance, dict):
        data = dict(instance)
    else:
        data = (
            instance.model_dump() if hasattr(instance, "model_dump") else dict(instance)
        )
    return data


async def _ensure_table(
    dao: PgVectorDAO,
    table: str,
    data: dict[str, Any],
) -> None:
    """Ensure the target table exists, creating it if necessary."""
    if table in dao._ensured_tables:
        return

    col_defs = ["uid TEXT PRIMARY KEY"]
    for key, value in data.items():
        if key == "uid":
            continue
        if key == "embedding":
            dim = dao.embedding_dim
            col_defs.append(f"embedding vector({dim})")
        else:
            col_type = infer_column_type(value, key)
            col_defs.append(f"{key} {col_type}")

    ddl = "CREATE TABLE IF NOT EXISTS {} ({})".format(table, ", ".join(col_defs))

    assert dao.pool is not None
    async with dao.pool.acquire() as conn:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await conn.execute(ddl)

    dao._ensured_tables.add(table)
    logger.debug("Ensured table %s", table)
