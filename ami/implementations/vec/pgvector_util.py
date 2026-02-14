"""Utility functions for pgvector operations.

Provides SQL helpers for table creation, column type inference,
serialization, index management, and embedding-column DDL specific
to the pgvector extension.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import SecretStr

from ami.core.exceptions import StorageError

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Identifier validation
# ------------------------------------------------------------------

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def is_valid_identifier(name: str) -> bool:
    """Return *True* if *name* is safe for use as an SQL identifier."""
    return bool(_IDENT_RE.match(name))


def get_safe_table_name(collection_name: str) -> str:
    """Validate and return *collection_name* for SQL use."""
    if not is_valid_identifier(collection_name):
        msg = f"Invalid table name: {collection_name}"
        raise StorageError(msg)
    return collection_name


# ------------------------------------------------------------------
# Column type inference
# ------------------------------------------------------------------

_ISO_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")

_TIMESTAMP_NAMES: set[str] = {
    "created_at",
    "updated_at",
    "modified_at",
    "timestamp",
    "date",
    "time",
    "expires_at",
    "deleted_at",
    "published_at",
    "started_at",
    "ended_at",
}


def infer_column_type(value: Any, column_name: str | None = None) -> str:
    """Infer the PostgreSQL column type from a Python *value*."""
    type_mapping: list[tuple[Callable[[Any], bool], str]] = [
        (lambda v: v is None, "TEXT"),
        (lambda v: isinstance(v, bool), "BOOLEAN"),
        (lambda v: isinstance(v, int), "BIGINT"),
        (lambda v: isinstance(v, float), "DOUBLE PRECISION"),
        (lambda v: isinstance(v, datetime), "TIMESTAMP WITH TIME ZONE"),
        (lambda v: isinstance(v, dict | list), "JSONB"),
    ]

    for check, sql_type in type_mapping:
        if check(value):
            return sql_type

    if column_name and isinstance(value, str):
        lower = column_name.lower()
        if (
            lower in _TIMESTAMP_NAMES
            or lower.endswith("_at")
            or lower.endswith("_date")
        ) and _ISO_TS_RE.match(value):
            return "TIMESTAMP WITH TIME ZONE"

    return "TEXT"


# ------------------------------------------------------------------
# Serialization helpers
# ------------------------------------------------------------------


def serialize_value(value: Any) -> Any:
    """Serialize a Python value for PostgreSQL storage."""
    if isinstance(value, dict | list):
        return json.dumps(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, SecretStr):
        msg = "SecretStr should be processed by vault adapter before serialization"
        raise TypeError(msg)
    if isinstance(value, datetime):
        return value
    return value


def deserialize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Deserialize a row returned from PostgreSQL."""
    result: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, str):
            try:
                result[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                result[key] = value
        else:
            result[key] = value
    return result


# ------------------------------------------------------------------
# WHERE clause builder
# ------------------------------------------------------------------


def build_where_clause(
    filters: dict[str, Any],
) -> tuple[str, list[Any]]:
    """Build a parameterised WHERE clause from *filters*."""
    where_parts: list[str] = []
    values: list[Any] = []
    param_count = 1

    for key, value in filters.items():
        if is_valid_identifier(key):
            if value is None:
                where_parts.append(f"{key} IS NULL")
            else:
                where_parts.append(f"{key} = ${param_count}")
                values.append(serialize_value(value))
                param_count += 1

    if where_parts:
        return " AND ".join(where_parts), values
    return "", []


# ------------------------------------------------------------------
# Index management
# ------------------------------------------------------------------


async def create_indexes_for_table(
    conn: Any,
    table_name: str,
    data: dict[str, Any],
) -> None:
    """Create default GIN / B-tree indexes on *table_name*."""
    jsonb_cols = [
        k
        for k, v in data.items()
        if isinstance(v, dict | list) and is_valid_identifier(k)
    ]
    for col in jsonb_cols:
        try:
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_gin "
                f"ON {table_name} USING gin ({col})",
            )
        except Exception:
            logger.warning("Failed to create GIN index on %s.%s", table_name, col)

    ts_cols = [
        k for k, v in data.items() if isinstance(v, datetime) and is_valid_identifier(k)
    ]
    for col in ts_cols:
        try:
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_btree "
                f"ON {table_name} ({col})",
            )
        except Exception:
            logger.warning("Failed to create B-tree index on %s.%s", table_name, col)


async def create_model_indexes(
    conn: Any,
    table_name: str,
    model_cls: type[Any],
) -> None:
    """Create indexes declared in model metadata."""
    metadata = model_cls.get_metadata()
    indexes: list[dict[str, Any]] = metadata.indexes

    for idx_def in indexes:
        columns = idx_def.get("columns", [])
        unique = idx_def.get("unique", False)
        index_type = idx_def.get("type", "btree")
        name = idx_def.get("name") or "idx_{}_{}".format(
            table_name,
            "_".join(columns),
        )

        if not columns or not all(is_valid_identifier(c) for c in columns):
            logger.warning("Skipping invalid index definition: %s", idx_def)
            continue

        col_list = ", ".join(columns)
        unique_kw = "UNIQUE " if unique else ""
        sql = (
            f"CREATE {unique_kw}INDEX IF NOT EXISTS {name} "
            f"ON {table_name} USING {index_type} ({col_list})"
        )
        try:
            await conn.execute(sql)
        except Exception:
            logger.warning("Failed to create index %s on %s", name, table_name)
