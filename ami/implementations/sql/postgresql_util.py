"""Utility functions for PostgreSQL operations."""

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


def is_valid_identifier(name: str) -> bool:
    """Validate that identifier is safe for SQL."""
    return bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name))


def get_safe_table_name(collection_name: str) -> str:
    """Get validated table name for use in SQL queries."""
    if not is_valid_identifier(collection_name):
        msg = f"Invalid table name: {collection_name}"
        raise StorageError(msg)
    return collection_name


def infer_column_type(value: Any, column_name: str | None = None) -> str:
    """Infer SQL column type from Python value and column name."""
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
        timestamp_patterns = {
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
        if (
            column_name.lower() in timestamp_patterns
            or column_name.lower().endswith("_at")
            or column_name.lower().endswith("_date")
        ):
            iso_pattern = re.compile(
                r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
            )
            if iso_pattern.match(value):
                return "TIMESTAMP WITH TIME ZONE"

    return "TEXT"


def serialize_value(value: Any) -> Any:
    """Serialize value for PostgreSQL storage."""
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
    """Deserialize row from PostgreSQL."""
    result = {}
    for key, value in row.items():
        if isinstance(value, str):
            try:
                result[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                result[key] = value
        else:
            result[key] = value
    return result


def build_where_clause(
    filters: dict[str, Any],
) -> tuple[str, list[Any]]:
    """Build WHERE clause from filters."""
    where_clauses = []
    values = []
    param_count = 1

    for key, value in filters.items():
        if is_valid_identifier(key):
            if value is None:
                where_clauses.append(f"{key} IS NULL")
            else:
                where_clauses.append(f"{key} = ${param_count}")
                values.append(serialize_value(value))
                param_count += 1

    if where_clauses:
        return " AND ".join(where_clauses), values
    return "", []


async def create_indexes_for_table(
    conn: Any,
    table_name: str,
    data: dict[str, Any],
) -> None:
    """Create indexes for efficient querying."""
    jsonb_columns = [
        key
        for key, value in data.items()
        if isinstance(value, dict | list) and is_valid_identifier(key)
    ]

    for col in jsonb_columns:
        try:
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_gin "
                f"ON {table_name} USING gin ({col})",
            )
        except Exception as e:
            logger.warning("Failed to create GIN index for %s: %s", col, e)

    timestamp_columns = [
        key
        for key, value in data.items()
        if isinstance(value, datetime) and is_valid_identifier(key)
    ]

    for col in timestamp_columns:
        try:
            await conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_btree "
                f"ON {table_name} ({col})",
            )
        except Exception as e:
            logger.warning(
                "Failed to create B-tree index for %s: %s",
                col,
                e,
            )
