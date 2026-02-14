"""Create operations for PostgreSQL DAO."""

import logging
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import Enum
from typing import Any, get_args, get_origin

from uuid_utils import uuid7

from ami.core.exceptions import StorageError
from ami.implementations.sql.postgresql_util import (
    get_safe_table_name,
    is_valid_identifier,
    serialize_value,
)

logger = logging.getLogger(__name__)

ISO_DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{6})?(\+00:00|Z)?$",
)


def convert_datetime_strings_for_timestamps(
    data: dict[str, Any],
) -> dict[str, Any]:
    """Convert ISO datetime strings back to datetime objects."""
    timestamp_columns = {
        "created_at",
        "updated_at",
        "modified_at",
        "expires_at",
        "deleted_at",
        "last_accessed_at",
        "accessed_at",
        "started_at",
        "ended_at",
    }
    converted_data: dict[str, Any] = {}

    for key, value in data.items():
        is_timestamp_field = (
            key in timestamp_columns
            or key.lower().endswith("_at")
            or key.lower().endswith("_time")
            or key.lower().endswith("_date")
        )

        if (
            is_timestamp_field
            and isinstance(value, str)
            and ISO_DATETIME_PATTERN.match(value)
        ):
            datetime_str = value[:-1] + "+00:00" if value.endswith("Z") else value
            try:
                converted_data[key] = datetime.fromisoformat(datetime_str)
            except ValueError as e:
                msg = (
                    f"Failed to parse datetime field '{key}' with value '{value}': {e}"
                )
                raise StorageError(msg) from e
        else:
            converted_data[key] = value

    return converted_data


def _strip_optional(annotation: Any) -> Any:
    """Remove Optional/Union[None, T] wrappers from annotations."""
    origin = get_origin(annotation)
    if origin is None:
        return annotation

    none_type: type[None] = type(None)

    if getattr(annotation, "_name", None) == "Optional":
        args = [arg for arg in get_args(annotation) if arg is not none_type]
        return args[0] if args else str

    if getattr(annotation, "_name", None) == "Annotated":
        args = list(get_args(annotation))
        return args[0] if args else str

    if getattr(annotation, "_name", None) == "Union":
        args = [arg for arg in get_args(annotation) if arg is not none_type]
        return args[0] if args else str

    args = list(get_args(annotation))
    if args:
        non_none = [arg for arg in args if arg is not none_type]
        return non_none[0] if non_none else str

    return origin


def _annotation_to_sql_type(annotation: Any) -> str:
    """Map a Pydantic field annotation to a PostgreSQL column type."""
    origin_hint = get_origin(annotation)
    if origin_hint in {list, dict}:
        return "JSONB"
    resolved = _strip_optional(annotation)
    origin = get_origin(resolved)
    if origin:
        resolved = origin

    result = "TEXT"
    if resolved is bool:
        result = "BOOLEAN"
    elif resolved is int:
        result = "BIGINT"
    elif resolved is float:
        result = "DOUBLE PRECISION"
    elif resolved is datetime:
        result = "TIMESTAMPTZ"
    elif resolved is dict or resolved is list:
        result = "JSONB"
    elif isinstance(resolved, type) and issubclass(resolved, Enum):
        result = "TEXT"

    if resolved is None:
        result = "TEXT"
    return result


def _get_model_defined_columns(dao: Any) -> dict[str, str]:
    """Derive column definitions from the DAO's model class."""
    model_cls = getattr(dao, "model_cls", None)
    if model_cls is None:
        return {}

    columns: dict[str, str] = {}
    model_fields = getattr(model_cls, "model_fields", {})
    omit_columns = set(getattr(dao, "_omit_columns", set()))
    for field_name, field_info in model_fields.items():
        if not is_valid_identifier(field_name):
            continue
        if field_name in omit_columns:
            continue
        columns[field_name] = _annotation_to_sql_type(field_info.annotation)
    return columns


async def _add_missing_schema_columns(
    conn: Any,
    table_name: str,
    columns: dict[str, str],
) -> None:
    """Ensure columns from model metadata exist on the table."""
    if not columns:
        return

    existing_columns = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1 AND table_schema = 'public'
        """,
        table_name,
    )
    existing_names = {row["column_name"] for row in existing_columns}

    for name, sql_type in columns.items():
        if name == "id" or name in existing_names:
            continue
        if not is_valid_identifier(name):
            continue
        try:
            await conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {name} {sql_type}",
            )
        except Exception as e:
            msg = f"Failed to add model-defined column {name} to table {table_name}"
            raise StorageError(msg) from e
        logger.info(
            "Added model-defined column %s (%s) to table %s",
            name,
            sql_type,
            table_name,
        )


async def _ensure_metadata_indexes(
    conn: Any,
    table_name: str,
    dao: Any,
) -> None:
    """Create indexes declared in model metadata."""
    model_cls = getattr(dao, "model_cls", None)
    if model_cls is None:
        return
    metadata = model_cls.get_metadata()
    indexes: Iterable[dict[str, Any]] = metadata.indexes
    if not indexes:
        return

    for index in indexes:
        field = index.get("field")
        if not field or not is_valid_identifier(field):
            continue

        unique = bool(index.get("unique"))
        index_type = index.get("type", "btree")
        if unique:
            index_type = "btree"

        name = index.get("name") or f"idx_{table_name}_{field}"
        if index_type == "hash" and not unique:
            try:
                await conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {name} "
                    f"ON {table_name} USING hash ({field})",
                )
            except Exception as e:
                msg = f"Failed to create index {name} on table {table_name}"
                raise StorageError(msg) from e
        else:
            unique_prefix = "UNIQUE " if unique else ""
            try:
                await conn.execute(
                    f"CREATE {unique_prefix}INDEX IF NOT EXISTS {name} "
                    f"ON {table_name} ({field})",
                )
            except Exception as e:
                msg = f"Failed to create index {name} on table {table_name}"
                raise StorageError(msg) from e


async def ensure_table_exists(dao: Any) -> None:
    """Ensure table exists with schema from model metadata."""
    if dao._table_created:
        return

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)
    model_columns = _get_model_defined_columns(dao)

    if not model_columns:
        msg = f"No model fields found for {dao.model_cls.__name__}"
        raise StorageError(msg)

    async with dao.pool.acquire() as conn:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = $1
            )
            """,
            table_name,
        )

        if not exists:
            await create_table_from_schema(conn, table_name, model_columns)

        await _add_missing_schema_columns(conn, table_name, model_columns)
        await _ensure_metadata_indexes(conn, table_name, dao)

        dao._table_created = True


async def create_table_from_schema(
    conn: Any,
    table_name: str,
    columns: dict[str, str],
) -> None:
    """Create table using model-defined schema."""
    column_defs = ["id TEXT PRIMARY KEY"]
    for name, sql_type in columns.items():
        if name == "id" or not is_valid_identifier(name):
            continue
        column_defs.append(f"{name} {sql_type}")

    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(column_defs)}
        )
    """
    try:
        await conn.execute(create_sql)
    except Exception as e:
        msg = f"Failed to create table {table_name} from schema"
        raise StorageError(msg) from e
    logger.info(
        "Created table %s from model schema with %d columns",
        table_name,
        len(column_defs),
    )


async def create(dao: Any, data: dict[str, Any]) -> str:
    """Create a new record."""
    data = convert_datetime_strings_for_timestamps(data)
    await ensure_table_exists(dao)

    if not dao.pool:
        await dao.connect()

    table_name = get_safe_table_name(dao.collection_name)

    if "uid" not in data and "id" not in data:
        data["id"] = str(uuid7())
    elif "uid" in data and "id" not in data:
        data["id"] = data["uid"]

    now = datetime.now(UTC)
    data["created_at"] = now
    data["updated_at"] = now

    async with dao.pool.acquire() as conn:
        columns: list[str] = []
        values: list[Any] = []
        param_markers: list[str] = []

        existing_columns = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1 AND table_schema = 'public'
            """,
            table_name,
        )
        existing_names = {row["column_name"] for row in existing_columns}

        if "data" in existing_names and "data" not in data:
            data["data"] = {}

        for i, (key, value) in enumerate(data.items(), 1):
            if is_valid_identifier(key):
                columns.append(key)
                values.append(serialize_value(value))
                param_markers.append(f"${i}")

        update_cols = [
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col not in ("id", "updated_at")
        ]
        update_clause = (
            ", ".join(update_cols) + ", updated_at = CURRENT_TIMESTAMP"
            if update_cols
            else "updated_at = CURRENT_TIMESTAMP"
        )

        insert_sql = f"""
            INSERT INTO {table_name} ({", ".join(columns)})
            VALUES ({", ".join(param_markers)})
            ON CONFLICT (id) DO UPDATE SET {update_clause}
            RETURNING id
        """

        try:
            result = await conn.fetchval(insert_sql, *values)
        except Exception as e:
            msg = f"Failed to create record in table {table_name}"
            raise StorageError(msg) from e

        uid_value = data.get("uid")
        return uid_value if uid_value is not None else str(result)
