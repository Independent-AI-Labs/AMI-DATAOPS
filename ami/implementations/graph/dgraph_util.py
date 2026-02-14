"""Utility functions for Dgraph operations."""

import json
import logging
import types
from datetime import datetime
from enum import Enum
from typing import Any, Union, get_args

import pydgraph
from pydantic import SecretStr

from ami.core.exceptions import StorageError

logger = logging.getLogger(__name__)


_DGRAPH_TYPE_MAPPING = {
    str: "string",
    int: "int",
    float: "float",
    bool: "bool",
    datetime: "datetime",
    list: "[string]",
    dict: "string",
}


def _is_union_type(python_type: Any) -> bool:
    """Check if a type is a Union type."""
    if hasattr(python_type, "__origin__") and python_type.__origin__ is Union:
        return True
    return isinstance(python_type, types.UnionType)


def _resolve_union_type(python_type: Any) -> str:
    """Resolve a Union type to a Dgraph type string."""
    args = get_args(python_type)
    non_none_types = [arg for arg in args if arg is not type(None)]
    if len(non_none_types) == 1:
        return get_dgraph_type(non_none_types[0])
    if len(non_none_types) > 1:
        msg = (
            f"Union type {python_type} with multiple"
            " non-None types not supported in Dgraph"
        )
        raise StorageError(msg)
    msg = f"Cannot map Union type {python_type} with only None"
    raise StorageError(msg)


def _resolve_generic_type(python_type: Any) -> str | None:
    """Resolve a generic type (list[X], dict[X,Y]) to Dgraph."""
    if not hasattr(python_type, "__origin__"):
        return None
    origin = python_type.__origin__
    if origin is list and list in _DGRAPH_TYPE_MAPPING:
        return _DGRAPH_TYPE_MAPPING[list]
    if origin is dict and dict in _DGRAPH_TYPE_MAPPING:
        return _DGRAPH_TYPE_MAPPING[dict]
    return None


def get_dgraph_type(python_type: Any) -> str:
    """Map Python type to Dgraph type.

    Raises:
        StorageError: If the Python type cannot be mapped.
    """
    if _is_union_type(python_type):
        return _resolve_union_type(python_type)

    generic_result = _resolve_generic_type(python_type)
    if generic_result is not None:
        return generic_result

    # Get base type from explicit mapping only
    for py_type, dg_type in _DGRAPH_TYPE_MAPPING.items():
        if python_type == py_type:
            return dg_type

    # Handle Enum types - store as string
    if isinstance(python_type, type) and issubclass(python_type, Enum):
        return "string"

    # No implicit default
    msg = (
        f"Cannot map Python type {python_type}"
        " to Dgraph type. Supported types:"
        f" {list(_DGRAPH_TYPE_MAPPING.keys())}"
    )
    raise StorageError(msg)


def _convert_field_value(value: Any) -> Any:
    """Convert a single field value to Dgraph-compatible format."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, list | dict):
        return json.dumps(value, default=json_encoder)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def to_dgraph_format(instance: Any, collection_name: str) -> dict[str, Any]:
    """Convert model instance to Dgraph format."""
    model_class_name = None
    if hasattr(instance, "model_dump"):
        data = instance.model_dump()
        model_class_name = (
            f"{instance.__class__.__module__}.{instance.__class__.__name__}"
        )
    elif isinstance(instance, dict):
        data = instance
    else:
        msg = f"Cannot convert {type(instance)} to Dgraph format"
        raise ValueError(msg)

    prefixed: dict[str, Any] = {}

    if model_class_name:
        prefixed[f"{collection_name}._model_class"] = model_class_name

    for key, value in data.items():
        if key in ("storage_configs", "path"):
            continue
        if key == "uid":
            prefixed[f"{collection_name}.app_uid"] = value
        elif value is not None:
            prefixed[f"{collection_name}.{key}"] = _convert_field_value(value)

    return prefixed


def process_dgraph_value(value: Any) -> Any:
    """Process a single value from Dgraph format."""
    result = value

    # Handle Dgraph list fields that contain JSON strings
    if isinstance(value, list) and len(value) == 1 and isinstance(value[0], str):
        # Dgraph returns JSON strings wrapped in an array
        if value[0].startswith(("[", "{")):
            try:
                result = json.loads(value[0])
            except json.JSONDecodeError:
                result = value[0]
        else:
            result = value[0]
    # Parse JSON strings back to objects for string values that look like JSON
    elif isinstance(value, str) and value.startswith(("[", "{")):
        try:
            parsed = json.loads(value)
            # Check if it's still a JSON string (double-encoded)
            result = (
                json.loads(parsed)
                if isinstance(parsed, str) and parsed.startswith(("[", "{"))
                else parsed
            )
        except json.JSONDecodeError:
            result = value

    return result


def _extract_clean_data(
    data: dict[str, Any], collection_name: str
) -> tuple[dict[str, Any], str | None]:
    """Extract clean data and model class name from Dgraph data."""
    clean_data: dict[str, Any] = {}
    prefix = f"{collection_name}."
    model_class_name = None

    for key, value in data.items():
        if key == "uid":
            continue  # Store Dgraph's internal uid separately
        if key.startswith(prefix):
            field_name = key[len(prefix) :]
            clean_data, model_class_name = _process_prefixed_field(
                field_name, value, clean_data, model_class_name
            )
        elif key != "dgraph.type":
            clean_data[key] = process_dgraph_value(value)

    return clean_data, model_class_name


def _process_prefixed_field(
    field_name: str,
    value: Any,
    clean_data: dict[str, Any],
    model_class_name: str | None,
) -> tuple[dict[str, Any], str | None]:
    """Process a prefixed field from Dgraph."""
    if field_name == "_model_class":
        model_class_name = value
    elif field_name == "app_uid":
        clean_data["uid"] = process_dgraph_value(value)
    elif field_name != "uid":
        clean_data[field_name] = process_dgraph_value(value)

    return clean_data, model_class_name


def _create_model_instance(
    model_class_name: str | None,
    clean_data: dict[str, Any],
    model_cls: Any,
) -> Any:
    """Create model instance from clean data.

    Raises:
        StorageError: If instance cannot be created.
    """
    try:
        return model_cls(**clean_data)
    except Exception as e:
        msg = f"Could not create model instance from clean data: {e}"
        raise StorageError(msg) from e


def from_dgraph_format(
    data: dict[str, Any], model_cls: Any, collection_name: str
) -> Any | None:
    """Convert Dgraph data to model instance."""
    if not data:
        return None

    clean_data, model_class_name = _extract_clean_data(data, collection_name)
    return _create_model_instance(model_class_name, clean_data, model_cls)


def _build_operator_filter(key: str, value: Any, collection_name: str) -> str:
    """Build filter for operator queries ($or, $and)."""
    if key == "$or":
        or_filters = [build_filter(or_query, collection_name) for or_query in value]
        return "({})".format(" OR ".join(or_filters))
    if key == "$and":
        and_filters = [build_filter(and_query, collection_name) for and_query in value]
        return "({})".format(" AND ".join(and_filters))
    return ""


def _build_function_call(collection_name: str, limit: int | None, offset: int) -> str:
    """Build DQL function call with pagination."""
    func_params = [f"type({collection_name})"]

    if offset:
        func_params.append(f"offset: {offset}")
    if limit:
        func_params.append(f"first: {limit}")

    func_call = f"{collection_name}_results(func: {func_params[0]}"
    if len(func_params) > 1:
        func_call += ", " + ", ".join(func_params[1:])
    return func_call + ")"


def build_dql_query(
    query: dict[str, Any],
    collection_name: str,
    limit: int | None = None,
    offset: int = 0,
) -> str:
    """Build DQL query from dictionary."""
    filters = []

    for key, value in query.items():
        if key.startswith("$"):
            filter_result = _build_operator_filter(key, value, collection_name)
            if filter_result:
                filters.append(filter_result)
        else:
            # Simple equality
            field = f"{collection_name}.{key}"
            filters.append(f'eq({field}, "{value}")')

    # Combine filters and build query
    filter_str = " AND ".join(filters) if filters else ""
    func_call = _build_function_call(collection_name, limit, offset)

    query_parts = [func_call]
    if filter_str:
        query_parts.append(f"@filter({filter_str})")

    query_parts.extend(["{", "uid", "expand(_all_)", "}"])

    return "{" + " ".join(query_parts) + "}"


def build_filter(query: dict[str, Any], collection_name: str) -> str:
    """Build filter expression from query dict."""
    filters = []

    for key, value in query.items():
        if isinstance(value, dict):
            # Handle operators like $in, $gt, etc.
            for op, op_value in value.items():
                if op == "$in":
                    in_values = ", ".join([f'"{v}"' for v in op_value])
                    filters.append(f"eq({collection_name}.{key}, [{in_values}])")
                elif op == "$gt":
                    filters.append(f"gt({collection_name}.{key}, {op_value})")
                elif op == "$lt":
                    filters.append(f"lt({collection_name}.{key}, {op_value})")
                elif op == "$regex":
                    filters.append(f'regexp({collection_name}.{key}, "/{op_value}/")')
        else:
            # Simple equality
            filters.append(f'eq({collection_name}.{key}, "{value}")')

    return " AND ".join(filters)


def build_count_query(query: dict[str, Any], collection_name: str) -> str:
    """Build count query."""
    filter_str = build_filter(query, collection_name) if query else ""

    query_parts = ["{", f"count(func: type({collection_name}))"]

    if filter_str:
        query_parts.append(f"@filter({filter_str})")

    query_parts.append("{")
    query_parts.append("total: count(uid)")
    query_parts.append("}")
    query_parts.append("}")

    return " ".join(query_parts)


def json_encoder(obj: Any) -> str:
    """JSON encoder that handles datetime and enums."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, SecretStr):
        return obj.get_secret_value()
    if (
        hasattr(obj, "__class__")
        and hasattr(obj.__class__, "__bases__")
        and isinstance(obj, Enum)
    ):
        # Handle enums by returning their value
        return str(obj.value)
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def _collect_indexed_fields(
    metadata: Any | None,
) -> dict[str, str]:
    """Collect indexed fields from model metadata."""
    indexed_fields: dict[str, str] = {}
    if metadata and hasattr(metadata, "indexes"):
        tokenizer_map = {"text": "fulltext", "hash": "exact"}
        for index in metadata.indexes:
            field_name = index.get("field")
            index_type = index.get("type", "hash")
            indexed_fields[field_name] = tokenizer_map.get(index_type, index_type)
    indexed_fields["app_uid"] = "exact"
    return indexed_fields


def _build_field_schema(
    collection_name: str,
    model_cls: Any,
    indexed_fields: dict[str, str],
) -> tuple[list[str], str]:
    """Build schema parts and type definition for model fields."""
    schema_parts = [
        f"{collection_name}.app_uid: string @index(exact) .",
        f"{collection_name}._model_class: string .",
    ]
    type_def = (
        f"type {collection_name} {{\n"
        f"  {collection_name}.app_uid\n"
        f"  {collection_name}._model_class"
    )
    skip_fields = {"id", "uid", "storage_configs", "path"}

    for field_name, field_info in model_cls.model_fields.items():
        if field_name in skip_fields:
            continue
        dgraph_type = get_dgraph_type(field_info.annotation)
        if field_name in indexed_fields and dgraph_type != "bool":
            idx = indexed_fields[field_name]
            schema_parts.append(
                f"{collection_name}.{field_name}: {dgraph_type} @index({idx}) ."
            )
        else:
            schema_parts.append(f"{collection_name}.{field_name}: {dgraph_type} .")
        type_def += f"\n  {collection_name}.{field_name}"

    type_def += "\n}"
    return schema_parts, type_def


def ensure_schema(
    client: Any,
    model_cls: Any,
    metadata: Any | None,
    collection_name: str,
) -> None:
    """Ensure Dgraph schema is set up for the model."""
    if not client:
        msg = "Not connected to Dgraph"
        raise StorageError(msg)

    indexed_fields = _collect_indexed_fields(metadata)
    schema_parts, type_def = _build_field_schema(
        collection_name, model_cls, indexed_fields
    )

    schema = "\n".join(schema_parts) + "\n\n" + type_def

    try:
        op = pydgraph.Operation(schema=schema)
        client.alter(op)
        logger.debug("Schema applied for %s", collection_name)
    except Exception as e:
        msg = f"Schema update failed for {collection_name}: {e}"
        raise StorageError(msg) from e
