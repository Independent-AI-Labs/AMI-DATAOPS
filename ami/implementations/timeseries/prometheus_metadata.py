"""Prometheus discovery and metadata helpers.

Contains functions split from ``PrometheusDAO`` to keep the main
module under the 512-line limit.  Each function receives the *dao*
instance as its first parameter.
"""

from __future__ import annotations

import logging
from typing import Any

from ami.core.exceptions import StorageError
from ami.implementations.timeseries.prometheus_connection import (
    health_check,
)
from ami.implementations.timeseries.prometheus_read import (
    get_label_names,
    get_label_values,
    get_metric_metadata,
)

logger = logging.getLogger(__name__)

HTTP_OK = 200


async def list_databases(dao: Any) -> list[str]:
    """List 'databases' for Prometheus.

    Prometheus does not have a database concept; this returns the
    base URL as a single-element list (or configured database name).
    """
    if dao.config and dao.config.database:
        return [dao.config.database]
    return [dao.base_url]


async def list_schemas(
    dao: Any,
    database: str | None = None,
) -> list[str]:
    """List 'schemas' -- mapped to metric label namespaces.

    Returns all known label names as a proxy for schemas.
    """
    try:
        await dao._ensure_session()
        labels = await get_label_names(dao)
    except StorageError:
        return ["__name__"]
    else:
        return labels


async def list_models(
    dao: Any,
    database: str | None = None,
    schema: str | None = None,
) -> list[str]:
    """List 'models' -- mapped to metric names.

    Retrieves all unique ``__name__`` label values.
    """
    try:
        await dao._ensure_session()
        names = await get_label_values(dao, "__name__")
    except StorageError:
        return [dao._metric_name]
    else:
        return names


async def get_model_info(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Get information about a specific metric.

    Uses the ``/api/v1/metadata`` endpoint to retrieve type, help,
    and unit information.
    """
    try:
        await dao._ensure_session()
        metadata = await get_metric_metadata(dao, metric_name=path)
    except StorageError:
        return {
            "name": path,
            "type": "unknown",
            "help": "metadata unavailable",
        }
    else:
        if path in metadata:
            entries = metadata[path]
            if isinstance(entries, list) and entries:
                entry = entries[0]
                return {
                    "name": path,
                    "type": entry.get("type", "unknown"),
                    "help": entry.get("help", ""),
                    "unit": entry.get("unit", ""),
                }
        return {
            "name": path,
            "type": "unknown",
            "help": "",
            "unit": "",
        }


async def get_model_schema(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Get the 'schema' of a metric -- its label keys and type.

    Combines metadata and label discovery.
    """
    info = await get_model_info(dao, path, database, schema)
    fields: dict[str, Any] = {
        "__name__": {"type": "string", "required": True},
        "value": {"type": "float", "required": True},
        "timestamp": {"type": "datetime", "required": False},
    }

    try:
        await dao._ensure_session()
        labels = await get_label_names(dao, match=[path])
        for label in labels:
            if label != "__name__":
                fields[label] = {"type": "string", "required": False}
    except StorageError:
        pass

    return {
        "name": path,
        "metric_type": info.get("type", "unknown"),
        "help": info.get("help", ""),
        "fields": fields,
    }


async def get_model_fields(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Get field (label) information for a metric."""
    schema_info = await get_model_schema(dao, path, database, schema)
    fields = schema_info.get("fields", {})
    return [
        {"name": k, **v} if isinstance(v, dict) else {"name": k, "type": str(v)}
        for k, v in fields.items()
    ]


async def get_model_indexes(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Get index information for a metric.

    Prometheus automatically indexes all labels; this returns a
    representation of that.
    """
    try:
        await dao._ensure_session()
        labels = await get_label_names(dao, match=[path])
        return [
            {
                "name": f"label_index_{label}",
                "field": label,
                "type": "inverted",
                "unique": False,
            }
            for label in labels
            if label != "__name__"
        ]
    except StorageError:
        return []


async def test_connection(dao: Any) -> bool:
    """Test Prometheus connectivity via the health endpoint."""
    try:
        await dao._ensure_session()
        if dao.session is None:
            return False
        result = await health_check(dao.session, dao.base_url)
    except Exception:
        logger.exception("Prometheus connection test failed")
        return False
    else:
        return result
