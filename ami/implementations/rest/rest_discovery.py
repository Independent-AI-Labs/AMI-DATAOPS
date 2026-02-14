"""REST discovery and metadata helpers.

Contains functions that were split from ``RestDAO`` to keep the main
module under the 512-line limit.  Each function receives the *dao*
instance as its first parameter.
"""

from __future__ import annotations

import logging
from typing import Any

from ami.core.exceptions import StorageError
from ami.utils.http_client import request_with_retry

logger = logging.getLogger(__name__)

HTTP_OK = 200
_HTTP_SERVER_ERROR = 500


async def _try_discovery_endpoint(
    dao: Any,
    path: str,
) -> list[dict[str, Any]] | None:
    """Attempt a GET on a discovery endpoint, return parsed JSON or *None*."""
    session = await dao._ensure_session()
    url = f"{dao.base_url}/{path}"
    try:
        resp = await request_with_retry(session, "GET", url)
        async with resp:
            if resp.status != HTTP_OK:
                return None
            result = await resp.json()
    except StorageError:
        logger.debug("Discovery endpoint %s not available", path)
        return None
    else:
        data = dao._extract_data(result)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return None


async def list_databases(dao: Any) -> list[str]:
    """List databases / namespaces exposed by the REST API."""
    for endpoint in ("databases", "_databases", "namespaces"):
        result = await _try_discovery_endpoint(dao, endpoint)
        if result is not None:
            return [
                str(item.get("name", item)) if isinstance(item, dict) else str(item)
                for item in result
            ]
    # Fallback: return base database from config
    if dao.config and dao.config.database:
        return [dao.config.database]
    return []


async def list_schemas(
    dao: Any,
    database: str | None = None,
) -> list[str]:
    """List schemas / collections available in a database."""
    paths = ["schemas", "_schemas", "collections"]
    if database:
        paths = [f"{database}/{p}" for p in paths] + paths
    for endpoint in paths:
        result = await _try_discovery_endpoint(dao, endpoint)
        if result is not None:
            return [
                str(item.get("name", item)) if isinstance(item, dict) else str(item)
                for item in result
            ]
    return [dao.collection_name]


async def list_models(
    dao: Any,
    database: str | None = None,
    schema: str | None = None,
) -> list[str]:
    """List models / resources available."""
    prefix_parts: list[str] = []
    if database:
        prefix_parts.append(database)
    if schema:
        prefix_parts.append(schema)
    prefix = "/".join(prefix_parts)

    paths = ["models", "_models", "resources", "endpoints"]
    if prefix:
        paths = [f"{prefix}/{p}" for p in paths] + paths
    for endpoint in paths:
        result = await _try_discovery_endpoint(dao, endpoint)
        if result is not None:
            return [
                str(item.get("name", item)) if isinstance(item, dict) else str(item)
                for item in result
            ]
    return [dao.collection_name]


async def get_model_info(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Retrieve metadata about a specific model / resource."""
    parts: list[str] = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(path)
    resource_path = "/".join(parts)

    for suffix in ("_info", "_meta", ""):
        endpoint = f"{resource_path}{suffix}"
        result = await _try_discovery_endpoint(dao, endpoint)
        if result:
            return result[0] if len(result) == 1 else {"items": result}
    return {
        "name": path,
        "type": "rest_resource",
        "base_url": dao.base_url,
    }


async def get_model_schema(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Retrieve the JSON schema for a model / resource."""
    parts: list[str] = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(path)
    resource_path = "/".join(parts)

    for suffix in ("/_schema", "/schema", "/$schema"):
        endpoint = f"{resource_path}{suffix}"
        result = await _try_discovery_endpoint(dao, endpoint)
        if result:
            return result[0] if isinstance(result[0], dict) else {"schema": result}

    # Derive from model_cls fields
    fields_info: dict[str, Any] = {}
    model_fields = getattr(dao.model_cls, "model_fields", {})
    for name, field_info in model_fields.items():
        annotation = field_info.annotation
        type_name = getattr(annotation, "__name__", str(annotation))
        fields_info[name] = {
            "type": type_name,
            "required": field_info.is_required(),
        }
    return {"name": path, "fields": fields_info}


async def get_model_fields(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve field definitions for a model / resource."""
    schema_info = await get_model_schema(dao, path, database, schema)
    fields = schema_info.get("fields", {})
    if isinstance(fields, dict):
        return [
            {"name": k, **v} if isinstance(v, dict) else {"name": k, "type": str(v)}
            for k, v in fields.items()
        ]
    if isinstance(fields, list):
        return fields
    return []


async def get_model_indexes(
    dao: Any,
    path: str,
    database: str | None = None,
    schema: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve index definitions -- typically empty for REST."""
    parts: list[str] = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(path)
    resource_path = "/".join(parts)

    for suffix in ("/_indexes", "/indexes"):
        endpoint = f"{resource_path}{suffix}"
        result = await _try_discovery_endpoint(dao, endpoint)
        if result:
            return result
    return []


async def test_connection(dao: Any) -> bool:
    """Test REST API connectivity via a lightweight probe."""
    try:
        session = await dao._ensure_session()
    except Exception:
        logger.exception("REST connection test failed")
        return False
    else:
        # Try health / ping endpoints first
        for probe in ("health", "ping", "_health", ""):
            url = f"{dao.base_url}/{probe}" if probe else dao.base_url
            try:
                resp = await request_with_retry(
                    session,
                    "GET",
                    url,
                    max_retries=1,
                )
                async with resp:
                    if resp.status < _HTTP_SERVER_ERROR:
                        logger.info(
                            "REST connection test OK via %s",
                            url,
                        )
                        return True
            except StorageError:
                continue
        return False
