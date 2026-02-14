"""REST API Data Access Object implementation.

Provides CRUD operations against generic REST/HTTP endpoints.
Discovery and metadata methods are delegated to ``rest_discovery``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import aiohttp

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageError
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig
from ami.utils.http_client import request_with_retry

logger = logging.getLogger(__name__)

# HTTP status helpers
HTTP_OK = 200
_HTTPS_PORT = 443
HTTP_CREATED = 201
HTTP_NO_CONTENT = 204
HTTP_NOT_FOUND = 404


class RestDAO(BaseDAO):
    """DAO for REST/HTTP API backends.

    Translates standard CRUD operations into HTTP requests.
    """

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        super().__init__(model_cls, config)
        self.session: aiohttp.ClientSession | None = None
        self.base_url: str = ""
        self._connected: bool = False

        if config:
            self.base_url = self._build_base_url(config)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_base_url(config: StorageConfig) -> str:
        """Build the base URL from storage config."""
        if config.connection_string:
            return config.connection_string.rstrip("/")
        protocol = "https" if config.port == _HTTPS_PORT else "http"
        host = config.host or "localhost"
        port = config.port or 443
        base = f"{protocol}://{host}:{port}"
        if config.database:
            base = f"{base}/{config.database}"
        return base

    def _build_url(self, path: str = "", item_id: str | None = None) -> str:
        """Build full request URL."""
        url = f"{self.base_url}/{self.collection_name}"
        if path:
            url = f"{url}/{path}"
        if item_id is not None:
            url = f"{url}/{item_id}"
        return url

    def _prepare_headers(self) -> dict[str, str]:
        """Prepare HTTP headers including auth tokens."""
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.config and self.config.options:
            token = self.config.options.get("auth_token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
            api_key = self.config.options.get("api_key")
            if api_key:
                headers["X-API-Key"] = api_key
            extra_headers = self.config.options.get("headers")
            if isinstance(extra_headers, dict):
                headers.update(extra_headers)
        return headers

    def _map_fields(self, data: dict[str, Any]) -> dict[str, Any]:
        """Apply any field-name mappings from config options."""
        if not self.config or not self.config.options:
            return data
        mapping = self.config.options.get("field_mapping")
        if not mapping or not isinstance(mapping, dict):
            return data
        mapped: dict[str, Any] = {}
        reverse = {v: k for k, v in mapping.items()}
        for key, value in data.items():
            mapped_key = reverse.get(key, key)
            mapped[mapped_key] = value
        return mapped

    def _extract_data(self, response_json: Any) -> Any:
        """Extract data payload from response JSON.

        Handles common REST envelope patterns like
        ``{"data": ...}`` or ``{"results": ...}``.
        """
        if isinstance(response_json, dict):
            for key in ("data", "results", "items", "records"):
                if key in response_json:
                    return response_json[key]
        return response_json

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open an aiohttp session."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                headers=self._prepare_headers(),
                timeout=timeout,
            )
            self._connected = True
            logger.info("REST session opened for %s", self.base_url)

    async def disconnect(self) -> None:
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            self._connected = False
            logger.info("REST session closed for %s", self.base_url)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            await self.connect()
        assert self.session is not None
        return self.session

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create(self, instance: Any) -> str:
        """POST a new resource."""
        session = await self._ensure_session()
        if isinstance(instance, StorageModel):
            data = instance.to_storage_dict()
        elif isinstance(instance, dict):
            data = instance
        else:
            data = (
                instance.model_dump(mode="json")
                if hasattr(instance, "model_dump")
                else dict(instance)
            )

        data["updated_at"] = datetime.now(UTC).isoformat()
        url = self._build_url()
        resp = await request_with_retry(session, "POST", url, json=data)
        async with resp:
            if resp.status not in (HTTP_OK, HTTP_CREATED):
                body = await resp.text()
                msg = f"REST create failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
            result = await resp.json()
        extracted = self._extract_data(result)
        if isinstance(extracted, dict):
            return str(extracted.get("uid") or extracted.get("id", ""))
        return str(extracted)

    async def find_by_id(self, item_id: str) -> Any | None:
        """GET a single resource by ID."""
        session = await self._ensure_session()
        url = self._build_url(item_id=item_id)
        resp = await request_with_retry(session, "GET", url)
        async with resp:
            if resp.status == HTTP_NOT_FOUND:
                return None
            if resp.status != HTTP_OK:
                body = await resp.text()
                msg = f"REST find_by_id failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
            result = await resp.json()
        data = self._extract_data(result)
        if isinstance(data, dict):
            data = self._map_fields(data)
            return self.model_cls.from_storage_dict(data)
        return data

    async def find_one(self, query: dict[str, Any]) -> Any | None:
        """Find a single record matching *query*."""
        results = await self.find(query, limit=1)
        return results[0] if results else None

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        """GET resources with query-string filters."""
        session = await self._ensure_session()
        params: dict[str, str] = {}
        for k, v in query.items():
            params[k] = str(v)
        if limit is not None:
            params["limit"] = str(limit)
        if skip:
            params["offset"] = str(skip)
        url = self._build_url()
        resp = await request_with_retry(session, "GET", url, params=params)
        async with resp:
            if resp.status != HTTP_OK:
                body = await resp.text()
                msg = f"REST find failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
            result = await resp.json()
        items = self._extract_data(result)
        if not isinstance(items, list):
            items = [items] if items else []
        output: list[Any] = []
        for item in items:
            if isinstance(item, dict):
                mapped_item = self._map_fields(item)
                output.append(self.model_cls.from_storage_dict(mapped_item))
            else:
                output.append(item)
        return output

    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """PUT/PATCH a resource."""
        session = await self._ensure_session()
        data["updated_at"] = datetime.now(UTC).isoformat()
        url = self._build_url(item_id=item_id)
        method = "PATCH"
        if self.config and self.config.options:
            method = self.config.options.get("update_method", "PATCH")
        resp = await request_with_retry(session, method, url, json=data)
        async with resp:
            if resp.status not in (HTTP_OK, HTTP_NO_CONTENT):
                body = await resp.text()
                msg = f"REST update failed: {resp.status} {body[:200]}"
                raise StorageError(msg)

    async def delete(self, item_id: str) -> bool:
        """DELETE a resource by ID."""
        session = await self._ensure_session()
        url = self._build_url(item_id=item_id)
        resp = await request_with_retry(session, "DELETE", url)
        async with resp:
            if resp.status == HTTP_NOT_FOUND:
                return False
            if resp.status not in (HTTP_OK, HTTP_NO_CONTENT):
                body = await resp.text()
                msg = f"REST delete failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
            return True

    async def count(self, query: dict[str, Any]) -> int:
        """Count matching resources."""
        session = await self._ensure_session()
        params = {k: str(v) for k, v in query.items()}
        url = self._build_url(path="count")
        try:
            resp = await request_with_retry(session, "GET", url, params=params)
            async with resp:
                if resp.status == HTTP_OK:
                    result = await resp.json()
                    extracted = self._extract_data(result)
                    if isinstance(extracted, int):
                        return extracted
                    if isinstance(extracted, dict):
                        return int(extracted.get("count", 0))
        except StorageError:
            pass
        # Fallback: fetch all and count
        items = await self.find(query)
        return len(items)

    async def exists(self, item_id: str) -> bool:
        """Check if resource exists via HEAD or GET."""
        session = await self._ensure_session()
        url = self._build_url(item_id=item_id)
        try:
            resp = await request_with_retry(session, "HEAD", url)
            async with resp:
                return resp.status == HTTP_OK
        except StorageError:
            result = await self.find_by_id(item_id)
            return result is not None

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    async def bulk_create(self, instances: list[Any]) -> list[str]:
        """Create multiple resources."""
        ids: list[str] = []
        for inst in instances:
            uid = await self.create(inst)
            ids.append(uid)
        return ids

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Update multiple resources."""
        for upd in updates:
            item_id = str(upd.get("uid") or upd.get("id", ""))
            if not item_id:
                logger.warning("Skipping bulk_update entry without id")
                continue
            data = {k: v for k, v in upd.items() if k not in ("uid", "id")}
            await self.update(item_id, data)

    async def bulk_delete(self, ids: list[str]) -> int:
        """Delete multiple resources."""
        deleted = 0
        for item_id in ids:
            if await self.delete(item_id):
                deleted += 1
        return deleted

    # ------------------------------------------------------------------
    # Schema / indexes
    # ------------------------------------------------------------------

    async def create_indexes(self) -> None:
        """No-op for REST backends."""
        logger.debug("create_indexes is a no-op for REST DAO")

    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a raw GET request with *query* as the URL path."""
        session = await self._ensure_session()
        url = f"{self.base_url}/{query}"
        resp = await request_with_retry(session, "GET", url, params=params)
        async with resp:
            if resp.status != HTTP_OK:
                body = await resp.text()
                msg = f"REST raw_read_query failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
            result = await resp.json()
        data = self._extract_data(result)
        if isinstance(data, list):
            return data
        return [data] if data else []

    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a raw POST request with *query* as the URL path."""
        session = await self._ensure_session()
        url = f"{self.base_url}/{query}"
        resp = await request_with_retry(session, "POST", url, json=params)
        async with resp:
            if resp.status not in (HTTP_OK, HTTP_CREATED, HTTP_NO_CONTENT):
                body = await resp.text()
                msg = f"REST raw_write_query failed: {resp.status} {body[:200]}"
                raise StorageError(msg)
            if resp.status == HTTP_NO_CONTENT:
                return 1
            result = await resp.json()
        extracted = self._extract_data(result)
        if isinstance(extracted, int):
            return extracted
        if isinstance(extracted, dict):
            return int(extracted.get("affected", 1))
        return 1

    # ------------------------------------------------------------------
    # Discovery / metadata (delegated to rest_discovery)
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[str]:
        from ami.implementations.rest.rest_discovery import (
            list_databases,
        )

        return await list_databases(self)

    async def list_schemas(self, database: str | None = None) -> list[str]:
        from ami.implementations.rest.rest_discovery import (
            list_schemas,
        )

        return await list_schemas(self, database)

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        from ami.implementations.rest.rest_discovery import (
            list_models,
        )

        return await list_models(self, database, schema)

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        from ami.implementations.rest.rest_discovery import (
            get_model_info,
        )

        return await get_model_info(self, path, database, schema)

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        from ami.implementations.rest.rest_discovery import (
            get_model_schema,
        )

        return await get_model_schema(self, path, database, schema)

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        from ami.implementations.rest.rest_discovery import (
            get_model_fields,
        )

        return await get_model_fields(self, path, database, schema)

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        from ami.implementations.rest.rest_discovery import (
            get_model_indexes,
        )

        return await get_model_indexes(self, path, database, schema)

    async def test_connection(self) -> bool:
        from ami.implementations.rest.rest_discovery import (
            test_connection,
        )

        return await test_connection(self)
