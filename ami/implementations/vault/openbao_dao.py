"""OpenBao (Vault) Data Access Object implementation.

Provides CRUD operations against a Vault-compatible (OpenBao) KV-v2
secrets engine.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from uuid_utils import uuid7

from ami.core.dao import BaseDAO
from ami.core.exceptions import StorageError
from ami.models.storage_config import StorageConfig

# Deferred import: the openbao client may not be installed in every
# environment.  The default values let the module load without error;
# actual usage of OpenBaoDAO.connect() will raise StorageError if the
# client is missing.
OpenBaoClient: type[Any] | None = None
OpenBaoError: type[Exception] = Exception
try:
    from openbao import Client as _Cli
    from openbao.exceptions import OpenBaoError as _Err

    OpenBaoClient = _Cli
    OpenBaoError = _Err
except Exception:
    pass

logger = logging.getLogger(__name__)

# Minimum ordinal for printable characters (space = 32)
_MIN_PRINTABLE_ORD = 32
# Default Vault/OpenBao HTTPS port
_VAULT_DEFAULT_PORT = 8200


class OpenBaoDAO(BaseDAO):
    """DAO for OpenBao/Vault KV-v2 secrets engine.

    Each "record" is a Vault secret stored at
    ``<mount>/<collection_name>/<item_id>``.
    """

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        super().__init__(model_cls, config)
        self.client: Any | None = None
        self._mount: str = "secret"
        self._connected: bool = False

        if config and config.options:
            self._mount = config.options.get("mount", "secret")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _reference(self, item_id: str) -> str:
        """Build the vault path for a given item ID.

        Validates the item_id to prevent path traversal and injection.
        """
        if (
            ".." in item_id
            or item_id.startswith("/")
            or any(ord(c) < _MIN_PRINTABLE_ORD for c in item_id)
        ):
            msg = f"Invalid item ID for vault reference: {item_id!r}"
            raise StorageError(msg)
        return f"{self.collection_name}/{item_id}"

    def _ensure_client(self) -> Any:
        """Return the Vault client, raising if not connected."""
        if self.client is None:
            msg = "OpenBao client not connected. Call connect() first."
            raise StorageError(msg)
        return self.client

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to OpenBao/Vault."""
        if OpenBaoClient is None:
            msg = "OpenBaoClient is not available. Install the ami.secrets package."
            raise StorageError(msg)

        if self.client is not None:
            return

        try:
            url = ""
            token = ""
            if self.config:
                is_default = self.config.port == _VAULT_DEFAULT_PORT
                protocol = "https" if is_default else "http"
                url = (
                    self.config.connection_string
                    or f"{protocol}://{self.config.host}:{self.config.port}"
                )
                token = self.config.password or ""
                if self.config.options:
                    token = self.config.options.get("token", token)

            self.client = OpenBaoClient(url=url, token=token)
            self._connected = True
            logger.info("Connected to OpenBao at %s", url)
        except OpenBaoError as exc:
            msg = f"Failed to connect to OpenBao: {exc}"
            raise StorageError(msg) from exc

    async def disconnect(self) -> None:
        """Disconnect from OpenBao/Vault."""
        self.client = None
        self._connected = False
        logger.info("Disconnected from OpenBao")

    async def test_connection(self) -> bool:
        """Test connectivity to the Vault server."""
        try:
            if self.client is None:
                await self.connect()
            client = self._ensure_client()
            # Attempt to read the mount config as a health check
            if hasattr(client, "sys") and hasattr(client.sys, "read_health_status"):
                client.sys.read_health_status()
            elif hasattr(client, "is_authenticated"):
                return bool(client.is_authenticated())
        except Exception:
            logger.exception("OpenBao connection test failed")
            return False
        else:
            return True

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create(self, instance: Any) -> str:
        """Create a new secret."""
        client = self._ensure_client()

        if hasattr(instance, "to_storage_dict"):
            data = instance.to_storage_dict()
        elif isinstance(instance, dict):
            data = instance.copy()
        else:
            data = (
                instance.model_dump(mode="json")
                if hasattr(instance, "model_dump")
                else dict(instance)
            )

        uid = str(data.get("uid") or data.get("id") or uuid7())
        data["uid"] = uid
        data["updated_at"] = datetime.now(UTC).isoformat()
        data["created_at"] = data.get("created_at", data["updated_at"])

        path = self._reference(uid)
        try:
            client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=data,
                mount_point=self._mount,
            )
        except OpenBaoError as exc:
            msg = f"Failed to create secret at {path}: {exc}"
            raise StorageError(msg) from exc

        logger.debug("Created vault secret at %s", path)
        return uid

    async def find_by_id(self, item_id: str) -> Any | None:
        """Read a secret by ID."""
        client = self._ensure_client()
        path = self._reference(item_id)
        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=self._mount,
            )
        except OpenBaoError:
            return None

        secret_data = (
            response.get("data", {}).get("data", {})
            if isinstance(response, dict)
            else {}
        )
        if not secret_data:
            return None
        return self.model_cls.from_storage_dict(secret_data)

    async def find_one(self, query: dict[str, Any]) -> Any | None:
        """Find a single secret matching *query*."""
        if "uid" in query:
            return await self.find_by_id(str(query["uid"]))
        if "id" in query:
            return await self.find_by_id(str(query["id"]))
        results = await self.find(query, limit=1)
        return results[0] if results else None

    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        """List and filter secrets."""
        client = self._ensure_client()
        try:
            response = client.secrets.kv.v2.list_secrets(
                path=self.collection_name,
                mount_point=self._mount,
            )
        except OpenBaoError:
            return []

        keys: list[str] = (
            response.get("data", {}).get("keys", [])
            if isinstance(response, dict)
            else []
        )

        results: list[Any] = []
        for key in keys:
            clean_key = key.rstrip("/")
            item = await self.find_by_id(clean_key)
            if item is None:
                continue
            if query:
                item_dict = (
                    item.to_storage_dict()
                    if hasattr(item, "to_storage_dict")
                    else item
                    if isinstance(item, dict)
                    else {}
                )
                if not all(item_dict.get(k) == v for k, v in query.items()):
                    continue
            results.append(item)

        if skip:
            results = results[skip:]
        if limit is not None:
            results = results[:limit]
        return results

    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """Update an existing secret."""
        client = self._ensure_client()
        existing = await self.find_by_id(item_id)
        if existing is None:
            msg = f"Secret not found: {item_id}"
            raise StorageError(msg)

        merged = (
            existing.to_storage_dict()
            if hasattr(existing, "to_storage_dict")
            else existing
            if isinstance(existing, dict)
            else {}
        )
        merged.update(data)
        merged["updated_at"] = datetime.now(UTC).isoformat()

        path = self._reference(item_id)
        try:
            client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=merged,
                mount_point=self._mount,
            )
        except OpenBaoError as exc:
            msg = f"Failed to update secret at {path}: {exc}"
            raise StorageError(msg) from exc
        logger.debug("Updated vault secret at %s", path)

    async def delete(self, item_id: str) -> bool:
        """Delete a secret by ID."""
        client = self._ensure_client()
        path = self._reference(item_id)
        try:
            client.secrets.kv.v2.delete_metadata_and_all_versions(
                path=path,
                mount_point=self._mount,
            )
        except OpenBaoError:
            return False
        else:
            logger.debug("Deleted vault secret at %s", path)
            return True

    async def count(self, query: dict[str, Any]) -> int:
        """Count matching secrets."""
        results = await self.find(query)
        return len(results)

    async def exists(self, item_id: str) -> bool:
        """Check whether a secret exists."""
        return await self.find_by_id(item_id) is not None

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    async def bulk_create(self, instances: list[Any]) -> list[str]:
        """Create multiple secrets."""
        ids: list[str] = []
        for inst in instances:
            uid = await self.create(inst)
            ids.append(uid)
        return ids

    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Update multiple secrets."""
        for upd in updates:
            item_id = str(upd.get("uid") or upd.get("id", ""))
            if not item_id:
                logger.warning("Skipping bulk_update entry without id")
                continue
            data = {k: v for k, v in upd.items() if k not in ("uid", "id")}
            await self.update(item_id, data)

    async def bulk_delete(self, ids: list[str]) -> int:
        """Delete multiple secrets."""
        deleted = 0
        for item_id in ids:
            if await self.delete(item_id):
                deleted += 1
        return deleted

    # ------------------------------------------------------------------
    # Schema / indexes  (mostly no-ops for Vault)
    # ------------------------------------------------------------------

    async def create_indexes(self) -> None:
        """No-op: Vault does not support indexes."""
        logger.debug("create_indexes is a no-op for OpenBao DAO")

    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a raw Vault read at an arbitrary path."""
        client = self._ensure_client()
        try:
            response = client.secrets.kv.v2.read_secret_version(
                path=query,
                mount_point=self._mount,
            )
        except OpenBaoError as exc:
            msg = f"Raw read failed at {query}: {exc}"
            raise StorageError(msg) from exc
        data = (
            response.get("data", {}).get("data", {})
            if isinstance(response, dict)
            else {}
        )
        return [data] if data else []

    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute a raw Vault write at an arbitrary path."""
        client = self._ensure_client()
        try:
            client.secrets.kv.v2.create_or_update_secret(
                path=query,
                secret=params or {},
                mount_point=self._mount,
            )
        except OpenBaoError as exc:
            msg = f"Raw write failed at {query}: {exc}"
            raise StorageError(msg) from exc
        else:
            return 1

    # ------------------------------------------------------------------
    # Discovery / metadata
    # ------------------------------------------------------------------

    async def list_databases(self) -> list[str]:
        """List available mounts as 'databases'."""
        client = self._ensure_client()
        try:
            if hasattr(client, "sys") and hasattr(
                client.sys, "list_mounted_secrets_engines"
            ):
                mounts = client.sys.list_mounted_secrets_engines()
                if isinstance(mounts, dict):
                    data = mounts.get("data", mounts)
                    return [k.rstrip("/") for k in data if isinstance(k, str)]
        except OpenBaoError:
            pass
        return [self._mount]

    async def list_schemas(self, database: str | None = None) -> list[str]:
        """List secret paths as 'schemas'."""
        client = self._ensure_client()
        mount = database or self._mount
        try:
            response = client.secrets.kv.v2.list_secrets(
                path="",
                mount_point=mount,
            )
            keys = (
                response.get("data", {}).get("keys", [])
                if isinstance(response, dict)
                else []
            )
            return [k.rstrip("/") for k in keys]
        except OpenBaoError:
            return [self.collection_name]

    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List secret keys under the collection path as 'models'."""
        client = self._ensure_client()
        mount = database or self._mount
        path = schema or self.collection_name
        try:
            response = client.secrets.kv.v2.list_secrets(
                path=path,
                mount_point=mount,
            )
            keys = (
                response.get("data", {}).get("keys", [])
                if isinstance(response, dict)
                else []
            )
            return [k.rstrip("/") for k in keys]
        except OpenBaoError:
            return []

    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get info about a secret path."""
        return {
            "name": path,
            "type": "vault_secret",
            "mount": database or self._mount,
            "schema": schema or self.collection_name,
        }

    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Derive schema from the model class fields."""
        fields_info: dict[str, Any] = {}
        model_fields = getattr(self.model_cls, "model_fields", {})
        for name, field_info in model_fields.items():
            annotation = field_info.annotation
            type_name = getattr(annotation, "__name__", str(annotation))
            fields_info[name] = {
                "type": type_name,
                "required": field_info.is_required(),
            }
        return {"name": path, "fields": fields_info}

    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get field info for a model."""
        schema_info = await self.get_model_schema(path, database, schema)
        fields = schema_info.get("fields", {})
        return [
            {"name": k, **v} if isinstance(v, dict) else {"name": k, "type": str(v)}
            for k, v in fields.items()
        ]

    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Vault does not support indexes."""
        return []
