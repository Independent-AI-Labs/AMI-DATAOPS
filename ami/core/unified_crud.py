"""UnifiedCRUD — persistence logic for StorageModel instances."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from ami.core.dao import DAOFactory
from ami.core.exceptions import NotFoundError
from ami.core.storage_types import StorageType
from ami.models.storage_config import StorageConfig

if TYPE_CHECKING:
    from ami.core.dao import BaseDAO
    from ami.models.base_model import StorageModel


class UnifiedCRUD:
    """Unified CRUD operations for StorageModel instances.

    Handles all persistence logic: DAO caching, event-loop safety,
    field mapping, and UID tracking.
    """

    def __init__(self) -> None:
        self._dao_cache: dict[tuple[type[StorageModel], int], BaseDAO] = {}
        self._dao_loop_cache: dict[
            tuple[type[StorageModel], int], asyncio.AbstractEventLoop
        ] = {}
        self._uid_registry: dict[str, tuple[type[StorageModel], int]] = {}

    # ------------------------------------------------------------------
    # DAO resolution helpers
    # ------------------------------------------------------------------

    async def _get_dao(
        self,
        model: StorageModel | type[StorageModel],
        config_index: int = 0,
    ) -> BaseDAO:
        model_class = self._resolve_model_class(model)
        cache_key = (model_class, config_index)
        current_loop = asyncio.get_running_loop()
        await self._evict_if_loop_changed(cache_key, current_loop)

        cached = self._dao_cache.get(cache_key)
        if cached is not None:
            return cached

        configs = self._resolve_storage_configs(model_class, model)
        config = self._select_config(configs, config_index, model_class)
        return await self._create_and_cache_dao(
            cache_key,
            model_class,
            config,
            current_loop,
        )

    @staticmethod
    def _resolve_model_class(
        model: StorageModel | type[StorageModel],
    ) -> type[StorageModel]:
        if isinstance(model, type):
            return model
        model_class = model.__class__
        if model_class.__name__ == "StorageModel":
            msg = (
                "Received bare StorageModel instance; "
                "storage lookup requires a concrete subclass"
            )
            raise ValueError(msg)
        return model_class

    async def _evict_if_loop_changed(
        self,
        cache_key: tuple[type[StorageModel], int],
        current_loop: asyncio.AbstractEventLoop,
    ) -> None:
        cached_dao = self._dao_cache.get(cache_key)
        cached_loop = self._dao_loop_cache.get(cache_key)
        if not cached_dao:
            return
        loop_changed = cached_loop is not None and (
            cached_loop.is_closed() or cached_loop is not current_loop
        )
        if not loop_changed:
            return
        try:
            await cached_dao.disconnect()
        finally:
            self._dao_cache.pop(cache_key, None)
            self._dao_loop_cache.pop(cache_key, None)

    def _resolve_storage_configs(
        self,
        model_class: type[StorageModel],
        model: StorageModel | type[StorageModel],
    ) -> list[StorageConfig]:
        meta_configs = model_class.get_metadata().storage_configs
        configs: (
            StorageConfig | list[StorageConfig] | dict[Any, StorageConfig] | None
        ) = meta_configs or None
        if configs is None and not isinstance(model, type):
            configs = getattr(model, "storage_configs", None)
        if configs is None:
            msg = (
                f"No storage configs available for {model_class.__name__}. "
                "Set _model_meta on the class."
            )
            raise ValueError(msg)
        if isinstance(configs, StorageConfig):
            return [configs]
        if isinstance(configs, dict):
            return list(configs.values())
        return list(configs)

    @staticmethod
    def _select_config(
        configs: list[StorageConfig],
        config_index: int,
        model_class: type[StorageModel],
    ) -> StorageConfig:
        if config_index < 0 or config_index >= len(configs):
            msg = (
                f"Config index {config_index} out of range "
                f"for {model_class.__name__} ({len(configs)} available)"
            )
            raise ValueError(msg)
        return configs[config_index]

    async def _create_and_cache_dao(
        self,
        cache_key: tuple[type[StorageModel], int],
        model_class: type[StorageModel],
        config: StorageConfig,
        current_loop: asyncio.AbstractEventLoop,
    ) -> BaseDAO:
        dao = DAOFactory.create(model_class, config)
        await dao.connect()
        self._dao_cache[cache_key] = dao
        self._dao_loop_cache[cache_key] = current_loop
        return dao

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------

    async def create(
        self,
        model: StorageModel,
        config_index: int = 0,
    ) -> str:
        """Create a new model instance in storage. Returns UID."""
        dao = await self._get_dao(model, config_index)
        uid = await dao.create(model)
        model.uid = uid
        self._uid_registry[uid] = (model.__class__, config_index)
        return uid

    async def read(
        self,
        model_class: type[StorageModel],
        uid: str,
        config_index: int = 0,
    ) -> StorageModel:
        """Read a model instance from storage by UID."""
        dao = await self._get_dao(model_class, config_index)
        result = await dao.find_by_id(uid)
        if result is None:
            msg = f"Instance with UID {uid} not found for model {model_class.__name__}"
            raise NotFoundError(msg)
        found: StorageModel = result
        return found

    async def update(
        self,
        model: StorageModel,
        config_index: int = 0,
    ) -> None:
        """Update a model instance in storage."""
        if not model.uid:
            msg = "Cannot update model without UID"
            raise ValueError(msg)
        dao = await self._get_dao(model, config_index)
        model.updated_at = datetime.now(UTC)
        await dao.update(model.uid, model.to_storage_dict())

    async def delete(
        self,
        model: StorageModel,
        config_index: int = 0,
    ) -> bool:
        """Delete a model instance from storage."""
        if not model.uid:
            msg = "Cannot delete model without UID"
            raise ValueError(msg)
        dao = await self._get_dao(model, config_index)
        return await dao.delete(model.uid)

    async def query(
        self,
        model_class: type[StorageModel],
        query: dict[str, Any] | None = None,
        limit: int | None = None,
        skip: int = 0,
        config_index: int = 0,
    ) -> list[StorageModel]:
        """Query for multiple model instances."""
        dao = await self._get_dao(model_class, config_index)
        return await dao.find(query or {}, limit=limit, skip=skip)

    async def count(
        self,
        model_class: type[StorageModel],
        query: dict[str, Any] | None = None,
        config_index: int = 0,
    ) -> int:
        """Count model instances matching query."""
        dao = await self._get_dao(model_class, config_index)
        return await dao.count(query or {})

    # ------------------------------------------------------------------
    # UID-based lookups
    # ------------------------------------------------------------------

    async def read_by_uid(self, uid: str) -> StorageModel | None:
        """Read a model instance by globally unique UID."""
        if uid in self._uid_registry:
            model_class, config_index = self._uid_registry[uid]
            return await self.read(model_class, uid, config_index)
        return None

    async def delete_by_uid(self, uid: str) -> bool:
        """Delete a model instance by globally unique UID."""
        if uid not in self._uid_registry:
            msg = f"UID {uid} not found in registry, cannot delete"
            raise NotFoundError(msg)
        model_class, config_index = self._uid_registry[uid]
        dao = await self._get_dao(model_class, config_index)
        result = await dao.delete(uid)
        if result:
            del self._uid_registry[uid]
        return result

    # ------------------------------------------------------------------
    # Raw queries
    # ------------------------------------------------------------------

    async def raw_query(
        self,
        model_class: type[StorageModel],
        config: StorageConfig,
        query: str,
    ) -> list[dict[str, Any]]:
        """Execute a raw read query on a specific storage backend."""
        dao = DAOFactory.create(model_class, config)
        await dao.connect()
        try:
            if hasattr(dao, "raw_read_query"):
                return await dao.raw_read_query(query)
            msg = f"Raw queries not supported for {config.storage_type}"
            raise NotImplementedError(msg)
        finally:
            await dao.disconnect()

    # ------------------------------------------------------------------
    # Field mapping helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _map_to_storage(model: StorageModel) -> dict[str, Any]:
        """Map model fields to storage engine properties."""
        data: dict[str, Any] = model.model_dump(exclude_none=False)
        config = model.get_primary_storage_config()
        if not config:
            return data

        if config.storage_type == StorageType.RELATIONAL:
            if "uid" in data:
                data["id"] = data.pop("uid")
        elif config.storage_type == StorageType.GRAPH and (
            "uid" not in data or data["uid"] is None
        ):
            data["uid"] = str(uuid7())

        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

    @staticmethod
    def _map_from_storage(
        model_class: type[StorageModel],
        data: dict[str, Any],
    ) -> StorageModel:
        """Map storage data back to model instance."""
        if isinstance(data, dict):
            if "id" in data and "uid" not in data:
                data["uid"] = data.pop("id")
            for field_name, field_info in model_class.model_fields.items():
                if (
                    field_name in data
                    and field_info.annotation is datetime
                    and isinstance(data[field_name], str)
                ):
                    data[field_name] = datetime.fromisoformat(
                        data[field_name],
                    )
        return model_class(**data)
