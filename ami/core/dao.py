"""Data Access Object base classes and factory."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.models.storage_config import StorageConfig
from ami.models.storage_config_factory import StorageConfigFactory

if TYPE_CHECKING:
    from ami.models.base_model import StorageModel

T = TypeVar("T", bound="StorageModel")

_logger = logging.getLogger(__name__)


class BaseDAO(ABC):
    """Abstract base class for all Data Access Objects."""

    def __init__(
        self,
        model_cls: type[Any],
        config: StorageConfig | None = None,
    ) -> None:
        self.model_cls = model_cls
        metadata = model_cls.get_metadata()
        self.collection_name: str = metadata.path or (model_cls.__name__.lower() + "s")
        self.config = config

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to storage backend."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to storage backend."""

    @abstractmethod
    async def test_connection(self) -> bool:
        """Test if connection is valid."""

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    @abstractmethod
    async def create(self, instance: Any) -> str:
        """Create new record, return ID."""

    @abstractmethod
    async def find_by_id(self, item_id: str) -> Any | None:
        """Find record by ID."""

    @abstractmethod
    async def find_one(self, query: dict[str, Any]) -> Any | None:
        """Find single record matching query."""

    @abstractmethod
    async def find(
        self,
        query: dict[str, Any],
        limit: int | None = None,
        skip: int = 0,
    ) -> list[Any]:
        """Find multiple records matching query."""

    @abstractmethod
    async def update(self, item_id: str, data: dict[str, Any]) -> None:
        """Update record by ID."""

    @abstractmethod
    async def delete(self, item_id: str) -> bool:
        """Delete record by ID."""

    @abstractmethod
    async def count(self, query: dict[str, Any]) -> int:
        """Count records matching query."""

    @abstractmethod
    async def exists(self, item_id: str) -> bool:
        """Check if record exists."""

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    @abstractmethod
    async def bulk_create(self, instances: list[Any]) -> list[str]:
        """Bulk insert multiple records."""

    @abstractmethod
    async def bulk_update(self, updates: list[dict[str, Any]]) -> None:
        """Bulk update multiple records."""

    @abstractmethod
    async def bulk_delete(self, ids: list[str]) -> dict[str, Any] | int:
        """Bulk delete multiple records."""

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    @abstractmethod
    async def create_indexes(self) -> None:
        """Create indexes defined in metadata."""

    @abstractmethod
    async def raw_read_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute raw read query."""

    @abstractmethod
    async def raw_write_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> int:
        """Execute raw write query."""

    @abstractmethod
    async def list_databases(self) -> list[str]:
        """List all databases/namespaces/buckets."""

    @abstractmethod
    async def list_schemas(
        self,
        database: str | None = None,
    ) -> list[str]:
        """List all schemas/collections/directories."""

    @abstractmethod
    async def list_models(
        self,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """List all models in current storage."""

    @abstractmethod
    async def get_model_info(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get information about a model."""

    @abstractmethod
    async def get_model_schema(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> dict[str, Any]:
        """Get schema/structure information for a model."""

    @abstractmethod
    async def get_model_fields(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get field information for a model."""

    @abstractmethod
    async def get_model_indexes(
        self,
        path: str,
        database: str | None = None,
        schema: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get index information for a model."""

    # ------------------------------------------------------------------
    # Convenience (non-abstract)
    # ------------------------------------------------------------------

    async def find_or_create(
        self,
        query: dict[str, Any],
        defaults: dict[str, Any] | None = None,
    ) -> tuple[Any, bool]:
        """Find record or create if not exists."""
        instance = await self.find_one(query)
        if instance:
            return instance, False
        create_data = {**query, **(defaults or {})}
        new_instance = self.model_cls(**create_data)
        saved_id = await self.create(new_instance)
        new_instance.uid = saved_id
        return new_instance, True

    async def update_or_create(
        self,
        query: dict[str, Any],
        defaults: dict[str, Any] | None = None,
    ) -> tuple[Any, bool]:
        """Update record or create if not exists."""
        instance = await self.find_one(query)
        if instance and instance.uid:
            await self.update(instance.uid, defaults or {})
            saved = await self.find_by_id(instance.uid)
            if saved:
                return saved, False
        create_data = {**query, **(defaults or {})}
        new_instance = self.model_cls(**create_data)
        saved_id = await self.create(new_instance)
        new_instance.uid = saved_id
        return new_instance, True


# ======================================================================
# DAO Registry — simple dict-based registry (no external dependencies)
# ======================================================================

_dao_registry: dict[StorageType, type[BaseDAO]] = {}


def register_dao(
    storage_type: StorageType,
    dao_class: type[BaseDAO],
) -> None:
    """Register a DAO implementation for a storage type."""
    _dao_registry[storage_type] = dao_class


def get_dao_class(storage_type: StorageType) -> type[BaseDAO]:
    """Look up the DAO class for a storage type."""
    dao_class = _dao_registry.get(storage_type)
    if dao_class is None:
        msg = f"No DAO registered for storage type: {storage_type}"
        raise StorageError(msg)
    return dao_class


# ======================================================================
# DAOFactory
# ======================================================================

_TYPE_TO_YAML_NAME: dict[StorageType, str | None] = {
    StorageType.GRAPH: "dgraph",
    StorageType.INMEM: "redis",
    StorageType.RELATIONAL: "postgres",
    StorageType.VECTOR: "pgvector",
    StorageType.DOCUMENT: "mongodb",
    StorageType.TIMESERIES: "prometheus",
    StorageType.REST: None,
    StorageType.VAULT: None,
    StorageType.FILE: None,
}


def _merge_with_yaml_defaults(
    storage_type: StorageType,
    storage_config: StorageConfig,
) -> StorageConfig:
    """Best-effort merge of provided config with YAML defaults."""
    yaml_name = _TYPE_TO_YAML_NAME.get(storage_type)
    if not yaml_name:
        return storage_config
    yaml_cfg = StorageConfigFactory.from_yaml(yaml_name)
    return StorageConfig(
        storage_type=storage_config.storage_type,
        host=storage_config.host or yaml_cfg.host,
        port=storage_config.port or yaml_cfg.port,
        database=storage_config.database or yaml_cfg.database,
        username=storage_config.username or yaml_cfg.username,
        password=storage_config.password or yaml_cfg.password,
        options={**(yaml_cfg.options or {}), **(storage_config.options or {})},
    )


class DAOFactory:
    """Factory for creating appropriate DAO instances."""

    @classmethod
    def create(
        cls,
        model_cls: type[Any],
        storage_config: StorageConfig,
    ) -> BaseDAO:
        """Create appropriate DAO for model with specific storage config."""
        storage_type = storage_config.storage_type
        if storage_type is None:
            msg = "Storage type cannot be None"
            raise StorageError(msg)
        dao_class = get_dao_class(storage_type)
        merged = _merge_with_yaml_defaults(storage_type, storage_config)
        return dao_class(model_cls, merged)


def get_dao(
    model_cls: type[Any],
    storage_config: StorageConfig,
) -> BaseDAO:
    """Convenience: get a DAO instance for the given model and config."""
    return DAOFactory.create(model_cls, storage_config)
