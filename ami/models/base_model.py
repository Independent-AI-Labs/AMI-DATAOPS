"""Base StorageModel class that all persisted models inherit from."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator
from uuid_utils import uuid7

from ami.models.secured_mixin import SecuredModelMixin
from ami.models.security import Permission, SecurityContext
from ami.models.storage_config import StorageConfig
from ami.models.storage_mixin import StorageConfigMixin

if TYPE_CHECKING:
    from ami.core.dao import BaseDAO

_logger = logging.getLogger(__name__)


class ModelMetadata(BaseModel):
    """Typed, immutable model metadata.

    Replaces the untyped ``class Meta`` inner-class pattern.
    Subclasses declare a ClassVar::

        class MyModel(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
                path="my_models",
                storage_configs={"pg": some_config},
            )
    """

    model_config = ConfigDict(frozen=True)

    path: str | None = None
    storage_configs: dict[str, StorageConfig] = Field(
        default_factory=dict,
    )
    indexes: list[dict[str, Any]] = Field(
        default_factory=list,
    )


# Sentinel used when no metadata is declared on a subclass.
_EMPTY_META = ModelMetadata()


class StorageModel(SecuredModelMixin, StorageConfigMixin, BaseModel):
    """Base model for all storage-aware models.

    Pure data model — persistence logic lives in UnifiedCRUD.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        use_enum_values=True,
        validate_assignment=True,
    )

    _vault_pointer_cache: dict[str, Any] = PrivateAttr(
        default_factory=dict,
    )

    # Subclasses override this with their own ModelMetadata.
    _model_meta: ClassVar[ModelMetadata] = _EMPTY_META

    uid: str | None = Field(
        default_factory=lambda: str(uuid7()),
        description="Unique identifier",
    )
    updated_at: datetime | None = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Last update timestamp",
    )
    path: str | None = Field(
        default=None,
        description="Storage path/collection name",
    )

    # ------------------------------------------------------------------
    # Validators / hooks
    # ------------------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def _hydrate_sensitive_fields(cls, data: Any) -> Any:
        """Hydrate sensitive fields from vault pointers."""
        if not isinstance(data, dict):
            return data
        if not getattr(cls, "_sensitive_fields", None):
            return data
        try:
            from ami.secrets.adapter import (
                hydrate_sensitive_fields,
            )

            return hydrate_sensitive_fields(cls, data)
        except ImportError:
            return data

    def model_post_init(self, /, __context: Any) -> None:
        super().model_post_init(__context)
        try:
            from ami.secrets.adapter import (
                consume_pointer_cache,
            )

            pointer_map = consume_pointer_cache()
            if pointer_map:
                self._vault_pointer_cache.update(pointer_map)
        except ImportError:
            pass

    # ------------------------------------------------------------------
    # Metadata access
    # ------------------------------------------------------------------

    def get_collection_name(self) -> str:
        """Get collection/table name for this instance."""
        meta = self.__class__._model_meta
        return meta.path or self.__class__.__name__.lower() + "s"

    def get_primary_storage_config(self) -> StorageConfig | None:
        """Get the primary storage configuration."""
        if self.storage_configs and len(self.storage_configs) > 0:
            return self.storage_configs[0]
        return None

    @classmethod
    def get_metadata(cls) -> ModelMetadata:
        """Return the typed ``ModelMetadata`` for this model class."""
        return cls._model_meta

    @classmethod
    def get_all_daos(cls) -> dict[str, BaseDAO]:
        """Get all DAOs for configured storage backends."""
        from ami.core.dao import DAOFactory

        configs = cls._model_meta.storage_configs
        if not configs:
            msg = (
                f"No storage configs for model {cls.__name__}. "
                "Set _model_meta on the class."
            )
            raise ValueError(msg)
        return {name: DAOFactory.create(cls, cfg) for name, cfg in configs.items()}

    @classmethod
    def get_dao(
        cls,
        storage_name: str | None = None,
    ) -> BaseDAO:
        """Get a specific DAO by storage name."""
        all_daos = cls.get_all_daos()
        if storage_name:
            if storage_name in all_daos:
                return all_daos[storage_name]
            msg = f"Storage '{storage_name}' not found"
            raise ValueError(msg)
        if all_daos:
            return next(iter(all_daos.values()))
        msg = "No storage configurations available"
        raise ValueError(msg)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_storage_dict(
        self,
        context: SecurityContext | None = None,
    ) -> dict[str, Any]:
        """Convert model to storage dictionary format."""
        data = self.model_dump(mode="python", exclude_none=False)
        data.pop("__vault_pointer_cache__", None)
        data.pop("storage_configs", None)
        data.pop("path", None)
        if getattr(self.__class__, "_sensitive_fields", None):
            try:
                from ami.secrets.adapter import (
                    prepare_instance_for_storage,
                )

                return prepare_instance_for_storage(self, data, context)
            except ImportError:
                pass
        return data

    @classmethod
    def from_storage_dict(cls, data: dict[str, Any]) -> StorageModel:
        """Create model instance from storage dictionary."""
        processed = data.copy()
        for field_name, field_info in cls.model_fields.items():
            if (
                field_name in processed
                and field_info.annotation is datetime
                and isinstance(processed[field_name], str)
            ):
                try:
                    processed[field_name] = datetime.fromisoformat(
                        processed[field_name],
                    )
                except ValueError:
                    _logger.warning(
                        "Failed to parse datetime '%s' for '%s'",
                        processed[field_name],
                        field_name,
                    )
        return cls(**processed)

    # ------------------------------------------------------------------
    # Security-aware operations
    # ------------------------------------------------------------------

    @classmethod
    async def create_with_security(
        cls,
        context: SecurityContext,
        **data: Any,
    ) -> StorageModel:
        """Create a new instance with security context."""
        security_data: dict[str, Any] = {
            "created_by": context.user_id,
            "modified_by": context.user_id,
            "owner_id": context.user_id,
        }
        if context.tenant_id:
            security_data["tenant_id"] = context.tenant_id
        return cls(**{**data, **security_data})

    @classmethod
    async def find_with_security(
        cls,
        context: SecurityContext,
        query: dict[str, Any],
        **kwargs: Any,
    ) -> list[StorageModel]:
        """Find instances with security filtering."""
        from ami.core.unified_crud import UnifiedCRUD

        crud = UnifiedCRUD()
        secured_query = query.copy()
        if context.tenant_id:
            tenant_filter = {"tenant_id": context.tenant_id}
            secured_query = (
                {"$and": [secured_query, tenant_filter]}
                if secured_query
                else tenant_filter
            )
        results = await crud.query(cls, secured_query, **kwargs)
        accessible: list[StorageModel] = []
        for inst in results:
            if hasattr(inst, "check_permission"):
                if await inst.check_permission(
                    context,
                    Permission.READ,
                    raise_on_deny=False,
                ):
                    accessible.append(inst)
            else:
                accessible.append(inst)
        return accessible
