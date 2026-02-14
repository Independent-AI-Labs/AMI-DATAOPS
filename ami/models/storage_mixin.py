"""Storage configuration mixin for models."""

from __future__ import annotations

from pydantic import Field

from ami.models.storage_config import StorageConfig


class StorageConfigMixin:
    """Mixin that provides storage_configs field to models."""

    storage_configs: list[StorageConfig] | None = Field(
        default=None,
        description="List of storage configurations for this model",
    )
