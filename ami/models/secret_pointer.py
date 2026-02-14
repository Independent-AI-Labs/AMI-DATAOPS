"""Storage model for secrets broker pointer metadata."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

from pydantic import Field

from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config_factory import StorageConfigFactory


class SecretPointerRecord(StorageModel):
    """Persists metadata about secrets broker vault pointers."""

    vault_reference: str = ""
    namespace: str = ""
    model_name: str = ""
    field_name: str = ""
    integrity_hash: str = ""
    version: int = 1
    rotation_count: int = 0
    secret_created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    secret_updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )
    secret_last_accessed_at: datetime | None = None
    status: str = Field(
        default="active",
        description="Lifecycle status for the pointer",
    )

    _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
        path="secret_pointer_records",
        storage_configs={
            "postgres": StorageConfigFactory.from_yaml("postgres"),
        },
        indexes=[
            {"field": "vault_reference", "type": "hash", "unique": True},
            {"field": "namespace", "type": "hash"},
            {"field": "model_name", "type": "hash"},
            {"field": "field_name", "type": "hash"},
        ],
    )
