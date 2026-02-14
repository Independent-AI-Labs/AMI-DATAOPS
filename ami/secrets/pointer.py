"""Pointer representations for vault-backed sensitive fields."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class VaultFieldPointer(BaseModel):
    """Reference stored in primary storage for a sensitive field."""

    model_config = ConfigDict(
        extra="ignore",
        populate_by_name=True,
    )

    vault_reference: str
    integrity_hash: str
    version: int = 1
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
    )

    def to_storage(self) -> dict[str, Any]:
        """Serialize pointer for storage backends."""
        return self.model_dump(mode="json")
