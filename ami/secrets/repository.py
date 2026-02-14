"""Persistence helpers for secrets broker pointer metadata."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, cast

from pydantic import BaseModel

from ami.core.unified_crud import UnifiedCRUD
from ami.models.secret_pointer import SecretPointerRecord
from ami.models.security import DataClassification

logger = logging.getLogger(__name__)


class EnsureRecordParams(BaseModel):
    """Parameters for creating or updating a pointer record."""

    reference: str
    namespace: str
    model_name: str
    field_name: str
    integrity_hash: str
    classification: DataClassification | None = None


class SecretPointerRepository:
    """Thin wrapper around UnifiedCRUD for managing pointer metadata."""

    def __init__(
        self,
        crud: UnifiedCRUD | None = None,
        *,
        primary_index: int = 0,
    ) -> None:
        self._crud = crud or UnifiedCRUD()
        self._primary_index = primary_index

    async def ensure_record(
        self,
        params: EnsureRecordParams,
    ) -> SecretPointerRecord:
        """Create or update a pointer record."""
        record = await self._find_by_reference(
            params.reference,
        )
        now = datetime.now(tz=UTC)
        classification_value = params.classification or DataClassification.INTERNAL

        if record is None:
            new_record = SecretPointerRecord(
                vault_reference=params.reference,
                namespace=params.namespace,
                model_name=params.model_name,
                field_name=params.field_name,
                classification=classification_value,
                integrity_hash=params.integrity_hash,
                version=1,
                rotation_count=0,
                secret_created_at=now,
                secret_updated_at=now,
            )
            await self._crud.create(
                new_record,
                config_index=self._primary_index,
            )
            logger.debug(
                "Created pointer record: ref=%s model=%s field=%s",
                params.reference,
                params.model_name,
                params.field_name,
            )
            return new_record

        record.secret_updated_at = now
        if params.classification and record.classification != params.classification:
            record.classification = params.classification
        if record.integrity_hash != params.integrity_hash:
            record.integrity_hash = params.integrity_hash
            record.version += 1
            record.rotation_count += 1

        await self._crud.update(
            record,
            config_index=self._primary_index,
        )
        refreshed = await self._refresh(record.uid)
        logger.debug(
            "Updated pointer record: ref=%s version=%d rotations=%d",
            params.reference,
            refreshed.version,
            refreshed.rotation_count,
        )
        return refreshed

    async def get_by_reference(
        self,
        reference: str,
    ) -> SecretPointerRecord | None:
        """Retrieve a pointer record for a vault reference."""
        return await self._find_by_reference(reference)

    async def mark_accessed(self, reference: str) -> None:
        """Update last accessed timestamp for a pointer."""
        record = await self._find_by_reference(reference)
        if record is None:
            return
        record.secret_last_accessed_at = datetime.now(tz=UTC)
        await self._crud.update(
            record,
            config_index=self._primary_index,
        )

    async def delete(self, reference: str) -> None:
        """Remove pointer metadata for a vault reference."""
        record = await self._find_by_reference(reference)
        if record is None or record.uid is None:
            return
        await self._crud.delete(
            record,
            config_index=self._primary_index,
        )

    async def list_by_namespace(
        self,
        namespace: str,
    ) -> Iterable[SecretPointerRecord]:
        """List pointer records for a namespace."""
        results = await self._crud.query(
            SecretPointerRecord,
            {"namespace": namespace},
            config_index=self._primary_index,
        )
        return cast(list[SecretPointerRecord], results)

    async def _find_by_reference(
        self,
        reference: str,
    ) -> SecretPointerRecord | None:
        results = await self._crud.query(
            SecretPointerRecord,
            {"vault_reference": reference},
            config_index=self._primary_index,
        )
        casted = cast(list[SecretPointerRecord], results)
        return casted[0] if casted else None

    async def _refresh(self, uid: str | None) -> SecretPointerRecord:
        if not uid:
            msg = "Pointer record UID is required for refresh"
            raise ValueError(msg)
        refreshed = await self._crud.read(
            SecretPointerRecord,
            uid,
            config_index=self._primary_index,
        )
        if refreshed is None:
            msg = f"Pointer record {uid} disappeared after update"
            raise LookupError(msg)
        return cast(SecretPointerRecord, refreshed)


def parse_classification(value: Any) -> DataClassification | None:
    """Convert arbitrary input into a DataClassification enum."""
    if value is None or isinstance(value, DataClassification):
        return value
    try:
        return DataClassification(value)
    except ValueError:
        for member in DataClassification:
            if member.value.lower() == str(value).lower():
                return member
    return None
