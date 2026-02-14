"""Storage configuration validator for DataOps."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from ami.core.dao import DAOFactory
from ami.core.exceptions import StorageError
from ami.core.storage_types import StorageType
from ami.models.base_model import ModelMetadata, StorageModel
from ami.models.storage_config import StorageConfig
from ami.storage.registry import (
    ModelStorageUsage,
    StorageRegistry,
)

logger = logging.getLogger(__name__)

_STORAGE_REQUIRED_FIELDS: dict[StorageType, list[str]] = {
    StorageType.RELATIONAL: [
        "host",
        "port",
        "database",
        "username",
        "password",
    ],
    StorageType.VECTOR: [
        "host",
        "port",
        "database",
        "username",
        "password",
    ],
    StorageType.DOCUMENT: ["host", "port", "database"],
    StorageType.GRAPH: ["host", "port"],
    StorageType.INMEM: ["host", "port"],
    StorageType.TIMESERIES: ["host", "port"],
    StorageType.VAULT: ["host", "port", "options.token"],
    StorageType.REST: ["host"],
}


class StorageValidationResult(BaseModel):
    """Validation outcome for a storage backend."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    storage_type: str | None
    status: str
    details: str | None = None
    missing_fields: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "storage_type": self.storage_type,
            "status": self.status,
            "details": self.details,
            "missing_fields": self.missing_fields,
            "models": self.models,
        }


class StorageValidator:
    """Validate connectivity and configuration for storage backends."""

    def __init__(
        self,
        registry: StorageRegistry | None = None,
        dao_factory: (Callable[[type[StorageModel], StorageConfig], Any] | None) = None,
    ) -> None:
        self._registry = registry or StorageRegistry()
        self._dao_factory = dao_factory or DAOFactory.create

    async def validate_all(
        self,
        names: Iterable[str] | None = None,
    ) -> list[StorageValidationResult]:
        configs = self._registry.list_configs()
        usage_index = self._registry.get_config_usage_index()

        selected_names = list(names) if names is not None else list(configs.keys())
        results: list[StorageValidationResult] = []
        for name in selected_names:
            config = configs.get(name)
            if config is None:
                results.append(
                    StorageValidationResult(
                        name=name,
                        storage_type=None,
                        status="error",
                        details="Unknown storage config",
                    ),
                )
                continue
            models = usage_index.get(name, [])
            results.append(
                await self._validate_single(name, config, models),
            )
        return results

    async def validate_for_model(
        self,
        model_name: str,
    ) -> list[StorageValidationResult]:
        usage = self._registry.get_model_usage()
        matched: list[ModelStorageUsage] = [
            u
            for u in usage
            if u.model == model_name or u.model.endswith(f".{model_name}")
        ]
        if not matched:
            return []
        config_names: set[str] = set()
        for entry in matched:
            for storage in entry.storages:
                name = storage.get("name") or storage.get("storage_type")
                if name:
                    config_names.add(name)
        return await self.validate_all(sorted(config_names))

    async def _validate_single(
        self,
        name: str,
        config: StorageConfig,
        models: list[str],
    ) -> StorageValidationResult:
        missing = self._missing_required_fields(config)
        storage_type = config.storage_type.value if config.storage_type else None
        if missing:
            return StorageValidationResult(
                name=name,
                storage_type=storage_type,
                status="error",
                details="Missing required configuration fields",
                missing_fields=missing,
                models=models,
            )

        dao = self._build_validation_dao(config)
        try:
            await dao.connect()
            ok = await dao.test_connection()
            status = "ok" if ok else "error"
            details = None if ok else "DAO reported failed connection"
        except StorageError as exc:
            status = "error"
            details = str(exc)
        except Exception as exc:
            status = "error"
            details = f"Unexpected error: {exc}"
        finally:
            try:
                await dao.disconnect()
            except Exception:
                logger.warning(
                    "Failed to disconnect DAO during validation cleanup for %s",
                    name,
                )

        return StorageValidationResult(
            name=name,
            storage_type=storage_type,
            status=status,
            details=details,
            models=models,
        )

    def _build_validation_dao(self, config: StorageConfig) -> Any:
        class _ValidationModel(StorageModel):
            _model_meta: ClassVar[ModelMetadata] = ModelMetadata(
                storage_configs={"validation": config},
            )

        return self._dao_factory(_ValidationModel, config)

    @staticmethod
    def _is_field_missing(
        config: StorageConfig,
        field_name: str,
    ) -> bool:
        """Check if a required field is missing or empty."""
        if field_name.startswith("options."):
            option_key = field_name.split(".", 1)[1]
            value = (config.options or {}).get(option_key) if config.options else None
        else:
            value = getattr(config, field_name, None)
        return value in (None, "")

    @staticmethod
    def _missing_required_fields(
        config: StorageConfig,
    ) -> list[str]:
        if config.storage_type is None:
            return ["storage_type"]
        required_fields = _STORAGE_REQUIRED_FIELDS.get(
            config.storage_type,
            [],
        )
        return [
            f for f in required_fields if StorageValidator._is_field_missing(config, f)
        ]


async def validate_async(
    names: Iterable[str] | None = None,
) -> list[StorageValidationResult]:
    """Convenience helper for async validation."""
    validator = StorageValidator()
    return await validator.validate_all(names)


def validate(
    names: Iterable[str] | None = None,
) -> list[StorageValidationResult]:
    """Run validation synchronously using asyncio."""
    return asyncio.run(validate_async(names))
