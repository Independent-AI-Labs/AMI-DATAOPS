"""Storage configuration registry utilities."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, ConfigDict

from ami.core.exceptions import StorageError
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig
from ami.models.storage_config_factory import StorageConfigFactory

logger = logging.getLogger(__name__)

_SENSITIVE_OPTION_KEYS = (
    "password",
    "secret",
    "token",
    "key",
    "credential",
)


def _sanitize_options(options: dict[str, Any] | None) -> dict[str, Any]:
    if not options:
        return {}
    sanitized: dict[str, Any] = {}
    for key, value in options.items():
        lower_key = key.lower()
        if any(marker in lower_key for marker in _SENSITIVE_OPTION_KEYS):
            sanitized[key] = "***REDACTED***"
        else:
            sanitized[key] = value
    return sanitized


def _iter_storage_models() -> Iterable[type[StorageModel]]:
    seen: set[type[StorageModel]] = set()

    def _walk(
        cls: type[StorageModel],
    ) -> Iterable[type[StorageModel]]:
        for subclass in cls.__subclasses__():
            if subclass in seen:
                continue
            seen.add(subclass)
            yield subclass
            yield from _walk(subclass)

    yield from _walk(StorageModel)


class ModelStorageUsage(BaseModel):
    """Representation of storage usage for a given model."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    model: str
    storages: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {"model": self.model, "storages": self.storages}


class StorageRegistry:
    """Expose storage configuration and model usage metadata."""

    def __init__(self) -> None:
        self._config_cache: dict[str, StorageConfig] | None = None
        self._model_usage_cache: list[ModelStorageUsage] | None = None

    def refresh(self) -> None:
        """Reload configuration and clear caches."""
        self._config_cache = None
        self._model_usage_cache = None

    def list_configs(self) -> dict[str, StorageConfig]:
        """Return mapping of config name to StorageConfig."""
        self._ensure_configs()
        if not self._config_cache:
            msg = "Storage config cache not initialized"
            raise StorageError(msg)
        return dict(self._config_cache)

    def get_config(self, name: str) -> StorageConfig:
        """Return a single storage configuration by name."""
        configs = self.list_configs()
        if name not in configs:
            msg = f"Unknown storage config '{name}'"
            raise KeyError(msg)
        return configs[name]

    def list_config_summaries(self) -> list[dict[str, Any]]:
        """Return sanitized summaries for all storage configs."""
        summaries: list[dict[str, Any]] = []
        for name, config in self.list_configs().items():
            summaries.append(self._summarize_config(name, config))
        return summaries

    def get_model_usage(self) -> list[ModelStorageUsage]:
        """Return cached storage usage per model."""
        if self._model_usage_cache is None:
            self._model_usage_cache = list(self._build_model_usage())
        return list(self._model_usage_cache)

    def get_config_usage_index(self) -> dict[str, list[str]]:
        """Return reverse index mapping config names to models."""
        index: dict[str, list[str]] = {}
        for usage in self.get_model_usage():
            for storage in usage.storages:
                name = storage.get("name") or storage.get("storage_type") or "unknown"
                index.setdefault(name, []).append(usage.model)
        return index

    def _ensure_configs(self) -> None:
        if self._config_cache is not None:
            return
        self._config_cache = {}
        for config in StorageConfigFactory.get_all_configs():
            name = config.name or "unnamed"
            self._config_cache[name] = config

    def _build_model_usage(self) -> Iterable[ModelStorageUsage]:
        for model_cls in _iter_storage_models():
            meta = model_cls.get_metadata()
            if not meta.storage_configs:
                continue
            storage_entries = self._resolve_model_storages(
                meta.storage_configs,
            )
            if not storage_entries:
                continue
            model_name = f"{model_cls.__module__}.{model_cls.__name__}"
            storages = []
            for index, (name, config) in enumerate(storage_entries):
                storages.append(
                    {
                        "name": name,
                        "storage_type": (
                            config.storage_type.value if config.storage_type else None
                        ),
                        "primary": index == 0,
                    },
                )
            yield ModelStorageUsage(
                model=model_name,
                storages=storages,
            )

    @staticmethod
    def _resolve_model_storages(
        storage_configs: Any,
    ) -> list[tuple[str | None, StorageConfig]]:
        entries: list[tuple[str | None, StorageConfig]] = []
        if isinstance(storage_configs, dict):
            for key, config in storage_configs.items():
                name = config.name or key
                entries.append((name, config))
        elif isinstance(storage_configs, list):
            for idx, config in enumerate(storage_configs):
                derived_name = config.name or (
                    config.storage_type.value
                    if config.storage_type
                    else f"storage_{idx}"
                )
                entries.append((derived_name, config))
        return entries

    @staticmethod
    def _summarize_config(
        name: str,
        config: StorageConfig,
    ) -> dict[str, Any]:
        return {
            "name": name,
            "storage_type": (
                config.storage_type.value if config.storage_type else None
            ),
            "host": config.host,
            "port": config.port,
            "database": config.database,
            "options": _sanitize_options(config.options),
            "has_credentials": bool(
                config.username or config.password,
            ),
        }
