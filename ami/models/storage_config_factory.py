"""Factory for creating StorageConfig instances from YAML configuration."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml

from ami.core.storage_types import StorageType
from ami.models.storage_config import StorageConfig

_logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = os.environ.get(
    "AMI_DATAOPS_PROJECT_ROOT",
    str(
        Path(__file__).resolve().parents[2],
    ),
)
_DEFAULT_CONFIG = Path(_DEFAULT_CONFIG_PATH) / "config" / "storage-config.yaml"

_TYPE_MAPPING: dict[str, StorageType] = {
    "graph": StorageType.GRAPH,
    "inmem": StorageType.INMEM,
    "relational": StorageType.RELATIONAL,
    "vector": StorageType.VECTOR,
    "document": StorageType.DOCUMENT,
    "timeseries": StorageType.TIMESERIES,
    "vault": StorageType.VAULT,
    "rest": StorageType.REST,
}

_yaml_cache: dict[str, dict[str, Any] | None] = {"data": None}


def _load_yaml() -> dict[str, Any]:
    """Load and cache the storage-config.yaml file."""
    cached = _yaml_cache["data"]
    if cached is not None:
        return cached

    config_path = os.environ.get(
        "AMI_DATAOPS_CONFIG",
        str(_DEFAULT_CONFIG),
    )
    path = Path(config_path)
    if not path.exists():
        msg = f"Storage config not found: {path}"
        raise FileNotFoundError(msg)

    with path.open() as fh:
        raw: dict[str, Any] = yaml.safe_load(fh) or {}

    _yaml_cache["data"] = raw
    return raw


class StorageConfigFactory:
    """Factory for creating StorageConfig from storage-config.yaml."""

    @staticmethod
    def from_yaml(storage_name: str) -> StorageConfig:
        """Create a StorageConfig from YAML configuration."""
        raw = _load_yaml()
        configs = raw.get("storage_configs", {})
        yaml_config: dict[str, Any] | None = configs.get(storage_name)
        if yaml_config is None:
            msg = (
                f"Storage config '{storage_name}' not found in YAML. "
                f"Available: {list(configs.keys())}"
            )
            raise KeyError(msg)

        yaml_config = {**yaml_config, "name": storage_name}
        config_type = yaml_config.get("type")
        if config_type is None:
            msg = f"Storage entry '{storage_name}' missing required 'type'"
            raise ValueError(msg)

        storage_type = _TYPE_MAPPING.get(config_type)
        if storage_type is None:
            msg = f"Unsupported storage type '{config_type}' in entry '{storage_name}'"
            raise ValueError(msg)

        database = yaml_config.get("database")
        if database is not None and isinstance(database, int):
            database = str(database)

        return StorageConfig(
            name=storage_name,
            storage_type=storage_type,
            host=yaml_config.get("host"),
            port=yaml_config.get("port"),
            database=str(database) if database is not None else None,
            username=yaml_config.get("username"),
            password=yaml_config.get("password"),
            options=yaml_config.get("options", {}),
        )

    @staticmethod
    def get_all_configs() -> list[StorageConfig]:
        """Get all storage configs from YAML."""
        raw = _load_yaml()
        configs = raw.get("storage_configs", {})
        results: list[StorageConfig] = []
        for name in configs:
            try:
                results.append(
                    StorageConfigFactory.from_yaml(name),
                )
            except (KeyError, ValueError):
                _logger.debug(
                    "Skipping invalid config: %s",
                    name,
                )
        return results
