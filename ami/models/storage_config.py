"""Storage configuration model."""

from collections.abc import Callable
from typing import Any

from pydantic import Field

from ami.core.storage_types import StorageType
from ami.models.ip_config import IPConfig

_HTTPS_PORT = 443


class StorageConfig(IPConfig):
    """Configuration for storage backends.

    Extends IPConfig with storage-specific fields.
    """

    name: str | None = Field(
        default=None,
        description="Canonical storage config name",
    )
    storage_type: StorageType | None = Field(
        default=None,
        description="Type of storage backend",
    )
    connection_string: str | None = Field(
        default=None,
        description="Connection string override",
    )
    database: str | None = Field(
        default=None,
        description="Database name",
    )
    ttl_seconds: int | None = Field(
        default=None,
        description="TTL in seconds for cache expiry",
    )

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "StorageConfig":
        """Create StorageConfig from dictionary (e.g., from YAML)."""
        type_mapping: dict[str, StorageType] = {
            "graph": StorageType.GRAPH,
            "vector": StorageType.VECTOR,
            "relational": StorageType.RELATIONAL,
            "document": StorageType.DOCUMENT,
            "inmem": StorageType.INMEM,
            "timeseries": StorageType.TIMESERIES,
            "rest": StorageType.REST,
            "vault": StorageType.VAULT,
        }
        raw_type = config_dict.get("type", "")
        storage_type = type_mapping.get(raw_type)
        if not storage_type:
            msg = f"Unknown storage type: {raw_type}"
            raise ValueError(msg)

        database = config_dict.get("database")
        if database is not None:
            database = str(database)

        return cls(
            name=config_dict.get("name"),
            storage_type=storage_type,
            host=config_dict.get("host"),
            port=config_dict.get("port"),
            database=database,
            username=config_dict.get("username"),
            password=config_dict.get("password"),
            options=config_dict.get("options", {}),
        )

    def model_post_init(self, /, __context: Any) -> None:
        """Set default ports if not specified."""
        super().model_post_init(__context)
        if self.port is None and self.storage_type:
            default_ports: dict[StorageType, int] = {
                StorageType.RELATIONAL: 5432,
                StorageType.DOCUMENT: 27017,
                StorageType.TIMESERIES: 9090,
                StorageType.VECTOR: 5432,
                StorageType.GRAPH: 9080,
                StorageType.INMEM: 6379,
                StorageType.REST: _HTTPS_PORT,
                StorageType.VAULT: 8200,
            }
            self.port = default_ports.get(self.storage_type)

    def get_connection_string(self) -> str:
        """Generate connection string from components."""
        if self.connection_string:
            return self.connection_string
        if self.storage_type is None:
            msg = "Storage type not set"
            raise ValueError(msg)

        formatters: dict[StorageType, Callable[[], str]] = {
            StorageType.RELATIONAL: lambda: (
                f"postgresql+asyncpg://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            ),
            StorageType.VECTOR: lambda: (
                f"postgresql+asyncpg://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            ),
            StorageType.DOCUMENT: lambda: (
                f"mongodb://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            ),
            StorageType.INMEM: lambda: (
                f"redis://{self.host}:{self.port}/{self.database or 0}"
            ),
            StorageType.GRAPH: lambda: f"{self.host}:{self.port}",
            StorageType.TIMESERIES: lambda: f"http://{self.host}:{self.port}",
            StorageType.REST: self._get_rest_url,
        }
        formatter = formatters.get(self.storage_type)
        if not formatter:
            msg = f"Unsupported storage type: {self.storage_type}"
            raise ValueError(msg)
        return formatter()

    def _get_rest_url(self) -> str:
        protocol = "https" if self.port == _HTTPS_PORT else "http"
        return f"{protocol}://{self.host}:{self.port}/{self.database or ''}"
