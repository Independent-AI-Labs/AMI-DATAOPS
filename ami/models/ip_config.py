"""Network configuration base model."""

from typing import Any

from pydantic import BaseModel, Field, field_validator

_MAX_PORT = 65535


class IPConfig(BaseModel):
    """Base configuration for network-based services."""

    host: str | None = Field(
        default=None,
        description="Host address or IP",
    )
    port: int | None = Field(default=None, description="Port number")
    username: str | None = Field(
        default=None,
        description="Username for authentication",
    )
    password: str | None = Field(
        default=None,
        description="Password for authentication",
    )
    timeout: int | None = Field(
        default=30,
        description="Connection timeout in seconds",
    )
    options: dict[str, Any] | None = Field(
        default_factory=dict,
        description="Additional connection options",
    )

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str | None) -> str | None:
        """Validate host is not empty."""
        if v and not v.strip():
            msg = "Host cannot be empty"
            raise ValueError(msg)
        return v.strip() if v else v

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int | None) -> int | None:
        """Validate port is in valid range."""
        if v is not None and (v < 1 or v > _MAX_PORT):
            msg = f"Port must be between 1 and {_MAX_PORT}, got {v}"
            raise ValueError(msg)
        return v

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int | None) -> int | None:
        """Validate timeout is positive."""
        if v is not None and v <= 0:
            msg = f"Timeout must be positive, got {v}"
            raise ValueError(msg)
        return v
