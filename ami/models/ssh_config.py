"""SSH configuration model."""

import re
from typing import Any

from pydantic import Field, field_validator

from ami.models.ip_config import IPConfig

_DEFAULT_SSH_PORT = 22
_DEFAULT_TIMEOUT = 30


class SSHConfig(IPConfig):
    """Configuration for SSH connections."""

    name: str | None = Field(
        default=None,
        description="Server name/identifier",
    )
    description: str | None = Field(
        default=None,
        description="Server description",
    )
    port: int | None = Field(
        default=_DEFAULT_SSH_PORT,
        description="SSH port",
    )
    key_filename: str | None = Field(
        default=None,
        description="Path to SSH private key",
    )
    passphrase: str | None = Field(
        default=None,
        description="Passphrase for SSH key",
    )
    known_hosts_file: str | None = Field(
        default=None,
        description="Path to known_hosts file",
    )
    allow_agent: bool | None = Field(
        default=True,
        description="Allow SSH agent for authentication",
    )
    look_for_keys: bool | None = Field(
        default=True,
        description="Look for SSH keys in default locations",
    )
    compression: bool | None = Field(
        default=False,
        description="Enable SSH compression",
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str | None) -> str | None:
        """Validate server name."""
        if v and not v.strip():
            msg = "Server name cannot be empty"
            raise ValueError(msg)
        if v:
            if not re.match(r"^[a-zA-Z0-9_-]+$", v):
                msg = (
                    "Server name must contain only alphanumeric "
                    f"characters, underscores, and hyphens: {v}"
                )
                raise ValueError(msg)
            return v.strip()
        return v

    def to_paramiko_config(self) -> dict[str, Any]:
        """Convert to Paramiko connection parameters."""
        config: dict[str, Any] = {
            "hostname": self.host,
            "port": self.port or _DEFAULT_SSH_PORT,
            "username": self.username,
            "timeout": self.timeout or _DEFAULT_TIMEOUT,
            "compress": self.compression or False,
            "allow_agent": (self.allow_agent if self.allow_agent is not None else True),
            "look_for_keys": (
                self.look_for_keys if self.look_for_keys is not None else True
            ),
        }
        if self.password:
            config["password"] = self.password
        if self.key_filename:
            config["key_filename"] = self.key_filename
        if self.passphrase:
            config["passphrase"] = self.passphrase
        if self.known_hosts_file:
            config["known_hosts_filename"] = self.known_hosts_file
        return config
