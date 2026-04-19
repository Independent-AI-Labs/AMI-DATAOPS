"""Loader for `dataops_intake_config` — the daemon's runtime configuration.

Read from a YAML file (typically rendered by the Ansible playbook) and
coerced into a Pydantic model so every field is typed and validated at
startup. A missing required field aborts `ami-intake serve` before the
ASGI bind; a malformed value raises with a path trace the operator can
copy into the inventory fix.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ami.dataops.intake.validation import (
    DEFAULT_MAX_BUNDLE_BYTES,
    DEFAULT_MAX_FILE_BYTES,
    DEFAULT_MAX_FILES_PER_BUNDLE,
)

MIN_PORT = 1
MAX_PORT = 65535
DEFAULT_GLOBAL_CONCURRENCY = 4
BYTES_PER_MIB = 1024 * 1024


class IntakeConfig(BaseModel):
    """Typed runtime configuration for the intake daemon."""

    model_config = ConfigDict(extra="forbid")

    intake_port: int = Field(ge=MIN_PORT, le=MAX_PORT)
    intake_root: Path
    persist: bool = False
    max_file_mb: int = Field(default=DEFAULT_MAX_FILE_BYTES // BYTES_PER_MIB, gt=0)
    max_bundle_mb: int = Field(default=DEFAULT_MAX_BUNDLE_BYTES // BYTES_PER_MIB, gt=0)
    max_files_per_bundle: int = Field(default=DEFAULT_MAX_FILES_PER_BUNDLE, gt=0)
    global_concurrency: int = Field(default=DEFAULT_GLOBAL_CONCURRENCY, gt=0)
    allowed_senders: list[str] = Field(default_factory=list)

    @field_validator("intake_root")
    @classmethod
    def _resolve_root(cls, value: Path) -> Path:
        return value.expanduser().absolute()

    @field_validator("allowed_senders")
    @classmethod
    def _ensure_unique_senders(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            msg = f"allowed_senders has duplicates: {value}"
            raise ValueError(msg)
        return value

    @property
    def max_file_bytes(self) -> int:
        return self.max_file_mb * BYTES_PER_MIB

    @property
    def max_bundle_bytes(self) -> int:
        return self.max_bundle_mb * BYTES_PER_MIB


def load_intake_config(path: Path) -> IntakeConfig:
    """Parse `path` (YAML) into an `IntakeConfig` or raise with a clear message."""
    if not path.is_file():
        msg = f"intake config file not found: {path}"
        raise FileNotFoundError(msg)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"intake config at {path} is not a YAML mapping"
        raise TypeError(msg)
    return IntakeConfig.model_validate(raw)
