"""Structured JSONL access log for ami-intake, rotated by size.

One `AccessLogEntry` per HTTP transaction (except paths in
`access_log_exclude_paths` — healthz/metrics by default) lands in
`intake_root/access.log`, rotated via stdlib `RotatingFileHandler` at
`max_access_bytes` with `access_log_backups` kept. Nothing here is
cryptographically chained — the audit log is where the signal lives;
this module is the haystack operators grep with `jq`.

Designed to take the same `IntakeConfig` the rest of the daemon uses
so middleware wiring stays a single `configure(config)` call at app
startup + `shutdown()` at lifespan exit.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from ami.dataops.intake.config import IntakeConfig

ACCESS_LOG_NAME = "access.log"
_LOGGER_NAME = "ami.dataops.intake.access"
_CONFIGURE_LOCK = threading.Lock()


class AccessLogEntry(BaseModel):
    """One row in the JSONL access log."""

    model_config = ConfigDict(extra="forbid")

    ts: str
    method: str
    path: str
    status: int
    remote_addr: str
    duration_ms: int
    sender_id: str | None = None
    bundle_id: str | None = None
    reject_reason: str | None = None
    bytes_in: int | None = None


class AccessLogger:
    """Single-file sink for `AccessLogEntry` records with size-capped rotation."""

    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int,
        backup_count: int,
        exclude_paths: list[str],
    ) -> None:
        self._path = path
        self._exclude = frozenset(exclude_paths)
        self._logger = logging.getLogger(_LOGGER_NAME)
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        # Replace any prior handler attached by a previous app lifespan.
        for handler in list(self._logger.handlers):
            self._logger.removeHandler(handler)
            handler.close()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handler = RotatingFileHandler(
            path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        self._handler.setFormatter(logging.Formatter("%(message)s"))
        self._logger.addHandler(self._handler)

    def should_log(self, path: str) -> bool:
        return path not in self._exclude

    def write(self, entry: AccessLogEntry) -> None:
        self._logger.info(entry.model_dump_json())

    def shutdown(self) -> None:
        self._logger.removeHandler(self._handler)
        self._handler.close()

    @property
    def path(self) -> Path:
        return self._path


class _State:
    """Mutable module-level container so we avoid `global` ceremony."""

    logger: AccessLogger | None = None


_STATE = _State()


def configure(config: IntakeConfig) -> AccessLogger:
    """Install the singleton access logger. Safe to call twice; second wins."""
    with _CONFIGURE_LOCK:
        if _STATE.logger is not None:
            _STATE.logger.shutdown()
        _STATE.logger = AccessLogger(
            config.intake_root / ACCESS_LOG_NAME,
            max_bytes=config.max_access_bytes,
            backup_count=config.access_log_backups,
            exclude_paths=config.access_log_exclude_paths,
        )
        return _STATE.logger


def shutdown() -> None:
    """Close the singleton — used from app lifespan shutdown."""
    with _CONFIGURE_LOCK:
        if _STATE.logger is None:
            return
        _STATE.logger.shutdown()
        _STATE.logger = None


def get() -> AccessLogger | None:
    """Return the active logger (or None if `configure` was never called)."""
    return _STATE.logger


def now_rfc3339() -> str:
    """Module-level helper so middleware can mint consistent timestamps."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_entries(path: Path) -> list[AccessLogEntry]:
    """Parse `path` (the active log) into typed entries — testing + inspection."""
    if not path.is_file():
        return []
    out: list[AccessLogEntry] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        out.append(AccessLogEntry.model_validate(json.loads(stripped)))
    return out
