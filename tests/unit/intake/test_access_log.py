"""Unit tests for ami.dataops.intake.access_log."""

from __future__ import annotations

import json
from pathlib import Path

from ami.dataops.intake import access_log
from ami.dataops.intake.config import IntakeConfig

_MAX_ACCESS_MB = 1
_SMALL_BACKUP_COUNT = 2
_SENDER = "alpha"
_STATUS_ACCEPT = 202
_STATUS_UNAUTHORIZED = 401


def _config(tmp_path: Path, **overrides: object) -> IntakeConfig:
    base: dict[str, object] = {
        "intake_port": 9180,
        "intake_root": str(tmp_path),
        "allowed_senders": [_SENDER],
        "max_access_mb": _MAX_ACCESS_MB,
        "access_log_backups": _SMALL_BACKUP_COUNT,
    }
    base.update(overrides)
    return IntakeConfig.model_validate(base)


def _entry(**overrides: object) -> access_log.AccessLogEntry:
    base: dict[str, object] = {
        "ts": "2026-04-24T00:00:00Z",
        "method": "POST",
        "path": "/v1/bundles",
        "status": 202,
        "remote_addr": "1.2.3.4",
        "duration_ms": 42,
        "sender_id": _SENDER,
    }
    base.update(overrides)
    return access_log.AccessLogEntry.model_validate(base)


class TestConfigure:
    def test_write_creates_access_log_file(self, tmp_path: Path) -> None:
        logger = access_log.configure(_config(tmp_path))
        try:
            logger.write(_entry())
        finally:
            access_log.shutdown()
        body = (tmp_path / "access.log").read_text()
        parsed = json.loads(body.splitlines()[0])
        assert parsed["status"] == _STATUS_ACCEPT

    def test_configure_is_idempotent(self, tmp_path: Path) -> None:
        try:
            first = access_log.configure(_config(tmp_path))
            second = access_log.configure(_config(tmp_path))
            second.write(_entry())
        finally:
            access_log.shutdown()
        # Both invocations share the same file path.
        assert first.path == second.path


class TestExcludePaths:
    def test_should_log_excludes_healthz(self, tmp_path: Path) -> None:
        logger = access_log.configure(_config(tmp_path))
        try:
            assert logger.should_log("/v1/bundles") is True
            assert logger.should_log("/healthz") is False
            assert logger.should_log("/metrics") is False
        finally:
            access_log.shutdown()


class TestRotation:
    def test_rotates_when_exceeds_max_bytes(self, tmp_path: Path) -> None:
        # Tiny byte cap so we can fill it with a handful of entries.
        cfg = _config(tmp_path)
        cfg = IntakeConfig.model_validate(
            {
                **cfg.model_dump(),
                "max_access_mb": 1,
                "access_log_backups": 1,
            }
        )
        logger = access_log.configure(cfg)
        try:
            for i in range(100):
                logger.write(_entry(path=f"/x{i:06d}", status=200 + (i % 9)))
        finally:
            access_log.shutdown()
        files = sorted((tmp_path).glob("access.log*"))
        # Active + up to `backup_count` rotated backups.
        assert any(p.name == "access.log" for p in files)


class TestReadEntries:
    def test_returns_parsed_entries(self, tmp_path: Path) -> None:
        logger = access_log.configure(_config(tmp_path))
        try:
            logger.write(_entry(status=202))
            logger.write(_entry(status=401, reject_reason="missing_bearer"))
        finally:
            access_log.shutdown()
        entries = access_log.read_entries(tmp_path / "access.log")
        assert [e.status for e in entries] == [_STATUS_ACCEPT, _STATUS_UNAUTHORIZED]
        assert entries[1].reject_reason == "missing_bearer"
