"""Unit tests for ami.dataops.intake.audit: hash chain + rotation + verification."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ami.dataops.intake import audit
from ami.dataops.intake.audit import AuditAppendParams


def _params(**overrides: object) -> AuditAppendParams:
    defaults: dict[str, object] = {
        "event": "accept",
        "sender_id": "alpha",
        "bundle_id": "b",
        "remote_addr": "127.0.0.1",
        "byte_count": 0,
        "file_count": 1,
        "reject_reason": None,
        "receipt_sha256": "a" * 64,
    }
    defaults.update(overrides)
    return AuditAppendParams.model_validate(defaults)


def _read_lines(path: Path) -> list[dict]:
    raw = path.read_bytes()
    return [json.loads(line) for line in raw.splitlines() if line]


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class TestComputePrevHash:
    def test_empty_root_returns_genesis(self, tmp_path: Path) -> None:
        assert audit.compute_expected_prev_hash(tmp_path) == audit.GENESIS_PREV_HASH

    def test_after_one_record_returns_hash_of_that_line(self, tmp_path: Path) -> None:
        audit.append_audit_record(tmp_path, _params(bundle_id="b1"))
        last_line = (tmp_path / "audit.log").read_bytes()
        assert audit.compute_expected_prev_hash(tmp_path) == _sha256_hex(last_line)


class TestAppendAuditRecord:
    def test_first_record_has_genesis_prev_hash(self, tmp_path: Path) -> None:
        record, offset = audit.append_audit_record(tmp_path, _params(bundle_id="b1"))
        assert record.prev_hash == audit.GENESIS_PREV_HASH
        assert offset == 0
        lines = _read_lines(tmp_path / "audit.log")
        assert len(lines) == 1
        assert lines[0]["event"] == "accept"

    def test_chain_links_across_appends(self, tmp_path: Path) -> None:
        audit.append_audit_record(tmp_path, _params(bundle_id="b1"))
        r2, offset2 = audit.append_audit_record(
            tmp_path,
            _params(
                event="reject",
                bundle_id="b2",
                byte_count=20,
                file_count=0,
                reject_reason="ext_not_allowed",
                receipt_sha256="b" * 64,
            ),
        )
        first_line = (tmp_path / "audit.log").read_bytes().splitlines(keepends=True)[0]
        assert r2.prev_hash == _sha256_hex(first_line)
        assert offset2 == len(first_line)

    def test_file_mode_is_active(self, tmp_path: Path) -> None:
        audit.append_audit_record(tmp_path, _params())
        mode = (tmp_path / "audit.log").stat().st_mode & 0o777
        assert mode == audit.ACTIVE_LOG_MODE

    def test_reject_record_carries_reject_reason(self, tmp_path: Path) -> None:
        record, _ = audit.append_audit_record(
            tmp_path,
            _params(
                event="reject",
                byte_count=5,
                file_count=0,
                reject_reason="not_text",
                receipt_sha256="c" * 64,
            ),
        )
        assert record.reject_reason == "not_text"


class TestRotateAudit:
    def _seed(self, tmp_path: Path, n: int) -> None:
        for i in range(n):
            audit.append_audit_record(
                tmp_path,
                _params(bundle_id=f"b{i}", byte_count=i, receipt_sha256=f"{i:064x}"),
            )

    def test_rotation_produces_sealed_file_with_seal_record(
        self, tmp_path: Path
    ) -> None:
        self._seed(tmp_path, 2)
        sealed = audit.rotate_audit(tmp_path)
        assert sealed.exists()
        assert not (tmp_path / "audit.log").exists()
        lines = _read_lines(sealed)
        assert lines[-1]["event"] == "seal"
        assert "seal_hash" in lines[-1]

    def test_sealed_file_mode_is_read_only(self, tmp_path: Path) -> None:
        self._seed(tmp_path, 1)
        sealed = audit.rotate_audit(tmp_path)
        mode = sealed.stat().st_mode & 0o777
        assert mode == audit.SEALED_LOG_MODE

    def test_chain_continues_after_rotation(self, tmp_path: Path) -> None:
        self._seed(tmp_path, 1)
        sealed = audit.rotate_audit(tmp_path)
        seal_hash = _read_lines(sealed)[-1]["seal_hash"]
        new_record, _ = audit.append_audit_record(
            tmp_path,
            _params(bundle_id="after", byte_count=1, receipt_sha256="d" * 64),
        )
        assert new_record.prev_hash == seal_hash

    def test_rotation_with_no_active_log_is_noop(self, tmp_path: Path) -> None:
        sealed_path = audit.rotate_audit(tmp_path)
        assert not sealed_path.exists()
        assert not (tmp_path / "audit.log").exists()


class TestVerifyChain:
    def _seed(self, tmp_path: Path, n: int) -> None:
        for i in range(n):
            audit.append_audit_record(
                tmp_path,
                _params(bundle_id=f"b{i}", byte_count=i, receipt_sha256=f"{i:064x}"),
            )

    def test_intact_chain_validates(self, tmp_path: Path) -> None:
        self._seed(tmp_path, 3)
        audit.verify_chain(tmp_path)

    def test_empty_root_validates(self, tmp_path: Path) -> None:
        audit.verify_chain(tmp_path)

    def test_chain_across_rotation_validates(self, tmp_path: Path) -> None:
        self._seed(tmp_path, 2)
        audit.rotate_audit(tmp_path)
        self._seed(tmp_path, 2)
        audit.verify_chain(tmp_path)

    def test_tampered_middle_record_fails(self, tmp_path: Path) -> None:
        self._seed(tmp_path, 3)
        path = tmp_path / "audit.log"
        lines = path.read_bytes().splitlines(keepends=True)
        lines[1] = lines[1].replace(b'"alpha"', b'"ALPHA"')
        path.write_bytes(b"".join(lines))
        with pytest.raises(RuntimeError, match="chain broken"):
            audit.verify_chain(tmp_path)


class TestAuthRejectEvent:
    def test_auth_reject_roundtrips(self, tmp_path: Path) -> None:
        record, _ = audit.append_audit_record(
            tmp_path,
            _params(
                event="auth_reject",
                reject_reason="missing_bearer",
                receipt_sha256="",
                sender_id="<missing>",
                bundle_id="<missing>",
            ),
        )
        assert record.event == "auth_reject"
        assert record.reject_reason == "missing_bearer"
        audit.verify_chain(tmp_path)


class TestAutoRotation:
    def test_exceeds_threshold_triggers_seal(self, tmp_path: Path) -> None:
        # Seed a "big" active log, then append with a small threshold.
        for i in range(5):
            audit.append_audit_record(
                tmp_path,
                _params(bundle_id=f"b{i}", receipt_sha256=f"{i:064x}"),
            )
        active_size = (tmp_path / "audit.log").stat().st_size
        # Threshold strictly below current size so the next append rotates.
        audit.append_audit_record(
            tmp_path,
            _params(bundle_id="trigger", receipt_sha256="f" * 64),
            max_active_bytes=active_size // 2,
        )
        sealed = sorted((tmp_path / "audit").glob("*.log"))
        assert len(sealed) == 1, "threshold should have sealed the active log"
        # Active log must exist and be empty post-rotation.
        assert not (tmp_path / "audit.log").exists()

    def test_under_threshold_no_rotation(self, tmp_path: Path) -> None:
        audit.append_audit_record(
            tmp_path,
            _params(bundle_id="b", receipt_sha256="a" * 64),
            max_active_bytes=10 * 1024 * 1024,
        )
        archive = tmp_path / "audit"
        assert not archive.exists() or not list(archive.glob("*.log"))

    def test_chain_intact_across_auto_rotation(self, tmp_path: Path) -> None:
        audit.append_audit_record(
            tmp_path, _params(bundle_id="b1", receipt_sha256="a" * 64)
        )
        # Tiny threshold forces rotation on the next append.
        audit.append_audit_record(
            tmp_path,
            _params(bundle_id="b2", receipt_sha256="b" * 64),
            max_active_bytes=1,
        )
        audit.append_audit_record(
            tmp_path, _params(bundle_id="b3", receipt_sha256="c" * 64)
        )
        audit.verify_chain(tmp_path)
