"""Unit tests for ami.dataops.intake.cli.

Covers argparse wiring + every non-`serve` subcommand (serve is exercised
indirectly by the integration tests that spawn uvicorn). `serve` itself is
not unit-tested because `uvicorn.run` blocks; that path gets exercised in
commit 12's integration loopback.
"""

from __future__ import annotations

import hashlib
import hmac as _hmac
import io
import tarfile
from pathlib import Path

import pytest
import rfc8785
from fastapi.testclient import TestClient

from ami.dataops.intake import audit, cli
from ami.dataops.intake.app import create_app
from ami.dataops.intake.config import IntakeConfig

SENDER = "alpha"
TOKEN = "tok-secret"
SHARED = "shared-secret"
BUNDLE_ID = "019237d0-2c41-71a5-9f7e-bd6a10b53c07"


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "intake.yml"
    config_path.write_text(
        f"intake_port: 9180\n"
        f"intake_root: {tmp_path / 'root'}\n"
        "allowed_senders:\n"
        "  - alpha\n"
    )
    return config_path


def _seed_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """POST a real bundle through the TestClient so the quarantine + audit log exist."""
    monkeypatch.setenv(f"AMI_INTAKE_TOKENS__{SENDER.upper()}", TOKEN)
    monkeypatch.setenv(f"AMI_INTAKE_SECRETS__{SENDER.upper()}", SHARED)
    cfg = IntakeConfig.model_validate(
        {
            "intake_port": 9180,
            "intake_root": str(tmp_path / "root"),
            "allowed_senders": [SENDER],
        }
    )
    client = TestClient(create_app(cfg))
    payload = b"hello\n"
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(name="a.log")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    manifest = (
        rfc8785.dumps(
            {
                "schema_version": 1,
                "sender_id": SENDER,
                "sent_at": "2026-04-19T08:12:00Z",
                "bundle_id": BUNDLE_ID,
                "source_root": "/tmp/src",
                "files": [
                    {
                        "relative_path": "a.log",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                        "size_bytes": len(payload),
                        "mtime": "2026-04-19T08:11:04Z",
                    }
                ],
            }
        )
        + b"\n"
    )

    signature = (
        "sha256="
        + _hmac.new(SHARED.encode("utf-8"), manifest, hashlib.sha256).hexdigest()
    )
    client.post(
        "/v1/bundles",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "X-AMI-Sender-Id": SENDER,
            "X-AMI-Bundle-Id": BUNDLE_ID,
            "X-AMI-Signature": signature,
        },
        files={
            "manifest": ("m.json", manifest, "application/json"),
            "bundle": ("b.tar.gz", tar_buf.getvalue(), "application/gzip"),
        },
    )


class TestParser:
    def test_serve_requires_config(self) -> None:
        parser = cli.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["serve"])

    def test_rotate_parses(self, tmp_path: Path) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["rotate-audit", "--config", str(tmp_path / "c.yml")])
        assert args.command == "rotate-audit"


class TestDispatch:
    def test_status_missing_audit_log(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_path = _write_config(tmp_path)
        (tmp_path / "root").mkdir()
        rc = cli.main(["status", "--config", str(config_path)])
        assert rc == cli.EXIT_OK
        out = capsys.readouterr().out
        assert "intake_root:" in out
        assert "audit.log:" in out

    def test_ls_and_show_and_verify_roundtrip(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        config_path = _write_config(tmp_path)
        _seed_bundle(tmp_path, monkeypatch)

        assert cli.main(["ls", "--config", str(config_path)]) == cli.EXIT_OK
        ls_out = capsys.readouterr().out
        assert SENDER in ls_out

        assert (
            cli.main(["show", "--config", str(config_path), BUNDLE_ID]) == cli.EXIT_OK
        )
        show_out = capsys.readouterr().out
        assert BUNDLE_ID in show_out

        assert (
            cli.main(["verify", "--config", str(config_path), BUNDLE_ID]) == cli.EXIT_OK
        )

    def test_show_missing_bundle_exits_invalid_args(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_path = _write_config(tmp_path)
        (tmp_path / "root").mkdir()
        rc = cli.main(["show", "--config", str(config_path), "no-such-id"])
        assert rc == cli.EXIT_INVALID_ARGS

    def test_verify_missing_bundle_exits_invalid_args(self, tmp_path: Path) -> None:
        config_path = _write_config(tmp_path)
        (tmp_path / "root").mkdir()
        rc = cli.main(["verify", "--config", str(config_path), "nope"])
        assert rc == cli.EXIT_INVALID_ARGS

    def test_verify_detects_hash_mismatch(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        config_path = _write_config(tmp_path)
        _seed_bundle(tmp_path, monkeypatch)
        target = next((tmp_path / "root" / SENDER).rglob(BUNDLE_ID))
        corrupted = target / "a.log"
        corrupted.chmod(0o640)
        corrupted.write_bytes(b"tampered\n")
        rc = cli.main(["verify", "--config", str(config_path), BUNDLE_ID])
        assert rc == cli.EXIT_VERIFY_MISMATCH

    def test_rotate_seals_active_log(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        config_path = _write_config(tmp_path)
        _seed_bundle(tmp_path, monkeypatch)
        active = tmp_path / "root" / audit.AUDIT_LOG_NAME
        assert active.exists()
        rc = cli.main(["rotate-audit", "--config", str(config_path)])
        assert rc == cli.EXIT_OK
        assert not active.exists()
        out = capsys.readouterr().out
        assert "sealed" in out

    def test_missing_config_file_returns_invalid_args(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = cli.main(["status", "--config", str(tmp_path / "nope.yml")])
        assert rc == cli.EXIT_INVALID_ARGS


class TestFindBundle:
    def test_returns_none_when_intake_root_absent(self, tmp_path: Path) -> None:
        assert cli._find_bundle(tmp_path / "nope", "xyz") is None

    def test_skips_audit_archive_dir(self, tmp_path: Path) -> None:
        (tmp_path / audit.AUDIT_ARCHIVE_DIR).mkdir()
        assert cli._find_bundle(tmp_path, "no-bundle") is None
