"""Unit tests for ami.dataops.report.cli (non-interactive paths)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from ami.dataops.report import cli, pipeline
from ami.dataops.report.manifest import verify_signature
from ami.dataops.report.pipeline import _common_source_root
from ami.dataops.report.scanner import CandidateFile

SENDER = "alpha"
PEER_NAME = "bravo"
SHARED = "shared-secret"
BEARER = "bearer-token"
PEER_ENDPOINT = "https://intake.bravo.example.com/"
BUNDLES_URL = f"{PEER_ENDPOINT}v1/bundles"


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / "report.yml"
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "app.log").write_text("hello\n")
    (logs_dir / "trace.ndjson").write_text('{"x":1}\n')
    path.write_text(
        "dataops_report_sender_config:\n"
        f"  sender_id: {SENDER}\n"
        "  extra_roots:\n"
        f"    - {logs_dir}\n"
        "dataops_report_peers:\n"
        f"  - name: {PEER_NAME}\n"
        f"    endpoint: {PEER_ENDPOINT}\n"
        "    shared_secret_env_var: SECRET_BRAVO\n"
    )
    return path


@pytest.fixture(autouse=True)
def _clear_ami_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("AMI_ROOT", raising=False)
    monkeypatch.setenv("SECRET_BRAVO", SHARED)
    monkeypatch.setenv(f"AMI_REPORT_TOKENS__{PEER_NAME.upper()}", BEARER)


class TestPeersCommand:
    def test_lists_peers_with_env_state(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_path = _write_config(tmp_path)
        rc = cli.main(["peers", "--config", str(config_path)])
        assert rc == cli.EXIT_OK
        out = capsys.readouterr().out
        assert PEER_NAME in out
        assert "token: set" in out
        assert "secret: set" in out


class TestPreviewCommand:
    def test_lists_candidates(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_path = _write_config(tmp_path)
        rc = cli.main(["preview", "--config", str(config_path)])
        assert rc == cli.EXIT_OK
        out = capsys.readouterr().out
        assert "app.log" in out
        # Extension mismatches are dropped at scan time; preview omits them.
        assert "trace.ndjson" not in out


class TestSendDryRun:
    def test_ci_dry_run_prints_canonical_manifest_and_signature(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_path = _write_config(tmp_path)
        defaults_path = tmp_path / "defaults.yml"
        defaults_path.write_text(
            f"peer: {PEER_NAME}\nfiles:\n  - app.log\n  - trace.ndjson\n"
        )
        rc = cli.main(
            [
                "send",
                "--config",
                str(config_path),
                "--ci",
                "--defaults",
                str(defaults_path),
                "--dry-run",
            ]
        )
        assert rc == cli.EXIT_OK
        captured = capsys.readouterr().out
        signature_line = captured.strip().splitlines()[-1]
        assert signature_line.startswith("sha256=")
        manifest_bytes = captured.encode("utf-8").split(b"\nsha256=")[0] + b"\n"
        assert verify_signature(manifest_bytes, signature_line, SHARED)


class TestSendNonInteractive:
    @respx.mock
    def test_ci_happy_path_posts_and_prints_receipt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        respx.post(BUNDLES_URL).mock(
            return_value=httpx.Response(
                202,
                json={
                    "status": "accept",
                    "bundle_id": "x",
                    "received_at": "2026-04-19T08:12:01Z",
                    "per_file_sha256_verified": [],
                    "audit_log_offset": 0,
                },
            )
        )
        config_path = _write_config(tmp_path)
        defaults_path = tmp_path / "defaults.yml"
        defaults_path.write_text(
            f"peer: {PEER_NAME}\nfiles:\n  - app.log\n  - trace.ndjson\n"
        )
        rc = cli.main(
            [
                "send",
                "--config",
                str(config_path),
                "--ci",
                "--defaults",
                str(defaults_path),
            ]
        )
        assert rc == cli.EXIT_OK
        printed = json.loads(capsys.readouterr().out)
        assert printed["status"] == "accept"

    @respx.mock
    def test_ci_401_returns_auth_exit(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        respx.post(BUNDLES_URL).mock(
            return_value=httpx.Response(401, json={"status": "reject"})
        )
        config_path = _write_config(tmp_path)
        defaults_path = tmp_path / "defaults.yml"
        defaults_path.write_text(f"peer: {PEER_NAME}\nfiles:\n  - app.log\n")
        rc = cli.main(
            [
                "send",
                "--config",
                str(config_path),
                "--ci",
                "--defaults",
                str(defaults_path),
            ]
        )
        assert rc == pipeline.EXIT_AUTH_REJECTED

    def test_ci_without_defaults_errors(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_path = _write_config(tmp_path)
        rc = cli.main(["send", "--config", str(config_path), "--ci"])
        assert rc == cli.EXIT_INVALID_ARGS
        err = capsys.readouterr().err
        assert "--defaults" in err


class TestCommonSourceRoot:
    def test_finds_closest_common_ancestor(self, tmp_path: Path) -> None:
        a = tmp_path / "logs" / "a.log"
        b = tmp_path / "logs" / "nested" / "b.log"
        a.parent.mkdir(parents=True)
        b.parent.mkdir(parents=True)
        a.write_text("1\n")
        b.write_text("2\n")
        cands = [
            CandidateFile(
                absolute_path=a, relative_path="a.log", size_bytes=2, preflight="ok"
            ),
            CandidateFile(
                absolute_path=b,
                relative_path="nested/b.log",
                size_bytes=2,
                preflight="ok",
            ),
        ]
        assert _common_source_root(cands) == tmp_path / "logs"
