"""Integration: ami-report send --ci --dry-run assembles a valid signed manifest."""

from __future__ import annotations

from pathlib import Path

import pytest

from ami.dataops.report import cli
from ami.dataops.report.manifest import verify_signature

SENDER = "alpha"
PEER_NAME = "bravo"
SHARED = "shared-secret"
BEARER = "bearer-token"


def _prepare_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    monkeypatch.setenv("SECRET_BRAVO", SHARED)
    monkeypatch.setenv(f"AMI_REPORT_TOKENS__{PEER_NAME.upper()}", BEARER)
    monkeypatch.delenv("AMI_ROOT", raising=False)

    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "app.log").write_text("line-a\nline-b\n")
    (logs / "trace.ndjson").write_text('{"x":1}\n')

    config_path = tmp_path / "report.yml"
    config_path.write_text(
        "dataops_report_sender_config:\n"
        f"  sender_id: {SENDER}\n"
        "  extra_roots:\n"
        f"    - {logs}\n"
        "dataops_report_peers:\n"
        f"  - name: {PEER_NAME}\n"
        "    endpoint: https://intake.bravo.example.com/\n"
        "    shared_secret_env_var: SECRET_BRAVO\n"
    )
    defaults = tmp_path / "defaults.yml"
    defaults.write_text(f"peer: {PEER_NAME}\nfiles:\n  - app.log\n  - trace.ndjson\n")
    return config_path, defaults


class TestCliDryRunIntegration:
    def test_signature_verifies_against_shared_secret(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        config_path, defaults_path = _prepare_env(tmp_path, monkeypatch)
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
        lines = captured.strip().splitlines()
        signature_line = lines[-1]
        manifest_bytes = captured.encode("utf-8").split(b"\nsha256=")[0] + b"\n"
        assert verify_signature(manifest_bytes, signature_line, SHARED)
