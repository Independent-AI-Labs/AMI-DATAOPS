"""Shared fixtures for the report->intake loopback suite.

Spawns a real uvicorn process bound to 127.0.0.1:<random-port> with the
intake FastAPI app and tears it down after the test. Keeps setup in one
place so the happy-path + negative-path suites stay readable.
"""

from __future__ import annotations

import socket
import threading
import time
from pathlib import Path
from typing import NamedTuple

import httpx
import pytest
import uvicorn

from ami.dataops.intake.app import create_app
from ami.dataops.intake.config import IntakeConfig

SENDER_ID = "alpha"
SHARED_SECRET = "shared-secret"
BEARER_TOKEN = "bearer-token"
HEALTH_TIMEOUT_SECONDS = 5.0
HEALTH_POLL_INTERVAL_SECONDS = 0.05


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LoopbackEnv(NamedTuple):
    """A live intake daemon + a scratch workspace for the sender."""

    intake_root: Path
    source_root: Path
    sender_config_path: Path
    defaults_path: Path
    endpoint: str
    base_url: str


def _wait_for_health(base_url: str) -> None:
    deadline = time.monotonic() + HEALTH_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{base_url}/healthz", timeout=0.5)
        except httpx.HTTPError:
            time.sleep(HEALTH_POLL_INTERVAL_SECONDS)
            continue
        if response.status_code == httpx.codes.OK:
            return
        time.sleep(HEALTH_POLL_INTERVAL_SECONDS)
    msg = f"intake daemon did not become healthy at {base_url}"
    raise RuntimeError(msg)


@pytest.fixture
def loopback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> LoopbackEnv:
    """Spin up uvicorn serving ami-intake, return paths + env for the sender."""
    intake_root = tmp_path / "intake"
    source_root = tmp_path / "source"
    source_root.mkdir()

    monkeypatch.setenv(f"AMI_INTAKE_TOKENS__{SENDER_ID.upper()}", BEARER_TOKEN)
    monkeypatch.setenv(f"AMI_INTAKE_SECRETS__{SENDER_ID.upper()}", SHARED_SECRET)
    monkeypatch.setenv("SECRET_ALPHA", SHARED_SECRET)
    monkeypatch.setenv(f"AMI_REPORT_TOKENS__{SENDER_ID.upper()}", BEARER_TOKEN)
    monkeypatch.delenv("AMI_ROOT", raising=False)

    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"
    endpoint = f"{base_url}/"

    config = IntakeConfig.model_validate(
        {
            "intake_port": port,
            "intake_root": str(intake_root),
            "allowed_senders": [SENDER_ID],
        }
    )
    app = create_app(config)
    server_config = uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning", lifespan="off"
    )
    server = uvicorn.Server(server_config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        _wait_for_health(base_url)
    except RuntimeError:
        server.should_exit = True
        thread.join(timeout=2.0)
        raise

    sender_config_path = tmp_path / "report.yml"
    sender_config_path.write_text(
        "dataops_report_sender_config:\n"
        f"  sender_id: {SENDER_ID}\n"
        "  extra_roots:\n"
        f"    - {source_root}\n"
        "dataops_report_peers:\n"
        f"  - name: {SENDER_ID}\n"
        f"    endpoint: {endpoint}\n"
        "    shared_secret_env_var: SECRET_ALPHA\n"
    )
    defaults_path = tmp_path / "defaults.yml"

    try:
        yield LoopbackEnv(
            intake_root=intake_root,
            source_root=source_root,
            sender_config_path=sender_config_path,
            defaults_path=defaults_path,
            endpoint=endpoint,
            base_url=base_url,
        )
    finally:
        server.should_exit = True
        thread.join(timeout=5.0)
