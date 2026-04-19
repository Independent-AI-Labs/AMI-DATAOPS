"""Integration: exercise the full FastAPI intake pipeline via TestClient.

Sits in tests/integration/ because it drives the whole validate-quarantine-
audit-respond pipeline through the ASGI stack with a real tarball + real
manifest + real HMAC. The unit tests cover each branch in isolation; this
file proves the pieces compose correctly.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import tarfile
from pathlib import Path

import pytest
import rfc8785
from fastapi import status
from fastapi.testclient import TestClient

from ami.dataops.intake import audit, quarantine
from ami.dataops.intake.app import create_app
from ami.dataops.intake.config import IntakeConfig

SENDER = "alpha"
TOKEN = "tok-secret"
SHARED = "shared-secret"
BUNDLE_ID = "019237d0-2c41-71a5-9f7e-bd6a10b53c07"
SECOND_BUNDLE_ID = "019237d0-2c41-71a5-9f7e-bd6a10b53c99"
_EXPECTED_BUNDLES_IN_TEST = 2


def _make_config(tmp_path: Path) -> IntakeConfig:
    return IntakeConfig.model_validate(
        {
            "intake_port": 9180,
            "intake_root": str(tmp_path),
            "allowed_senders": [SENDER],
        }
    )


def _build_tarball(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, payload in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    return buf.getvalue()


def _build_manifest(files: dict[str, bytes], *, bundle_id: str) -> bytes:
    manifest_dict = {
        "schema_version": 1,
        "sender_id": SENDER,
        "sent_at": "2026-04-19T08:12:00Z",
        "bundle_id": bundle_id,
        "source_root": "/tmp/src",
        "files": [
            {
                "relative_path": name,
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "mtime": "2026-04-19T08:11:04Z",
            }
            for name, payload in files.items()
        ],
    }
    return rfc8785.dumps(manifest_dict) + b"\n"


def _sign(manifest_bytes: bytes) -> str:
    digest = hmac.new(
        SHARED.encode("utf-8"), manifest_bytes, hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


def _post(client: TestClient, manifest: bytes, tarball: bytes, bundle_id: str):
    return client.post(
        "/v1/bundles",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "X-AMI-Sender-Id": SENDER,
            "X-AMI-Bundle-Id": bundle_id,
            "X-AMI-Signature": _sign(manifest),
        },
        files={
            "manifest": ("m.json", manifest, "application/json"),
            "bundle": ("b.tar.gz", tarball, "application/gzip"),
        },
    )


@pytest.fixture(autouse=True)
def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"AMI_INTAKE_TOKENS__{SENDER.upper()}", TOKEN)
    monkeypatch.setenv(f"AMI_INTAKE_SECRETS__{SENDER.upper()}", SHARED)


class TestIntakeE2E:
    def test_two_bundles_chain_audit_and_populate_quarantine(
        self, tmp_path: Path
    ) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))

        files_a = {"a.log": b"alpha batch one\n"}
        manifest_a = _build_manifest(files_a, bundle_id=BUNDLE_ID)
        response = _post(client, manifest_a, _build_tarball(files_a), BUNDLE_ID)
        assert response.status_code == status.HTTP_202_ACCEPTED

        files_b = {"b.log": b"alpha batch two\n", "c.txt": b"note\n"}
        manifest_b = _build_manifest(files_b, bundle_id=SECOND_BUNDLE_ID)
        response = _post(client, manifest_b, _build_tarball(files_b), SECOND_BUNDLE_ID)
        assert response.status_code == status.HTTP_202_ACCEPTED

        assert quarantine.bundle_exists(tmp_path, SENDER, BUNDLE_ID) is not None
        assert quarantine.bundle_exists(tmp_path, SENDER, SECOND_BUNDLE_ID) is not None

        audit_lines = (tmp_path / "audit.log").read_bytes().splitlines()
        assert len(audit_lines) == _EXPECTED_BUNDLES_IN_TEST
        first = json.loads(audit_lines[0])
        second = json.loads(audit_lines[1])
        assert first["prev_hash"] == audit.GENESIS_PREV_HASH
        assert second["prev_hash"] != audit.GENESIS_PREV_HASH
        audit.verify_chain(tmp_path)

    def test_metrics_and_healthz_reachable(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        assert client.get("/healthz").status_code == status.HTTP_200_OK
        response = client.get("/metrics")
        assert response.status_code == status.HTTP_200_OK
