"""Unit tests for the FastAPI intake application.

Uses Starlette's TestClient: proxies through the full ASGI stack (headers,
multipart parsing, validation, quarantine, audit) without spawning uvicorn.
Env vars for per-sender tokens and secrets are monkeypatched.
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

EXPECTED_FILES_IN_HAPPY_TEST = 2

SENDER = "alpha"
TOKEN = "tok-secret"
SHARED = "shared-secret"
BUNDLE_ID = "019237d0-2c41-71a5-9f7e-bd6a10b53c07"


def _make_config(tmp_path: Path, max_bundle_mb: int = 500) -> IntakeConfig:
    return IntakeConfig.model_validate(
        {
            "intake_port": 9180,
            "intake_root": str(tmp_path),
            "allowed_senders": [SENDER],
            "max_bundle_mb": max_bundle_mb,
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


def _build_manifest(files: dict[str, bytes], *, bundle_id: str = BUNDLE_ID) -> bytes:
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


def _headers(signature: str, bundle_id: str = BUNDLE_ID) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {TOKEN}",
        "X-AMI-Sender-Id": SENDER,
        "X-AMI-Bundle-Id": bundle_id,
        "X-AMI-Signature": signature,
    }


def _post(
    client: TestClient,
    manifest_bytes: bytes,
    tarball: bytes,
    signature: str,
    bundle_id: str = BUNDLE_ID,
):
    return client.post(
        "/v1/bundles",
        headers=_headers(signature, bundle_id),
        files={
            "manifest": ("manifest.json", manifest_bytes, "application/json"),
            "bundle": ("bundle.tar.gz", tarball, "application/gzip"),
        },
    )


@pytest.fixture(autouse=True)
def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"AMI_INTAKE_TOKENS__{SENDER.upper()}", TOKEN)
    monkeypatch.setenv(f"AMI_INTAKE_SECRETS__{SENDER.upper()}", SHARED)


class TestHealthAndMetrics:
    def test_healthz(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        response = client.get("/healthz")
        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"status": "ok"}

    def test_metrics_exposes_prometheus(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        response = client.get("/metrics")
        assert response.status_code == status.HTTP_200_OK
        assert "text/plain" in response.headers["content-type"]


class TestAuth:
    def test_missing_auth_is_401(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        response = client.post("/v1/bundles")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_wrong_bearer_is_401(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"a.log": b"x\n"}
        manifest = _build_manifest(files)
        response = client.post(
            "/v1/bundles",
            headers={
                "Authorization": "Bearer wrong",
                "X-AMI-Sender-Id": SENDER,
                "X-AMI-Bundle-Id": BUNDLE_ID,
                "X-AMI-Signature": _sign(manifest),
            },
            files={
                "manifest": ("m.json", manifest, "application/json"),
                "bundle": ("b.tar.gz", _build_tarball(files), "application/gzip"),
            },
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_wrong_hmac_is_401(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"a.log": b"x\n"}
        manifest = _build_manifest(files)
        response = _post(client, manifest, _build_tarball(files), "sha256=" + "0" * 64)
        assert response.status_code == status.HTTP_401_UNAUTHORIZED

    def test_unknown_sender_is_401(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = IntakeConfig.model_validate(
            {
                "intake_port": 9180,
                "intake_root": str(tmp_path),
                "allowed_senders": ["someone-else"],
            }
        )
        client = TestClient(create_app(cfg))
        files = {"a.log": b"x\n"}
        manifest = _build_manifest(files)
        response = _post(client, manifest, _build_tarball(files), _sign(manifest))
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


class TestHappyPath:
    def test_accept_writes_quarantine_manifest_receipt_audit(
        self, tmp_path: Path
    ) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"app.log": b"hello\n", "trace.txt": b"line-a\n"}
        manifest = _build_manifest(files)
        response = _post(client, manifest, _build_tarball(files), _sign(manifest))
        assert response.status_code == status.HTTP_202_ACCEPTED
        body = response.json()
        assert body["status"] == "accept"
        assert body["bundle_id"] == BUNDLE_ID
        assert len(body["per_file_sha256_verified"]) == EXPECTED_FILES_IN_HAPPY_TEST

        target = quarantine.bundle_exists(tmp_path, SENDER, BUNDLE_ID)
        assert target is not None
        assert (target / "app.log").read_bytes() == b"hello\n"
        assert (target / "manifest.json").read_bytes() == manifest

        audit_lines = (tmp_path / "audit.log").read_bytes().splitlines()
        assert len(audit_lines) == 1
        accept = json.loads(audit_lines[0])
        assert accept["event"] == "accept"
        assert accept["sender_id"] == SENDER
        assert accept["prev_hash"] == audit.GENESIS_PREV_HASH


class TestIdempotency:
    def test_duplicate_returns_200_with_original_receipt(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"app.log": b"once\n"}
        manifest = _build_manifest(files)
        tarball = _build_tarball(files)
        first = _post(client, manifest, tarball, _sign(manifest))
        assert first.status_code == status.HTTP_202_ACCEPTED
        second = _post(client, manifest, tarball, _sign(manifest))
        assert second.status_code == status.HTTP_200_OK
        assert second.json()["bundle_id"] == BUNDLE_ID


class TestRejects:
    def test_disallowed_extension_is_400(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"bad.exe": b"MZ"}
        manifest = _build_manifest(files)
        response = _post(client, manifest, _build_tarball(files), _sign(manifest))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "ext_not_allowed" in response.text

    def test_schema_v2_rejected(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"a.log": b"x\n"}
        manifest_dict = {
            "schema_version": 2,
            "sender_id": SENDER,
            "sent_at": "2026-04-19T08:12:00Z",
            "bundle_id": BUNDLE_ID,
            "source_root": "/tmp/src",
            "files": [
                {
                    "relative_path": "a.log",
                    "sha256": hashlib.sha256(b"x\n").hexdigest(),
                    "size_bytes": 2,
                    "mtime": "2026-04-19T08:11:04Z",
                }
            ],
        }
        manifest = rfc8785.dumps(manifest_dict) + b"\n"
        response = _post(client, manifest, _build_tarball(files), _sign(manifest))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "schema_unsupported" in response.text

    def test_header_bundle_id_mismatch_is_401(self, tmp_path: Path) -> None:
        client = TestClient(create_app(_make_config(tmp_path)))
        files = {"a.log": b"x\n"}
        manifest = _build_manifest(files)
        response = _post(
            client,
            manifest,
            _build_tarball(files),
            _sign(manifest),
            bundle_id="different-id",
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
