"""Integration: `ami-report send --ci` against a live ami-intake daemon.

Spawns a real uvicorn process (see conftest), drives the report CLI with
a non-interactive defaults file, and verifies every REQ-REPORT receipt
contract end-to-end: quarantine layout, manifest round-trip, audit
record shape, hash-chain continuity, rotation + verify.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import tarfile
from pathlib import Path

import httpx
import rfc8785

from ami.dataops.intake import audit, quarantine
from ami.dataops.report import cli

from .conftest import BEARER_TOKEN, SENDER_ID, SHARED_SECRET, LoopbackEnv

EXPECTED_FILES = 3


def _write_source(source_root: Path) -> dict[str, bytes]:
    (source_root / "nested").mkdir()
    payloads = {
        "app.log": b"alpha\n",
        "trace.txt": b"line-a\n",
        "nested/debug.log": b"line-1\nline-2\n",
    }
    for rel, body in payloads.items():
        (source_root / rel).write_bytes(body)
    return payloads


def _write_defaults(defaults_path: Path, files: list[str]) -> None:
    defaults_path.write_text(
        f"peer: {SENDER_ID}\nfiles:\n" + "\n".join(f"  - {f}" for f in files) + "\n"
    )


class TestReportIntakeLoopback:
    def test_happy_path_quarantine_and_audit(self, loopback: LoopbackEnv) -> None:
        payloads = _write_source(loopback.source_root)
        _write_defaults(loopback.defaults_path, list(payloads))

        rc = cli.main(
            [
                "send",
                "--config",
                str(loopback.sender_config_path),
                "--ci",
                "--defaults",
                str(loopback.defaults_path),
            ]
        )
        assert rc == cli.EXIT_OK

        sender_dir = loopback.intake_root / SENDER_ID
        bundle_dirs = list(sender_dir.rglob("receipt.json"))
        assert len(bundle_dirs) == 1
        quarantine_dir = bundle_dirs[0].parent

        for rel, expected in payloads.items():
            assert (quarantine_dir / rel).read_bytes() == expected
            hex_digest = hashlib.sha256(expected).hexdigest()
            entry = next(
                e
                for e in quarantine.read_receipt(
                    quarantine_dir
                ).per_file_sha256_verified
                if e.relative_path == rel
            )
            assert entry.sha256 == hex_digest

        manifest_payload = (quarantine_dir / "manifest.json").read_text()
        loaded = json.loads(manifest_payload)
        assert loaded["sender_id"] == SENDER_ID
        assert len(loaded["files"]) == EXPECTED_FILES

        audit_lines = (loopback.intake_root / "audit.log").read_bytes().splitlines()
        assert len(audit_lines) == 1
        record = json.loads(audit_lines[0])
        assert record["event"] == "accept"
        assert record["sender_id"] == SENDER_ID
        assert record["prev_hash"] == audit.GENESIS_PREV_HASH

        audit.verify_chain(loopback.intake_root)

    def test_idempotent_replay_returns_200(self, loopback: LoopbackEnv) -> None:
        payloads = _write_source(loopback.source_root)
        _write_defaults(loopback.defaults_path, list(payloads))

        args = [
            "send",
            "--config",
            str(loopback.sender_config_path),
            "--ci",
            "--defaults",
            str(loopback.defaults_path),
        ]
        assert cli.main(args) == cli.EXIT_OK
        sender_dir = loopback.intake_root / SENDER_ID
        first_bundle_dir = next(sender_dir.rglob("receipt.json")).parent
        bundle_id = first_bundle_dir.name

        manifest_bytes = (
            rfc8785.dumps(
                {
                    "bundle_id": bundle_id,
                    "files": [
                        {
                            "relative_path": rel,
                            "sha256": hashlib.sha256(body).hexdigest(),
                            "size_bytes": len(body),
                            "mtime": "2026-04-19T08:11:04Z",
                        }
                        for rel, body in payloads.items()
                    ],
                    "schema_version": 1,
                    "sender_id": SENDER_ID,
                    "sent_at": "2026-04-19T08:12:00Z",
                    "source_root": str(loopback.source_root),
                }
            )
            + b"\n"
        )

        signature = (
            "sha256="
            + hmac.new(
                SHARED_SECRET.encode("utf-8"), manifest_bytes, hashlib.sha256
            ).hexdigest()
        )

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for rel, body in payloads.items():
                info = tarfile.TarInfo(name=rel)
                info.size = len(body)
                tar.addfile(info, io.BytesIO(body))

        response = httpx.post(
            f"{loopback.base_url}/v1/bundles",
            headers={
                "Authorization": f"Bearer {BEARER_TOKEN}",
                "X-AMI-Sender-Id": SENDER_ID,
                "X-AMI-Bundle-Id": bundle_id,
                "X-AMI-Signature": signature,
            },
            files={
                "manifest": ("m.json", manifest_bytes, "application/json"),
                "bundle": ("b.tar.gz", buf.getvalue(), "application/gzip"),
            },
        )
        assert response.status_code == httpx.codes.OK
        assert response.json()["bundle_id"] == bundle_id

    def test_rotate_audit_after_accept(self, loopback: LoopbackEnv) -> None:
        payloads = _write_source(loopback.source_root)
        _write_defaults(loopback.defaults_path, list(payloads))
        cli.main(
            [
                "send",
                "--config",
                str(loopback.sender_config_path),
                "--ci",
                "--defaults",
                str(loopback.defaults_path),
            ]
        )
        sealed = audit.rotate_audit(loopback.intake_root)
        assert sealed.is_file()
        assert not (loopback.intake_root / "audit.log").exists()
        audit.verify_chain(loopback.intake_root)
