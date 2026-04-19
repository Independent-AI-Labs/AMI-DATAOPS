"""Build + canonicalise + sign ami-report manifests.

The manifest is the atomic signed unit of a report send: it lists the
files by relative path with their per-file SHA256 and is signed with
HMAC-SHA256 using the per-peer shared secret. Canonicalisation follows
[RFC 8785 JSON Canonicalization Scheme (JCS)](https://www.rfc-editor.org/rfc/rfc8785)
so the receiver's HMAC verify is deterministic.

The receiver-side ManifestModel in `ami.dataops.intake.app` accepts the
same JSON shape; this module is the sender-side producer.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from pathlib import Path

import rfc8785
import uuid_utils
from pydantic import BaseModel, ConfigDict

SCHEMA_VERSION = 1
SIGNATURE_SCHEME_PREFIX = "sha256="
LF_TERMINATOR = b"\n"


class ManifestFileEntry(BaseModel):
    """One files[] entry: relative path + receiver-verifiable hash."""

    model_config = ConfigDict(extra="forbid")

    relative_path: str
    sha256: str
    size_bytes: int
    mtime: str


class SenderManifest(BaseModel):
    """The full manifest object signed with HMAC-SHA256 before POST."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int
    sender_id: str
    sent_at: str
    bundle_id: str
    source_root: str
    files: list[ManifestFileEntry]


def _sha256_of(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _rfc3339_mtime(path: Path) -> str:
    stat = path.stat()
    return datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _rfc3339_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_manifest(
    *,
    sender_id: str,
    source_root: Path,
    files: list[Path],
    bundle_id: str | None = None,
    sent_at: str | None = None,
) -> SenderManifest:
    """Produce a fresh SenderManifest from files on disk.

    Every `files` entry must be under `source_root`; the relative path is
    computed + normalised to forward slashes. If `bundle_id` is omitted a
    UUIDv7 is minted (workspace rule: uuid7 only). Hashes and mtimes are
    read from the filesystem here, not from callers, so the manifest is
    authoritative about what the sender actually saw.
    """
    source_root = source_root.expanduser().absolute()
    entries: list[ManifestFileEntry] = []
    for file_path in files:
        absolute = file_path.expanduser().absolute()
        if not absolute.is_file():
            msg = f"manifest source {absolute} is not a regular file"
            raise FileNotFoundError(msg)
        try:
            rel = absolute.relative_to(source_root).as_posix()
        except ValueError as exc:
            msg = f"{absolute} is not under source_root {source_root}"
            raise ValueError(msg) from exc
        entries.append(
            ManifestFileEntry(
                relative_path=rel,
                sha256=_sha256_of(absolute),
                size_bytes=absolute.stat().st_size,
                mtime=_rfc3339_mtime(absolute),
            )
        )
    return SenderManifest(
        schema_version=SCHEMA_VERSION,
        sender_id=sender_id,
        sent_at=sent_at or _rfc3339_now(),
        bundle_id=bundle_id or str(uuid_utils.uuid7()),
        source_root=str(source_root),
        files=entries,
    )


def canonical_manifest_bytes(manifest: SenderManifest) -> bytes:
    """Serialise `manifest` to JCS bytes + a single LF terminator.

    The LF is part of the signed payload, matching the receiver contract
    in SPEC-REPORT §3.2. Callers sign and POST *exactly* the returned
    bytes with no further whitespace or reformatting.
    """
    payload = manifest.model_dump(mode="json")
    return rfc8785.dumps(payload) + LF_TERMINATOR


def sign_manifest(manifest_bytes: bytes, shared_secret: str) -> str:
    """Return the `X-AMI-Signature` header value for `manifest_bytes`."""
    digest = hmac.new(
        shared_secret.encode("utf-8"), manifest_bytes, hashlib.sha256
    ).hexdigest()
    return f"{SIGNATURE_SCHEME_PREFIX}{digest}"


def verify_signature(
    manifest_bytes: bytes, signature_header: str, shared_secret: str
) -> bool:
    """Return True iff `signature_header` matches `HMAC-SHA256(secret, bytes)`."""
    if not signature_header.startswith(SIGNATURE_SCHEME_PREFIX):
        return False
    provided_hex = signature_header[len(SIGNATURE_SCHEME_PREFIX) :]
    expected = hmac.new(
        shared_secret.encode("utf-8"), manifest_bytes, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(provided_hex, expected)
