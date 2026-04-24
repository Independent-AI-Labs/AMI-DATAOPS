"""Append-only audit log with tamper-evident hash chain for ami-intake.

Implements REQ-REPORT §11 + SPEC-REPORT §7: one NDJSON record per receive
attempt; `prev_hash` = SHA256 of the previous record's bytes (including its
trailing LF); rotation writes a terminal `seal` record whose `seal_hash`
covers the full sealed file; the next fresh log's first record uses that
`seal_hash` as its `prev_hash`, so the chain spans rotations.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

GENESIS_PREV_HASH = "0" * 64
AUDIT_LOG_NAME = "audit.log"
AUDIT_ARCHIVE_DIR = "audit"
SEAL_TIMESTAMP_FORMAT = "%Y-%m-%dT%H%M%S%fZ"
ACTIVE_LOG_MODE = 0o640
SEALED_LOG_MODE = 0o440
ARCHIVE_DIR_MODE = 0o750

AuditEvent = Literal["accept", "reject", "auth_reject"]
AuditReasonCode = Literal[
    # Validation rejects (existing, emitted from the bundle pipeline).
    "ext_not_allowed",
    "path_unsafe",
    "not_text",
    "file_too_large",
    "bundle_too_large",
    "too_many_files",
    "hash_mismatch",
    "schema_unsupported",
    # Auth rejects (new, emitted before the bundle is parsed).
    "missing_bearer",
    "unknown_sender",
    "bad_bearer",
    "bad_signature",
    "header_manifest_mismatch",
]


class AuditRecord(BaseModel):
    """One receive-attempt record written to `audit.log`."""

    ts: str
    event: AuditEvent
    sender_id: str
    bundle_id: str
    remote_addr: str
    byte_count: int
    file_count: int
    reject_reason: AuditReasonCode | None
    receipt_sha256: str
    prev_hash: str


class SealRecord(BaseModel):
    """Terminal rotation record written at the tail of a sealed `audit.log`."""

    ts: str
    event: Literal["seal"] = "seal"
    prev_hash: str
    seal_hash: str


class AuditAppendParams(BaseModel):
    """Inputs to `append_audit_record`, grouped so the API stays argument-free."""

    event: AuditEvent
    sender_id: str
    bundle_id: str
    remote_addr: str
    byte_count: int
    file_count: int
    reject_reason: AuditReasonCode | None
    receipt_sha256: str


_AUDIT_LOCK = threading.Lock()


def _now_rfc3339() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _serialize_record(record: AuditRecord | SealRecord) -> bytes:
    """Return canonical NDJSON bytes for `record` (sorted keys, UTF-8, trailing LF)."""
    return (record.model_dump_json(by_alias=False) + "\n").encode("utf-8")


def _read_last_line_bytes(path: Path) -> bytes | None:
    """Return the bytes of the final line (including LF) of `path`, or None if empty."""
    if not path.exists():
        return None
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        if size == 0:
            return None
        read_chunk = min(size, 4096)
        handle.seek(size - read_chunk)
        tail = handle.read(read_chunk)
    if not tail.endswith(b"\n"):
        return tail
    idx = tail.rfind(b"\n", 0, len(tail) - 1)
    return tail[idx + 1 :] if idx >= 0 else tail


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _latest_sealed_file(intake_root: Path) -> Path | None:
    archive_dir = intake_root / AUDIT_ARCHIVE_DIR
    if not archive_dir.exists():
        return None
    candidates = sorted(archive_dir.glob("*.log"))
    return candidates[-1] if candidates else None


def _extract_seal_hash_from_sealed(path: Path) -> str:
    last = _read_last_line_bytes(path)
    if last is None:
        msg = f"sealed audit file {path} is empty; chain broken"
        raise RuntimeError(msg)
    record = SealRecord.model_validate_json(last.rstrip(b"\n"))
    return record.seal_hash


def compute_expected_prev_hash(intake_root: Path) -> str:
    """Return what the next appended record's `prev_hash` must be."""
    active = intake_root / AUDIT_LOG_NAME
    last_active = _read_last_line_bytes(active)
    if last_active is not None:
        return _sha256_hex(last_active)
    sealed = _latest_sealed_file(intake_root)
    if sealed is None:
        return GENESIS_PREV_HASH
    return _extract_seal_hash_from_sealed(sealed)


def append_audit_record(
    intake_root: Path,
    params: AuditAppendParams,
    *,
    max_active_bytes: int | None = None,
) -> tuple[AuditRecord, int]:
    """Append a new audit record and return (record, byte_offset).

    Serialises all appends through a process-wide lock so the prev_hash read
    and record write happen atomically. Fsyncs the file before returning so
    a power-loss after HTTP response still leaves the audit entry on disk.

    If `max_active_bytes` is set and the post-append file size crosses it,
    `rotate_audit` is invoked automatically — the chain stays intact because
    the seal record is written under the same lock.
    """
    intake_root.mkdir(parents=True, exist_ok=True)
    active = intake_root / AUDIT_LOG_NAME
    with _AUDIT_LOCK:
        prev_hash = compute_expected_prev_hash(intake_root)
        record = AuditRecord(
            ts=_now_rfc3339(),
            event=params.event,
            sender_id=params.sender_id,
            bundle_id=params.bundle_id,
            remote_addr=params.remote_addr,
            byte_count=params.byte_count,
            file_count=params.file_count,
            reject_reason=params.reject_reason,
            receipt_sha256=params.receipt_sha256,
            prev_hash=prev_hash,
        )
        payload = _serialize_record(record)
        needs_chmod = not active.exists()
        offset = active.stat().st_size if active.exists() else 0
        fd = os.open(active, os.O_WRONLY | os.O_APPEND | os.O_CREAT, ACTIVE_LOG_MODE)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        if needs_chmod:
            active.chmod(ACTIVE_LOG_MODE)
        post_size = active.stat().st_size
    if max_active_bytes is not None and post_size >= max_active_bytes:
        rotate_audit(intake_root)
    return record, offset


def rotate_audit(intake_root: Path) -> Path:
    """Seal the active `audit.log` and move it under `audit/<timestamp>.log`.

    Returns the path of the sealed file. If no active log exists, is a no-op
    returning the path that would have been written.
    """
    active = intake_root / AUDIT_LOG_NAME
    archive_dir = intake_root / AUDIT_ARCHIVE_DIR
    archive_dir.mkdir(mode=ARCHIVE_DIR_MODE, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime(SEAL_TIMESTAMP_FORMAT)
    sealed_path = archive_dir / f"{timestamp}.log"
    with _AUDIT_LOCK:
        if not active.exists() or active.stat().st_size == 0:
            return sealed_path
        seal_hash = _sha256_hex(active.read_bytes())
        prev_hash = _sha256_hex(_read_last_line_bytes(active) or b"")
        seal_record = SealRecord(
            ts=_now_rfc3339(),
            prev_hash=prev_hash,
            seal_hash=seal_hash,
        )
        payload = _serialize_record(seal_record)
        fd = os.open(active, os.O_WRONLY | os.O_APPEND)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        active.chmod(SEALED_LOG_MODE)
        active.rename(sealed_path)
    return sealed_path


def verify_chain(intake_root: Path) -> None:
    """Walk every record in every log (sealed + active) and raise on a broken link.

    The first record must carry `prev_hash == GENESIS_PREV_HASH`. Each subsequent
    record's `prev_hash` must match the SHA256 of the previous record's bytes.
    Across rotations, the first record of a new file must match the previous
    sealed file's `seal_hash`.
    """
    expected_prev = GENESIS_PREV_HASH
    sealed_files = (
        sorted((intake_root / AUDIT_ARCHIVE_DIR).glob("*.log"))
        if (intake_root / AUDIT_ARCHIVE_DIR).exists()
        else []
    )
    active_path = intake_root / AUDIT_LOG_NAME
    paths = [*sealed_files, active_path]
    for path in paths:
        if not path.exists():
            continue
        expected_prev = _verify_single_file(path, expected_prev)


def _verify_single_file(path: Path, expected_prev: str) -> str:
    with path.open("rb") as handle:
        body = handle.read()
    if not body:
        return expected_prev
    lines = [line + b"\n" for line in body.splitlines() if line]
    for raw in lines:
        record_dict = _parse_record_line(raw)
        actual_prev = record_dict.get("prev_hash", "missing")
        if actual_prev != expected_prev:
            msg = (
                f"chain broken in {path.name}: expected "
                f"prev_hash={expected_prev[:16]}..., got {actual_prev[:16]}..."
            )
            raise RuntimeError(msg)
        expected_prev = _sha256_hex(raw)
    if record_dict.get("event") == "seal":
        expected_prev = record_dict["seal_hash"]
    return expected_prev


def _parse_record_line(raw: bytes) -> dict[str, str]:
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        msg = f"audit line is not an object: {raw!r}"
        raise TypeError(msg)
    return {str(k): str(v) if v is not None else "" for k, v in parsed.items()}
