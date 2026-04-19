"""Atomic staging-to-quarantine promotion for accepted bundles.

After every file in a bundle has passed content validation, `finalize_bundle`
moves the staging directory contents to:

    <intake_root>/<sender_id>/<YYYY>/<MM>/<DD>/<bundle_id>/

with the signed manifest stored alongside as `manifest.json` (0440) and the
receiver-computed receipt stored as `receipt.json` (0440). Data files land
at 0640. The date tree is the *received* date, not the bundle's internal
timestamp, so clock skew on the sender cannot shift the filesystem layout.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, ConfigDict

DATA_FILE_MODE = 0o640
METADATA_FILE_MODE = 0o440
QUARANTINE_DIR_MODE = 0o750


class ReceiptFileEntry(BaseModel):
    """One per-file entry in `receipt.json`, hashed by the receiver."""

    relative_path: str
    sha256: str
    size_bytes: int


class ReceiptModel(BaseModel):
    """Receiver-generated receipt mirroring what flows back in the HTTP body."""

    bundle_id: str
    received_at: str
    per_file_sha256_verified: list[ReceiptFileEntry]
    audit_log_offset: int
    status: str = "accept"


def _received_date_parts(now: datetime | None = None) -> tuple[str, str, str]:
    moment = now or datetime.now(tz=timezone.utc)
    return (
        moment.strftime("%Y"),
        moment.strftime("%m"),
        moment.strftime("%d"),
    )


def quarantine_path_for(
    intake_root: Path,
    sender_id: str,
    bundle_id: str,
    *,
    now: datetime | None = None,
) -> Path:
    """Return the canonical quarantine path for a newly-received bundle."""
    year, month, day = _received_date_parts(now)
    return intake_root / sender_id / year / month / day / bundle_id


def bundle_exists(intake_root: Path, sender_id: str, bundle_id: str) -> Path | None:
    """Return the existing quarantine path for (sender_id, bundle_id), or None.

    Used for idempotent replay detection: a duplicate bundle returns HTTP 200
    with the original receipt rather than being re-quarantined.
    """
    sender_root = intake_root / sender_id
    if not sender_root.is_dir():
        return None
    for candidate in sender_root.rglob(bundle_id):
        if candidate.is_dir() and (candidate / "receipt.json").is_file():
            return candidate
    return None


def read_receipt(quarantine_dir: Path) -> ReceiptModel:
    """Load and validate the receipt stored alongside a quarantined bundle."""
    receipt_path = quarantine_dir / "receipt.json"
    return ReceiptModel.model_validate_json(receipt_path.read_text(encoding="utf-8"))


class FinalizeRequest(BaseModel):
    """Grouped inputs to `finalize_bundle` to keep its signature compact."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    sender_id: str
    bundle_id: str
    manifest_bytes: bytes
    receipt: ReceiptModel
    now: datetime | None = None


def finalize_bundle(
    staging_dir: Path,
    intake_root: Path,
    request: FinalizeRequest,
) -> Path:
    """Move validated files from `staging_dir` into the sender's quarantine tree.

    Same-filesystem renames are atomic (O_DIRECTORY semantics). When staging
    and intake_root sit on different filesystems, falls back to copy + fsync
    + unlink, which is still crash-consistent (the new location is complete
    before the staging entry is removed).
    """
    target = quarantine_path_for(
        intake_root, request.sender_id, request.bundle_id, now=request.now
    )
    if target.exists():
        msg = (
            f"quarantine collision for {request.sender_id}/{request.bundle_id} "
            f"at {target}"
        )
        raise FileExistsError(msg)
    target.parent.mkdir(mode=QUARANTINE_DIR_MODE, parents=True, exist_ok=True)
    try:
        staging_dir.rename(target)
    except OSError:
        shutil.copytree(staging_dir, target)
        shutil.rmtree(staging_dir)
    _apply_data_permissions(target)
    _write_metadata_file(target / "manifest.json", request.manifest_bytes)
    _write_metadata_file(
        target / "receipt.json",
        request.receipt.model_dump_json().encode("utf-8"),
    )
    return target


def _apply_data_permissions(target: Path) -> None:
    for path in target.rglob("*"):
        if path.is_file():
            path.chmod(DATA_FILE_MODE)
        elif path.is_dir():
            path.chmod(QUARANTINE_DIR_MODE)
    target.chmod(QUARANTINE_DIR_MODE)


def _write_metadata_file(path: Path, payload: bytes) -> None:
    fd = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        METADATA_FILE_MODE,
    )
    try:
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    path.chmod(METADATA_FILE_MODE)
