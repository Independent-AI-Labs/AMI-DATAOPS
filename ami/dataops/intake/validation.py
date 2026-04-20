"""Pure validation rules for ami-intake bundle acceptance.

Implements the content policy from REQ-REPORT §9: extension allowlist, path
safety via the PEP 706 tarfile data filter, text-only probe, size caps, and
SHA256 verification. No FastAPI / uvicorn dependency — any caller with a
staging directory and a manifest can invoke these rules.
"""

from __future__ import annotations

import hashlib
import tarfile
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, Literal

ReasonCode = Literal[
    "ext_not_allowed",
    "path_unsafe",
    "not_text",
    "file_too_large",
    "bundle_too_large",
    "too_many_files",
    "hash_mismatch",
    "schema_unsupported",
]

ALLOWED_EXTENSIONS: frozenset[str] = frozenset({".log", ".txt"})

TEXT_PROBE_BYTES = 8192
NULL_BYTE = b"\x00"
HASH_CHUNK_BYTES = 65536
DEFAULT_MAX_FILE_BYTES = 1 * 1024 * 1024
DEFAULT_MAX_BUNDLE_BYTES = 500 * 1024 * 1024
DEFAULT_MAX_FILES_PER_BUNDLE = 1000


class ValidationRejected(Exception):
    """Raised when a bundle or file violates the content policy.

    Carries a stable `reason_code` (for metrics + API response) and a
    human-readable `detail` for logs.
    """

    def __init__(self, reason_code: ReasonCode, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code: ReasonCode = reason_code
        self.detail = detail


def validate_extension(relative_path: str) -> None:
    """Reject when the final extension is not on the allowlist."""
    ext = Path(relative_path).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise ValidationRejected(
            "ext_not_allowed",
            f"extension {ext!r} for {relative_path!r} not in allowlist",
        )


def probe_text_content(path: Path, probe_bytes: int = TEXT_PROBE_BYTES) -> None:
    """Reject when the first `probe_bytes` of `path` contain a NUL byte."""
    with path.open("rb") as handle:
        probe = handle.read(probe_bytes)
    if NULL_BYTE in probe:
        raise ValidationRejected(
            "not_text",
            f"NUL byte within first {probe_bytes} bytes of {path.name}",
        )


def validate_file_size(size_bytes: int, max_bytes: int) -> None:
    """Reject when an individual file exceeds the per-file cap."""
    if size_bytes > max_bytes:
        raise ValidationRejected(
            "file_too_large",
            f"{size_bytes} bytes exceeds per-file cap {max_bytes}",
        )


def validate_bundle_aggregate(total_bytes: int, max_bytes: int) -> None:
    """Reject when the running aggregate of extracted bytes exceeds the cap."""
    if total_bytes > max_bytes:
        raise ValidationRejected(
            "bundle_too_large",
            f"bundle aggregate {total_bytes} bytes exceeds cap {max_bytes}",
        )


def validate_file_count(count: int, max_count: int) -> None:
    """Reject when file count exceeds the cap."""
    if count > max_count:
        raise ValidationRejected(
            "too_many_files",
            f"{count} files exceeds cap {max_count}",
        )


def compute_sha256(path: Path, chunk_bytes: int = HASH_CHUNK_BYTES) -> str:
    """Return the lowercase hex SHA256 of `path`, read in chunks."""
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_bytes):
            hasher.update(chunk)
    return hasher.hexdigest()


def verify_hash(path: Path, expected_hex_sha256: str) -> None:
    """Reject when the on-disk SHA256 of `path` does not match the manifest hash."""
    actual = compute_sha256(path)
    if actual != expected_hex_sha256:
        expected_head = expected_hex_sha256[:16]
        actual_head = actual[:16]
        raise ValidationRejected(
            "hash_mismatch",
            f"{path.name}: expected {expected_head}..., got {actual_head}...",
        )


def apply_data_filter(
    member: tarfile.TarInfo,
    dest: str,
) -> tarfile.TarInfo:
    """Invoke the PEP 706 `tarfile.data_filter`, translating its errors.

    Rejects symlinks, hardlinks, device nodes, FIFOs, setuid/setgid bits,
    absolute paths, and paths that would escape `dest` via `..`.
    """
    try:
        return tarfile.data_filter(member, dest)
    except tarfile.FilterError as exc:
        raise ValidationRejected("path_unsafe", str(exc)) from exc


def iter_validated_members(
    tar: tarfile.TarFile,
    staging_root: Path,
    *,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_bundle_bytes: int = DEFAULT_MAX_BUNDLE_BYTES,
    max_files: int = DEFAULT_MAX_FILES_PER_BUNDLE,
) -> Iterator[tarfile.TarInfo]:
    """Yield each tar member after running path-safety + size pre-checks.

    The caller is responsible for extracting the yielded member; this iterator
    exists so quota enforcement happens *before* any bytes hit disk. A
    zip-bomb is caught on the first oversized member or the first aggregate
    overflow, terminating extraction early.
    """
    running_total = 0
    file_count = 0
    staging_str = str(staging_root)
    for raw_member in tar:
        if raw_member.isdir():
            continue
        safe_member = apply_data_filter(raw_member, staging_str)
        if not safe_member.isfile():
            name = safe_member.name
            raise ValidationRejected(
                "path_unsafe",
                f"tar member {name!r} is not a regular file after filtering",
            )
        file_count += 1
        validate_file_count(file_count, max_files)
        validate_extension(safe_member.name)
        validate_file_size(safe_member.size, max_file_bytes)
        running_total += safe_member.size
        validate_bundle_aggregate(running_total, max_bundle_bytes)
        yield safe_member


def extract_bundle_stream(
    gz_stream: BinaryIO,
    staging_root: Path,
    *,
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_bundle_bytes: int = DEFAULT_MAX_BUNDLE_BYTES,
    max_files: int = DEFAULT_MAX_FILES_PER_BUNDLE,
) -> list[Path]:
    """Stream-extract a gzip-tar from `gz_stream` into `staging_root`.

    Runs every path / size / count rule via `iter_validated_members`, then
    after extraction probes each file for NUL bytes. Returns the list of
    absolute paths of every extracted file in tar order.
    """
    staging_root.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with tarfile.open(fileobj=gz_stream, mode="r|gz") as tar:
        for safe_member in iter_validated_members(
            tar,
            staging_root,
            max_file_bytes=max_file_bytes,
            max_bundle_bytes=max_bundle_bytes,
            max_files=max_files,
        ):
            tar.extract(safe_member, path=staging_root, filter="data")
            extracted.append(staging_root / safe_member.name)
    for path in extracted:
        probe_text_content(path)
    return extracted
