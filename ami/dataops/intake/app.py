"""FastAPI application factory for the ami-intake daemon.

Wires the REQ-REPORT pipeline (§7-§13) into a Starlette ASGI app with:

- `POST /v1/bundles` — the sole ingress; auth + HMAC + validation + quarantine + audit.
- `GET /metrics` — Prometheus metrics scraped by operator tooling.
- `GET /healthz` — trivial liveness used by systemd/ansible readiness checks.

The bundle handler never uses FastAPI's default `UploadFile` spool on the
request body; it wraps `Request.stream()` in `CappedAsyncStream` so the
413 fires before any bytes land anywhere.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from fastapi import FastAPI, HTTPException, Request, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, ValidationError
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartParser

from ami.dataops.intake import audit, quarantine, validation
from ami.dataops.intake.config import IntakeConfig
from ami.dataops.intake.stream import CappedAsyncStream

SUPPORTED_SCHEMA_VERSION = 1
RECEIPT_STATUS_REJECT = "reject"
BEARER_PREFIX = "Bearer "
BEARER_PREFIX_LEN = len(BEARER_PREFIX)
HEADER_SENDER_ID = "X-AMI-Sender-Id"
HEADER_BUNDLE_ID = "X-AMI-Bundle-Id"
HEADER_SIGNATURE = "X-AMI-Signature"
SIGNATURE_SCHEME_PREFIX = "sha256="
HASH_CHUNK_BYTES = 65536


class ManifestFileEntry(BaseModel):
    relative_path: str
    sha256: str
    size_bytes: int
    mtime: str


class ManifestModel(BaseModel):
    schema_version: int
    sender_id: str
    sent_at: str
    bundle_id: str
    source_root: str
    files: list[ManifestFileEntry]


class RequestContext(BaseModel):
    """Inputs threaded through the bundle-handling helpers."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    config: IntakeConfig
    sender_id: str
    bundle_id: str
    remote_addr: str
    manifest_bytes: bytes
    bundle_path: Path


def create_app(config: IntakeConfig) -> FastAPI:
    """Build a FastAPI application bound to `config`."""
    app = FastAPI(title="ami-intake", version="0.1.0")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/bundles")
    async def receive_bundle(request: Request) -> Response:
        return await _handle_bundle(request, config)

    return app


async def _handle_bundle(request: Request, config: IntakeConfig) -> Response:
    sender_id = _require_header(request, HEADER_SENDER_ID)
    bundle_id = _require_header(request, HEADER_BUNDLE_ID)
    signature_header = _require_header(request, HEADER_SIGNATURE)
    token = _require_bearer_token(request)
    _authenticate_sender(config, sender_id, token)
    existing = quarantine.bundle_exists(config.intake_root, sender_id, bundle_id)
    if existing is not None:
        return _ok_idempotent(existing)
    capped = CappedAsyncStream(request.stream(), config.max_bundle_bytes)
    manifest_bytes, bundle_path = await _parse_multipart(request, capped)
    ctx = RequestContext(
        config=config,
        sender_id=sender_id,
        bundle_id=bundle_id,
        remote_addr=_remote_addr(request),
        manifest_bytes=manifest_bytes,
        bundle_path=bundle_path,
    )
    try:
        _verify_signature(ctx, signature_header)
        manifest = _parse_manifest(manifest_bytes)
        _assert_headers_match(manifest, sender_id, bundle_id)
        receipt = _process_bundle(ctx, manifest)
    finally:
        if bundle_path.exists():
            bundle_path.unlink(missing_ok=True)
    return Response(
        content=receipt.model_dump_json(),
        media_type="application/json",
        status_code=status.HTTP_202_ACCEPTED,
    )


def _require_header(request: Request, name: str) -> str:
    value = request.headers.get(name)
    if not value:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")
    return value


def _require_bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith(BEARER_PREFIX):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")
    return auth[BEARER_PREFIX_LEN:]


def _authenticate_sender(config: IntakeConfig, sender_id: str, token: str) -> None:
    if sender_id not in config.allowed_senders:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")
    expected = os.environ.get(f"AMI_INTAKE_TOKENS__{sender_id.upper()}")
    if not expected or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")


def _verify_signature(ctx: RequestContext, signature_header: str) -> None:
    if not signature_header.startswith(SIGNATURE_SCHEME_PREFIX):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")
    provided_hex = signature_header[len(SIGNATURE_SCHEME_PREFIX) :]
    secret = os.environ.get(f"AMI_INTAKE_SECRETS__{ctx.sender_id.upper()}")
    if not secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")
    expected = hmac.new(
        secret.encode("utf-8"), ctx.manifest_bytes, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(provided_hex, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")


def _parse_manifest(manifest_bytes: bytes) -> ManifestModel:
    try:
        data = json.loads(manifest_bytes.decode("utf-8"))
        manifest = ManifestModel.model_validate(data)
    except (json.JSONDecodeError, ValidationError, UnicodeDecodeError) as exc:
        raise _reject("path_unsafe", f"manifest parse: {exc}") from exc
    if manifest.schema_version != SUPPORTED_SCHEMA_VERSION:
        raise _reject("schema_unsupported", f"version {manifest.schema_version}")
    return manifest


def _assert_headers_match(
    manifest: ManifestModel, sender_id: str, bundle_id: str
) -> None:
    if manifest.sender_id != sender_id or manifest.bundle_id != bundle_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="auth")


async def _parse_multipart(
    request: Request, capped: CappedAsyncStream
) -> tuple[bytes, Path]:
    parser = MultiPartParser(request.headers, cast(AsyncGenerator[bytes, None], capped))
    form = await parser.parse()
    manifest_field = form.get("manifest")
    bundle_field = form.get("bundle")
    if manifest_field is None or bundle_field is None:
        raise _reject("path_unsafe", "multipart missing manifest or bundle part")
    manifest_bytes = await _read_form_field_bytes(manifest_field)
    bundle_path = await _spool_form_field_to_disk(bundle_field)
    return manifest_bytes, bundle_path


async def _read_form_field_bytes(field: object) -> bytes:
    if isinstance(field, UploadFile):
        data = await field.read()
        return data if isinstance(data, bytes) else str(data).encode("utf-8")
    if isinstance(field, (bytes, bytearray)):
        return bytes(field)
    return str(field).encode("utf-8")


async def _spool_form_field_to_disk(field: object) -> Path:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bundle") as tmp:
        path = Path(tmp.name)
    payload = await _read_form_field_bytes(field)
    path.write_bytes(payload)
    return path


def _process_bundle(
    ctx: RequestContext, manifest: ManifestModel
) -> quarantine.ReceiptModel:
    staging = Path(tempfile.mkdtemp(prefix="ami-intake-"))
    try:
        extracted = _extract_and_verify(ctx.bundle_path, staging, manifest, ctx.config)
        receipt = _build_receipt(ctx.bundle_id, extracted)
        quarantine.finalize_bundle(
            staging,
            ctx.config.intake_root,
            quarantine.FinalizeRequest(
                sender_id=ctx.sender_id,
                bundle_id=ctx.bundle_id,
                manifest_bytes=ctx.manifest_bytes,
                receipt=receipt,
            ),
        )
    except validation.ValidationRejected as exc:
        _audit_reject(ctx, exc)
        raise _reject(exc.reason_code, exc.detail) from exc
    if staging.exists():
        _cleanup_staging(staging)
    _audit_accept(ctx, receipt)
    return receipt


def _extract_and_verify(
    bundle_path: Path,
    staging: Path,
    manifest: ManifestModel,
    config: IntakeConfig,
) -> list[quarantine.ReceiptFileEntry]:
    validation.validate_file_count(len(manifest.files), config.max_files_per_bundle)
    with bundle_path.open("rb") as handle:
        files = validation.extract_bundle_stream(
            handle,
            staging,
            max_file_bytes=config.max_file_bytes,
            max_bundle_bytes=config.max_bundle_bytes,
            max_files=config.max_files_per_bundle,
        )
    manifest_by_path = {entry.relative_path: entry for entry in manifest.files}
    receipts: list[quarantine.ReceiptFileEntry] = []
    for path in files:
        rel = path.relative_to(staging).as_posix()
        entry = manifest_by_path.get(rel)
        if entry is None:
            raise validation.ValidationRejected(
                "hash_mismatch",
                f"tar contained {rel!r} which is not in the manifest",
            )
        validation.verify_hash(path, entry.sha256)
        receipts.append(
            quarantine.ReceiptFileEntry(
                relative_path=rel,
                sha256=entry.sha256,
                size_bytes=path.stat().st_size,
            )
        )
    return receipts


def _build_receipt(
    bundle_id: str, extracted: list[quarantine.ReceiptFileEntry]
) -> quarantine.ReceiptModel:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return quarantine.ReceiptModel(
        bundle_id=bundle_id,
        received_at=now,
        per_file_sha256_verified=extracted,
        audit_log_offset=0,
    )


def _audit_accept(ctx: RequestContext, receipt: quarantine.ReceiptModel) -> None:
    receipt_sha = _receipt_sha(ctx.manifest_bytes, ctx.bundle_path)
    _, offset = audit.append_audit_record(
        ctx.config.intake_root,
        audit.AuditAppendParams(
            event="accept",
            sender_id=ctx.sender_id,
            bundle_id=ctx.bundle_id,
            remote_addr=ctx.remote_addr,
            byte_count=ctx.bundle_path.stat().st_size,
            file_count=len(receipt.per_file_sha256_verified),
            reject_reason=None,
            receipt_sha256=receipt_sha,
        ),
    )
    receipt.audit_log_offset = offset


def _audit_reject(ctx: RequestContext, exc: validation.ValidationRejected) -> None:
    receipt_sha = _receipt_sha(ctx.manifest_bytes, ctx.bundle_path)
    byte_count = ctx.bundle_path.stat().st_size if ctx.bundle_path.exists() else 0
    audit.append_audit_record(
        ctx.config.intake_root,
        audit.AuditAppendParams(
            event="reject",
            sender_id=ctx.sender_id,
            bundle_id=ctx.bundle_id,
            remote_addr=ctx.remote_addr,
            byte_count=byte_count,
            file_count=0,
            reject_reason=exc.reason_code,
            receipt_sha256=receipt_sha,
        ),
    )


def _receipt_sha(manifest_bytes: bytes, bundle_path: Path) -> str:
    hasher = hashlib.sha256()
    hasher.update(manifest_bytes)
    hasher.update(b"\x00")
    if bundle_path.exists():
        with bundle_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(HASH_CHUNK_BYTES), b""):
                hasher.update(chunk)
    return hasher.hexdigest()


def _remote_addr(request: Request) -> str:
    client = request.client
    return client.host if client else "unknown"


def _reject(reason_code: str, detail: str) -> HTTPException:
    body = json.dumps(
        {
            "status": RECEIPT_STATUS_REJECT,
            "reason_code": reason_code,
            "detail": detail,
        }
    )
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=body,
    )


def _ok_idempotent(existing: Path) -> Response:
    receipt = quarantine.read_receipt(existing)
    return Response(
        content=receipt.model_dump_json(),
        media_type="application/json",
        status_code=status.HTTP_200_OK,
    )


def _cleanup_staging(staging: Path) -> None:
    for sub in staging.rglob("*"):
        if sub.is_file():
            sub.unlink(missing_ok=True)
    for sub in sorted(staging.rglob("*"), reverse=True):
        if sub.is_dir():
            sub.rmdir()
    if staging.exists():
        staging.rmdir()
