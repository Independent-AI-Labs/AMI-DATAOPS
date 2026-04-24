"""HTTP-layer helpers for the ami-intake FastAPI app.

Split out of `app.py` to keep that module under the 512-line cap. These
functions do not depend on `RequestContext` — they read primitive inputs
from the raw `Request` and return either a reason-coded `HTTPException`
or a typed primitive. Helpers that need `RequestContext` stay in app.py.
"""

from __future__ import annotations

import json

from fastapi import HTTPException, Request, status

from ami.dataops.intake import audit
from ami.dataops.intake.config import IntakeConfig

HEADER_CF_CONNECTING_IP = "CF-Connecting-IP"
HEADER_X_FORWARDED_FOR = "X-Forwarded-For"
MISSING_SENTINEL = "<missing>"
RECEIPT_STATUS_REJECT = "reject"

AUTH_REASON_CODES: frozenset[str] = frozenset(
    {
        "missing_bearer",
        "unknown_sender",
        "bad_bearer",
        "bad_signature",
        "header_manifest_mismatch",
    }
)


def reject(reason_code: str, detail: str) -> HTTPException:
    """Build an HTTPException whose status is 401 for auth rejects, 400 else."""
    body = json.dumps(
        {
            "status": RECEIPT_STATUS_REJECT,
            "reason_code": reason_code,
            "detail": detail,
        }
    )
    status_code = (
        status.HTTP_401_UNAUTHORIZED
        if reason_code in AUTH_REASON_CODES
        else status.HTTP_400_BAD_REQUEST
    )
    return HTTPException(status_code=status_code, detail=body)


def auth_reject(
    config: IntakeConfig,
    request: Request,
    reason_code: audit.AuditReasonCode,
) -> HTTPException:
    """Append an `auth_reject` audit entry and return the 401 HTTPException."""
    state = request.state
    sender_id = getattr(state, "sender_id", None) or MISSING_SENTINEL
    bundle_id = getattr(state, "bundle_id", None) or MISSING_SENTINEL
    audit.append_audit_record(
        config.intake_root,
        audit.AuditAppendParams(
            event="auth_reject",
            sender_id=sender_id,
            bundle_id=bundle_id,
            remote_addr=client_ip(request, config.trust_proxy_headers),
            byte_count=0,
            file_count=0,
            reject_reason=reason_code,
            receipt_sha256="",
        ),
        max_active_bytes=config.max_audit_bytes,
    )
    request.state.reject_reason = reason_code
    return reject(reason_code, reason_code)


def record_pre_ctx_validation_reject(
    config: IntakeConfig,
    request: Request,
    reason_code: audit.AuditReasonCode,
) -> None:
    """Audit a validation reject from code paths that run before RequestContext."""
    state = request.state
    sender_id = getattr(state, "sender_id", None) or MISSING_SENTINEL
    bundle_id = getattr(state, "bundle_id", None) or MISSING_SENTINEL
    audit.append_audit_record(
        config.intake_root,
        audit.AuditAppendParams(
            event="reject",
            sender_id=sender_id,
            bundle_id=bundle_id,
            remote_addr=client_ip(request, config.trust_proxy_headers),
            byte_count=0,
            file_count=0,
            reject_reason=reason_code,
            receipt_sha256="",
        ),
        max_active_bytes=config.max_audit_bytes,
    )
    request.state.reject_reason = reason_code


def client_ip(request: Request, trust_proxy_headers: bool) -> str:
    """Resolve the real client IP, honouring CF-Connecting-IP when trusted."""
    if trust_proxy_headers:
        cf = request.headers.get(HEADER_CF_CONNECTING_IP)
        if cf:
            return cf.strip()
        xff = request.headers.get(HEADER_X_FORWARDED_FOR)
        if xff:
            first = xff.split(",", 1)[0].strip()
            if first:
                return first
    client = request.client
    return client.host if client else "unknown"


def content_length(request: Request) -> int | None:
    raw = request.headers.get("Content-Length")
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
