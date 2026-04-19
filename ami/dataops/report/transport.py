"""httpx-based multipart POST of a signed bundle to a peer intake daemon.

Implements the sender-side contract from SPEC-REPORT §4 + the retry
policy from REQ-REPORT §5: three attempts with exponential backoff,
honouring `Retry-After` on 429, within a 300 s total budget. Distinct
typed exceptions map directly onto ami-report's exit codes so the CLI
can translate a POST outcome into the right process exit.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable

import httpx
from pydantic import BaseModel, ConfigDict, Field

from ami.dataops.report.manifest import SenderManifest

SleepFn = Callable[[float], None]
RandFn = Callable[[], float]

DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
DEFAULT_TOTAL_TIMEOUT_SECONDS = 300.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
DEFAULT_BACKOFF_JITTER_FRAC = 0.25
HEADER_BUNDLE_ID = "X-AMI-Bundle-Id"
HEADER_SENDER_ID = "X-AMI-Sender-Id"
HEADER_SIGNATURE = "X-AMI-Signature"
RETRY_STATUS_CODES = frozenset({502, 503, 504})


class TransportError(Exception):
    """Base for typed sender-side transport failures."""


class NetworkError(TransportError):
    """Connection refused / TLS failure / DNS / timeout after retries."""


class AuthRejected(TransportError):
    """Receiver returned 401 (bearer or HMAC mismatch)."""


class ValidationRejectedByPeer(TransportError):
    """Receiver returned 400 / 413 (content or size violated policy)."""

    def __init__(self, status_code: int, reason_code: str, detail: str) -> None:
        super().__init__(f"{status_code} {reason_code}: {detail}")
        self.status_code = status_code
        self.reason_code = reason_code
        self.detail = detail


class RateLimited(TransportError):
    """Receiver returned 429 past the total budget."""


class RetryConfig(BaseModel):
    """Knobs for the POST retry loop."""

    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=DEFAULT_MAX_ATTEMPTS, gt=0)
    backoff_base_seconds: float = Field(default=DEFAULT_BACKOFF_BASE_SECONDS, gt=0)
    jitter_fraction: float = Field(default=DEFAULT_BACKOFF_JITTER_FRAC, ge=0)
    total_budget_seconds: float = Field(default=DEFAULT_TOTAL_TIMEOUT_SECONDS, gt=0)
    connect_timeout_seconds: float = Field(
        default=DEFAULT_CONNECT_TIMEOUT_SECONDS, gt=0
    )


class PostContext(BaseModel):
    """Inputs to `post_bundle`, grouped so the call site stays readable."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    endpoint: str
    bearer_token: str
    manifest: SenderManifest
    manifest_bytes: bytes
    signature: str
    bundle_bytes: bytes


def post_bundle(
    ctx: PostContext,
    *,
    retry: RetryConfig | None = None,
    client: httpx.Client | None = None,
    sleep: SleepFn | None = None,
    rng: RandFn | None = None,
) -> dict[str, object]:
    """POST the signed bundle; return the receiver's JSON receipt on accept.

    Retries on transient 5xx (502/503/504) and 429 (with Retry-After
    honoured). All retries share the `retry.total_budget_seconds`
    budget; when the budget runs out on a 429 the call raises
    RateLimited. Terminal 4xx (other than 429) raise the matching typed
    exception immediately.

    `client`, `sleep`, `rng` are injected for deterministic tests.
    """
    cfg = retry or RetryConfig()
    _sleep = sleep or time.sleep
    _rand = rng or random.random
    owned_client = client is None
    http = client or _build_client(cfg)
    deadline = time.monotonic() + cfg.total_budget_seconds
    try:
        last_exception: TransportError | None = None
        for attempt in range(cfg.max_attempts):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                return _one_post(http, ctx)
            except _TransientHTTP as exc:
                last_exception = NetworkError(str(exc))
                wait = _compute_backoff(attempt, cfg, _rand, exc.retry_after)
                if time.monotonic() + wait > deadline:
                    break
                _sleep(wait)
            except (httpx.ConnectError, httpx.ReadError, httpx.WriteError) as exc:
                last_exception = NetworkError(f"{type(exc).__name__}: {exc}")
                wait = _compute_backoff(attempt, cfg, _rand, None)
                if time.monotonic() + wait > deadline:
                    break
                _sleep(wait)
        raise last_exception or NetworkError("exhausted retries with no response")
    finally:
        if owned_client:
            http.close()


def _build_client(cfg: RetryConfig) -> httpx.Client:
    timeout = httpx.Timeout(
        connect=cfg.connect_timeout_seconds,
        read=cfg.total_budget_seconds,
        write=cfg.total_budget_seconds,
        pool=cfg.connect_timeout_seconds,
    )
    return httpx.Client(timeout=timeout)


class _TransientHTTP(Exception):
    """Internal marker for 5xx/429 responses that the retry loop should handle."""

    def __init__(self, status_code: int, retry_after: float | None) -> None:
        super().__init__(f"transient HTTP {status_code}")
        self.status_code = status_code
        self.retry_after = retry_after


def _one_post(http: httpx.Client, ctx: PostContext) -> dict[str, object]:
    headers = {
        "Authorization": f"Bearer {ctx.bearer_token}",
        HEADER_SENDER_ID: ctx.manifest.sender_id,
        HEADER_BUNDLE_ID: ctx.manifest.bundle_id,
        HEADER_SIGNATURE: ctx.signature,
    }
    files = {
        "manifest": ("manifest.json", ctx.manifest_bytes, "application/json"),
        "bundle": ("bundle.tar.gz", ctx.bundle_bytes, "application/gzip"),
    }
    response = http.post(ctx.endpoint, headers=headers, files=files)
    return _handle_response(response)


def _handle_response(response: httpx.Response) -> dict[str, object]:
    accept_codes = {httpx.codes.OK, httpx.codes.ACCEPTED}
    if response.status_code in accept_codes:
        body = response.json()
        if not isinstance(body, dict):
            msg = "receiver returned non-object JSON"
            raise NetworkError(msg)
        return dict(body)
    if response.status_code == httpx.codes.UNAUTHORIZED:
        raise AuthRejected(response.text or "auth rejected")
    terminal_reject_codes = {
        httpx.codes.BAD_REQUEST,
        httpx.codes.REQUEST_ENTITY_TOO_LARGE,
    }
    if response.status_code in terminal_reject_codes:
        raise _terminal_reject(response)
    if response.status_code == httpx.codes.TOO_MANY_REQUESTS:
        raise _TransientHTTP(response.status_code, _parse_retry_after(response))
    if response.status_code in RETRY_STATUS_CODES:
        raise _TransientHTTP(response.status_code, None)
    msg = f"unexpected HTTP {response.status_code}: {response.text[:200]}"
    raise NetworkError(msg)


def _terminal_reject(response: httpx.Response) -> ValidationRejectedByPeer:
    reason = "unknown"
    detail = ""
    try:
        body = response.json()
        reason = str(body.get("reason_code", "unknown"))
        detail = str(body.get("detail", ""))
    except (ValueError, TypeError):
        detail = response.text[:200]
    return ValidationRejectedByPeer(response.status_code, reason, detail)


def _parse_retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _compute_backoff(
    attempt: int,
    cfg: RetryConfig,
    rand: RandFn,
    retry_after: float | None,
) -> float:
    if retry_after is not None:
        return max(retry_after, 0.0)
    base = cfg.backoff_base_seconds * (4**attempt)
    jitter = base * cfg.jitter_fraction * (2 * rand() - 1)
    return max(float(base + jitter), 0.0)
