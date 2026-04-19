"""Unit tests for ami.dataops.report.transport: retry + auth + parsing."""

from __future__ import annotations

import httpx
import pytest
import respx

from ami.dataops.report.manifest import ManifestFileEntry, SenderManifest
from ami.dataops.report.transport import (
    AuthRejected,
    NetworkError,
    PostContext,
    RetryConfig,
    ValidationRejectedByPeer,
    post_bundle,
)

_BAD_REQUEST = 400
_REQUEST_TOO_LARGE = 413
_EXPECTED_RETRY_SLEEPS = 2

ENDPOINT = "https://intake.example.com/v1/bundles"
BEARER = "tok-secret"
SIGNATURE = "sha256=" + "a" * 64
MANIFEST_BYTES = b'{"schema_version":1}\n'
BUNDLE_BYTES = b"\x1f\x8b\x08fake-gzip-bytes"


def _manifest() -> SenderManifest:
    return SenderManifest(
        schema_version=1,
        sender_id="alpha",
        sent_at="2026-04-19T08:12:00Z",
        bundle_id="019237d0-2c41-71a5-9f7e-bd6a10b53c07",
        source_root="/tmp/src",
        files=[
            ManifestFileEntry(
                relative_path="a.log",
                sha256="b" * 64,
                size_bytes=1,
                mtime="2026-04-19T08:11:04Z",
            )
        ],
    )


def _ctx() -> PostContext:
    return PostContext(
        endpoint=ENDPOINT,
        bearer_token=BEARER,
        manifest=_manifest(),
        manifest_bytes=MANIFEST_BYTES,
        signature=SIGNATURE,
        bundle_bytes=BUNDLE_BYTES,
    )


def _fast_retry() -> RetryConfig:
    return RetryConfig(
        max_attempts=3,
        backoff_base_seconds=0.01,
        jitter_fraction=0.0,
        total_budget_seconds=5.0,
        connect_timeout_seconds=1.0,
    )


class _NoSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


class TestPostBundle:
    @respx.mock
    def test_happy_path_returns_receipt(self) -> None:
        route = respx.post(ENDPOINT).mock(
            return_value=httpx.Response(
                202, json={"status": "accept", "bundle_id": "x"}
            )
        )
        receipt = post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())
        assert receipt == {"status": "accept", "bundle_id": "x"}
        assert route.called

    @respx.mock
    def test_200_idempotent_also_accepted(self) -> None:
        respx.post(ENDPOINT).mock(
            return_value=httpx.Response(
                200, json={"status": "accept", "bundle_id": "x"}
            )
        )
        receipt = post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())
        assert receipt["status"] == "accept"

    @respx.mock
    def test_401_raises_auth_rejected(self) -> None:
        respx.post(ENDPOINT).mock(return_value=httpx.Response(401, json={}))
        with pytest.raises(AuthRejected):
            post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())

    @respx.mock
    def test_400_raises_validation_rejected_with_reason(self) -> None:
        respx.post(ENDPOINT).mock(
            return_value=httpx.Response(
                400,
                json={
                    "status": "reject",
                    "reason_code": "ext_not_allowed",
                    "detail": "bad ext",
                },
            )
        )
        with pytest.raises(ValidationRejectedByPeer) as exc:
            post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())
        assert exc.value.reason_code == "ext_not_allowed"
        assert exc.value.status_code == _BAD_REQUEST

    @respx.mock
    def test_413_raises_validation_rejected(self) -> None:
        respx.post(ENDPOINT).mock(
            return_value=httpx.Response(
                413, json={"reason_code": "bundle_too_large", "detail": "big"}
            )
        )
        with pytest.raises(ValidationRejectedByPeer) as exc:
            post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())
        assert exc.value.status_code == _REQUEST_TOO_LARGE

    @respx.mock
    def test_502_retried_then_accepted(self) -> None:
        respx.post(ENDPOINT).mock(
            side_effect=[
                httpx.Response(502, text="bad gateway"),
                httpx.Response(503, text="service unavailable"),
                httpx.Response(202, json={"status": "accept", "bundle_id": "x"}),
            ]
        )
        sleeper = _NoSleep()
        receipt = post_bundle(_ctx(), retry=_fast_retry(), sleep=sleeper)
        assert receipt["status"] == "accept"
        assert len(sleeper.calls) == _EXPECTED_RETRY_SLEEPS

    @respx.mock
    def test_503_exhausts_attempts_raises_network_error(self) -> None:
        respx.post(ENDPOINT).mock(return_value=httpx.Response(503, text="down"))
        with pytest.raises(NetworkError):
            post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())

    @respx.mock
    def test_429_honours_retry_after_then_accepts(self) -> None:
        respx.post(ENDPOINT).mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0"}, text="busy"),
                httpx.Response(202, json={"status": "accept", "bundle_id": "x"}),
            ]
        )
        sleeper = _NoSleep()
        receipt = post_bundle(_ctx(), retry=_fast_retry(), sleep=sleeper)
        assert receipt["status"] == "accept"
        assert sleeper.calls == [0.0]

    @respx.mock
    def test_connect_error_retried(self) -> None:
        respx.post(ENDPOINT).mock(
            side_effect=[
                httpx.ConnectError("no route"),
                httpx.Response(202, json={"status": "accept", "bundle_id": "x"}),
            ]
        )
        receipt = post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())
        assert receipt["status"] == "accept"

    @respx.mock
    def test_unexpected_status_raises_network_error(self) -> None:
        respx.post(ENDPOINT).mock(return_value=httpx.Response(418, text="teapot"))
        with pytest.raises(NetworkError):
            post_bundle(_ctx(), retry=_fast_retry(), sleep=_NoSleep())
