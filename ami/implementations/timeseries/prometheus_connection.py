"""Prometheus connection management.

Encapsulates the HTTP session lifecycle and base-URL construction
for communicating with Prometheus / VictoriaMetrics APIs.
"""

from __future__ import annotations

import logging

import aiohttp

from ami.models.storage_config import StorageConfig

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30
DEFAULT_PORT = 9090
# HTTP status threshold for success responses (2xx)
_HTTP_SUCCESS_MAX = 300


def build_base_url(config: StorageConfig | None) -> str:
    """Derive the Prometheus base URL from a ``StorageConfig``."""
    if config and config.connection_string:
        return config.connection_string.rstrip("/")
    host = (config.host if config else None) or "localhost"
    port = (config.port if config else None) or DEFAULT_PORT
    return f"http://{host}:{port}"


async def create_session(
    config: StorageConfig | None = None,
) -> aiohttp.ClientSession:
    """Create an ``aiohttp.ClientSession`` for Prometheus requests."""
    timeout_seconds = DEFAULT_TIMEOUT
    if config and config.options:
        timeout_seconds = int(config.options.get("timeout", DEFAULT_TIMEOUT))

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    headers: dict[str, str] = {
        "Accept": "application/json",
    }
    if config and config.options:
        token = config.options.get("auth_token")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        api_key = config.options.get("api_key")
        if api_key:
            headers["X-API-Key"] = api_key

    return aiohttp.ClientSession(headers=headers, timeout=timeout)


async def close_session(session: aiohttp.ClientSession | None) -> None:
    """Safely close an ``aiohttp.ClientSession``."""
    if session and not session.closed:
        await session.close()
        logger.debug("Prometheus HTTP session closed")


async def health_check(
    session: aiohttp.ClientSession,
    base_url: str,
) -> bool:
    """Probe the ``/-/healthy`` endpoint.

    Returns *True* if the server responds with 2xx.
    """
    url = f"{base_url}/-/healthy"
    try:
        async with session.get(url) as resp:
            return resp.status < _HTTP_SUCCESS_MAX
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.warning("Prometheus health check failed: %s", exc)
        return False


async def check_ready(
    session: aiohttp.ClientSession,
    base_url: str,
) -> bool:
    """Probe the ``/-/ready`` endpoint.

    Returns *True* if the server reports ready.
    """
    url = f"{base_url}/-/ready"
    try:
        async with session.get(url) as resp:
            return resp.status < _HTTP_SUCCESS_MAX
    except (aiohttp.ClientError, TimeoutError) as exc:
        logger.warning("Prometheus readiness check failed: %s", exc)
        return False
