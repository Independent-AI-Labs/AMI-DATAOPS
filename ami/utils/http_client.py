"""Shared HTTP client utilities for DAO implementations.

Provides retry logic and error handling for HTTP-based storage backends.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
from aiohttp import ClientResponse, ClientSession
from pydantic import BaseModel, ConfigDict

from ami.core.exceptions import StorageError

logger = logging.getLogger(__name__)

# HTTP status codes
HTTP_TOO_MANY_REQUESTS = 429
HTTP_INTERNAL_SERVER_ERROR = 500

# Retry configuration
DEFAULT_MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 0.5  # seconds
MAX_RETRY_DELAY = 10.0  # seconds
BACKOFF_MULTIPLIER = 2.0


class RetryConfig(BaseModel):
    """Configuration for HTTP request retry behaviour."""

    model_config = ConfigDict(frozen=True)

    max_retries: int = DEFAULT_MAX_RETRIES
    initial_delay: float = INITIAL_RETRY_DELAY
    max_delay: float = MAX_RETRY_DELAY
    backoff_multiplier: float = BACKOFF_MULTIPLIER


_DEFAULT_RETRY = RetryConfig()


async def request_with_retry(
    session: ClientSession,
    method: str,
    url: str,
    *,
    retry_cfg: RetryConfig = _DEFAULT_RETRY,
    **kwargs: Any,
) -> ClientResponse:
    """Execute HTTP request with exponential backoff retry logic.

    Retries on:
    - Network errors (ClientError, TimeoutError)
    - Server errors (5xx status codes)
    - Rate limiting (429 Too Many Requests)

    Does NOT retry on:
    - Client errors (4xx except 429)
    - Successful responses (2xx, 3xx)

    Args:
        session: aiohttp ClientSession
        method: HTTP method (GET, POST, etc.)
        url: Request URL
        retry_cfg: Retry configuration parameters
        **kwargs: Additional arguments passed to session.request()

    Returns:
        ClientResponse object (caller must use async with)

    Raises:
        StorageError: If all retries fail
    """
    max_retries = retry_cfg.max_retries
    retry_delay = retry_cfg.initial_delay
    last_exception: Exception | None = None

    for attempt in range(max_retries):
        try:
            response = await session.request(
                method,
                url,
                **kwargs,
            )

            if (
                response.status >= HTTP_INTERNAL_SERVER_ERROR
                or response.status == HTTP_TOO_MANY_REQUESTS
            ):
                is_last = attempt == max_retries - 1

                if is_last:
                    try:
                        error_text = await response.text()
                    except Exception:
                        error_text = "<unreadable body>"

                    logger.error(
                        "Request to %s failed after %d attempts with status %d: %s",
                        url,
                        max_retries,
                        response.status,
                        error_text,
                    )
                    msg = (
                        f"HTTP {max_retries} retries: "
                        f"{response.status} "
                        f"{response.reason}"
                    )
                    raise StorageError(msg)

                logger.warning(
                    "Request to %s returned %d, retrying (%d/%d) in %ss",
                    url,
                    response.status,
                    attempt + 1,
                    max_retries,
                    retry_delay,
                )
                await response.release()
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * retry_cfg.backoff_multiplier,
                    retry_cfg.max_delay,
                )
                continue

        except (TimeoutError, aiohttp.ClientError) as e:
            is_last = attempt == max_retries - 1
            last_exception = e

            if is_last:
                logger.exception(
                    "Request to %s failed after %d attempts",
                    url,
                    max_retries,
                )
                msg = f"HTTP request failed after {max_retries} retries"
                raise StorageError(msg) from e

            logger.warning(
                "Request to %s failed with %s, retrying (%d/%d) in %ss: %s",
                url,
                type(e).__name__,
                attempt + 1,
                max_retries,
                retry_delay,
                e,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(
                retry_delay * retry_cfg.backoff_multiplier,
                retry_cfg.max_delay,
            )
        else:
            return response

    # Should never reach here due to raise in loop
    if last_exception:
        msg = f"HTTP request failed: {last_exception}"
        raise StorageError(msg) from last_exception
    msg = "Unexpected retry loop exit"
    raise StorageError(msg)
