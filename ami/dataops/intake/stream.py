"""Size-capped async stream wrapper for the intake request body.

Wraps Starlette's `Request.stream()` iterator so a running byte counter
aborts the read at `max_bytes`. Lives upstream of the multipart parser so
the 413 fires before any part spools to disk. See REQ-REPORT R-REPORT-150
+ SPEC-REPORT §10.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import HTTPException, status


class BodyTooLargeError(HTTPException):
    """413 raised when the request body exceeds the configured cap."""

    def __init__(self, total_bytes: int, max_bytes: int) -> None:
        super().__init__(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"bundle exceeded {max_bytes} bytes at {total_bytes}",
        )


class CappedAsyncStream:
    """Async iterator over an underlying async byte stream, enforcing `max_bytes`.

    Iteration raises `BodyTooLargeError` on the first chunk that would push
    the running total past the cap; no partial chunk is yielded past the cap.
    """

    def __init__(self, stream: AsyncIterator[bytes], max_bytes: int) -> None:
        self._stream = stream
        self._max_bytes = max_bytes
        self._total = 0

    @property
    def bytes_read(self) -> int:
        return self._total

    def __aiter__(self) -> CappedAsyncStream:
        return self

    async def __anext__(self) -> bytes:
        chunk = await self._stream.__anext__()
        self._total += len(chunk)
        if self._total > self._max_bytes:
            raise BodyTooLargeError(self._total, self._max_bytes)
        return chunk
