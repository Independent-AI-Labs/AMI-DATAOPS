"""Unit tests for ami.dataops.intake.stream.CappedAsyncStream."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from ami.dataops.intake.stream import BodyTooLargeError, CappedAsyncStream

_EXPECTED_TOTAL_BYTES = 4
_HTTP_CONTENT_TOO_LARGE = 413


async def _source(chunks: list[bytes]) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


class TestCappedAsyncStream:
    @pytest.mark.asyncio
    async def test_yields_all_chunks_within_cap(self) -> None:
        stream = CappedAsyncStream(_source([b"ab", b"cd"]), max_bytes=10)
        collected = [chunk async for chunk in stream]
        assert collected == [b"ab", b"cd"]
        assert stream.bytes_read == _EXPECTED_TOTAL_BYTES

    @pytest.mark.asyncio
    async def test_raises_on_first_chunk_over_cap(self) -> None:
        stream = CappedAsyncStream(_source([b"abc", b"def"]), max_bytes=4)
        async_iter = stream.__aiter__()
        first = await async_iter.__anext__()
        assert first == b"abc"
        with pytest.raises(BodyTooLargeError) as exc:
            await async_iter.__anext__()
        assert exc.value.status_code == _HTTP_CONTENT_TOO_LARGE

    @pytest.mark.asyncio
    async def test_empty_stream(self) -> None:
        stream = CappedAsyncStream(_source([]), max_bytes=4)
        collected = [chunk async for chunk in stream]
        assert collected == []
        assert stream.bytes_read == 0

    @pytest.mark.asyncio
    async def test_at_exact_cap_accepts(self) -> None:
        stream = CappedAsyncStream(_source([b"abcd"]), max_bytes=4)
        collected = [chunk async for chunk in stream]
        assert collected == [b"abcd"]
