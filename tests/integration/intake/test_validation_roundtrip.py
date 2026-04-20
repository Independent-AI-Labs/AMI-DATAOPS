"""Integration: build a real gzipped tar bundle on disk, extract via the
intake validator, and verify every extracted file matches its source by SHA256.

This exercises the full `extract_bundle_stream` → `verify_hash` pipeline
end-to-end without any mock, which is what the pre-push integration-coverage
gate expects before the full report↔intake loopback lands in commit 12.
"""

from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from ami.dataops.intake import validation as v


def _build_bundle(source_dir: Path) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path in sorted(source_dir.rglob("*")):
            if not path.is_file():
                continue
            tar.add(path, arcname=path.relative_to(source_dir).as_posix())
    return buf.getvalue()


class TestExtractRoundTrip:
    def test_three_file_roundtrip_hashes_match(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        (source / "nested").mkdir(parents=True)
        files = {
            "root.log": b"one\ntwo\nthree\n",
            "nested/app.log": b"event payload with spaces",
            "nested/trace.txt": b"line-ts-2026-04-19\n",
        }
        expected_hashes: dict[str, str] = {}
        for rel, payload in files.items():
            (source / rel).write_bytes(payload)
            expected_hashes[rel] = hashlib.sha256(payload).hexdigest()

        bundle_bytes = _build_bundle(source)
        staging = tmp_path / "stage"
        extracted = v.extract_bundle_stream(io.BytesIO(bundle_bytes), staging)
        assert len(extracted) == len(files)
        for path in extracted:
            rel = path.relative_to(staging).as_posix()
            v.verify_hash(path, expected_hashes[rel])

    def test_bundle_with_mixed_disallowed_ext_rejects_whole(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        (source / "ok.log").write_bytes(b"fine\n")
        (source / "bad.exe").write_bytes(b"MZ not allowed")
        bundle_bytes = _build_bundle(source)
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(io.BytesIO(bundle_bytes), tmp_path / "stage")
        assert exc.value.reason_code == "ext_not_allowed"

    def test_aggregate_cap_aborts_before_all_bytes_written(
        self, tmp_path: Path
    ) -> None:
        source = tmp_path / "src"
        source.mkdir()
        (source / "a.log").write_bytes(b"x" * 4000)
        (source / "b.log").write_bytes(b"y" * 4000)
        bundle_bytes = _build_bundle(source)
        with pytest.raises(v.ValidationRejected) as exc:
            v.extract_bundle_stream(
                io.BytesIO(bundle_bytes),
                tmp_path / "stage",
                max_bundle_bytes=5000,
            )
        assert exc.value.reason_code == "bundle_too_large"
