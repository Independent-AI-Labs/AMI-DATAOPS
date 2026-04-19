"""Integration: build a report bundle on disk, then pipe it through the
intake extractor end-to-end, confirming sender + receiver agree on hashes.

Lightweight precursor to the full uvicorn loopback; catches any drift
between the sender-side bundle layout and the receiver-side extractor
without a real HTTP exchange.
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

from ami.dataops.intake import validation
from ami.dataops.report.bundling import build_bundle_tarball
from ami.dataops.report.manifest import build_manifest


class TestSenderReceiverRoundTrip:
    def test_extracted_files_match_manifest_hashes(self, tmp_path: Path) -> None:
        source = tmp_path / "src"
        (source / "nested").mkdir(parents=True)
        files = {
            "app.log": b"alpha entry\n",
            "nested/trace.ndjson": b'{"ok":true}\n',
        }
        expected: dict[str, str] = {}
        for rel, payload in files.items():
            (source / rel).write_bytes(payload)
            expected[rel] = hashlib.sha256(payload).hexdigest()

        manifest = build_manifest(
            sender_id="alpha",
            source_root=source,
            files=[source / rel for rel in files],
        )
        bundle_bytes = build_bundle_tarball(manifest, source)

        staging = tmp_path / "stage"
        extracted = validation.extract_bundle_stream(io.BytesIO(bundle_bytes), staging)
        for path in extracted:
            rel = path.relative_to(staging).as_posix()
            validation.verify_hash(path, expected[rel])
