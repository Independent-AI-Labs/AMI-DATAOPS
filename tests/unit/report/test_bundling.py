"""Unit tests for ami.dataops.report.bundling."""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

from ami.dataops.report.bundling import build_bundle_tarball
from ami.dataops.report.manifest import ManifestFileEntry, SenderManifest


class TestBuildBundleTarball:
    def test_tar_contains_every_manifest_file(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("alpha\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("beta\n")
        manifest = SenderManifest(
            schema_version=1,
            sender_id="alpha",
            sent_at="2026-04-19T08:12:00Z",
            bundle_id="019237d0-2c41-71a5-9f7e-bd6a10b53c07",
            source_root=str(tmp_path),
            files=[
                ManifestFileEntry(
                    relative_path="a.log",
                    sha256="a" * 64,
                    size_bytes=6,
                    mtime="2026-04-19T08:11:04Z",
                ),
                ManifestFileEntry(
                    relative_path="nested/b.log",
                    sha256="b" * 64,
                    size_bytes=5,
                    mtime="2026-04-19T08:11:04Z",
                ),
            ],
        )
        payload = build_bundle_tarball(manifest, tmp_path)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
            names = sorted(m.name for m in tar.getmembers())
        assert names == ["a.log", "nested/b.log"]
