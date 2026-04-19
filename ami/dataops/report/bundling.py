"""Assemble the gzipped-tar bundle that ships alongside the signed manifest.

Pure callable; no network, no TUI. Intake's extract_bundle_stream is the
receiver counterpart that reads this tar back.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

from ami.dataops.report.manifest import SenderManifest


def build_bundle_tarball(manifest: SenderManifest, source_root: Path) -> bytes:
    """Return the gzip-tar bytes for every file listed in `manifest.files`.

    Each entry is added at its `relative_path` under the tar root so the
    receiver extracts into `<staging>/relative_path` directly. Gzip is
    the only compression accepted by intake; other codecs are out of
    scope for v1.
    """
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for entry in manifest.files:
            absolute = source_root / entry.relative_path
            tar.add(absolute, arcname=entry.relative_path, recursive=False)
    return buf.getvalue()
