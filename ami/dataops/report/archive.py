"""Signed-bundle assembly + archive preview rendering.

Composes `manifest.build_manifest` + `manifest.canonical_manifest_bytes` +
`manifest.sign_manifest` + `bundling.build_bundle_tarball` into a single
`build_signed_bundle(plan, creds)` entry point that returns a typed
`SignedBundle` holding every artifact the pipeline needs downstream.
"""

from __future__ import annotations

from ami.dataops.report import manifest as manifest_mod
from ami.dataops.report.bundling import build_bundle_tarball
from ami.dataops.report.models import (
    ArchiveSummary,
    PeerCredentials,
    SendPlan,
    SignedBundle,
)

BYTES_PER_KIB = 1024.0
KIB_PER_MIB = 1024.0
ARCHIVE_PREVIEW_FILE_LIMIT = 20


def _format_size(size_bytes: int) -> str:
    kib = size_bytes / BYTES_PER_KIB
    if kib < KIB_PER_MIB:
        return f"{kib:.1f} KiB"
    return f"{kib / KIB_PER_MIB:.1f} MiB"


def build_signed_bundle(plan: SendPlan, creds: PeerCredentials) -> SignedBundle:
    """Build manifest + tarball, sign, and return the full `SignedBundle`."""
    manifest = manifest_mod.build_manifest(
        sender_id=plan.sender_id,
        source_root=plan.source_root,
        files=[c.absolute_path for c in plan.files],
    )
    canonical_bytes = manifest_mod.canonical_manifest_bytes(manifest)
    signature = manifest_mod.sign_manifest(canonical_bytes, creds.shared_secret)
    tar_bytes = build_bundle_tarball(manifest, plan.source_root)
    summary = ArchiveSummary(
        compressed_bytes=len(tar_bytes),
        uncompressed_bytes=sum(c.size_bytes for c in plan.files),
        files=plan.files,
    )
    return SignedBundle(
        manifest=manifest,
        canonical_bytes=canonical_bytes,
        signature=signature,
        tar_bytes=tar_bytes,
        archive_summary=summary,
    )


def render_archive_summary(summary: ArchiveSummary) -> str:
    """Format the archive-preview screen body. Pure function so tests verify it."""
    head = (
        f"Archive:  {_format_size(summary.compressed_bytes)} compressed  /  "
        f"{_format_size(summary.uncompressed_bytes)} uncompressed\n"
        f"Files:    {len(summary.files)}\n\n"
    )
    shown = summary.files[:ARCHIVE_PREVIEW_FILE_LIMIT]
    lines = [
        f"  {candidate.relative_path:<60}{_format_size(candidate.size_bytes)}"
        for candidate in shown
    ]
    extras = len(summary.files) - len(shown)
    if extras > 0:
        lines.append(f"  (+{extras} more)")
    return head + "\n".join(lines) + "\n\nReview complete?"
