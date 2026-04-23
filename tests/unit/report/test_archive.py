"""Unit tests for ami.dataops.report.archive."""

from __future__ import annotations

from pathlib import Path

from ami.dataops.report.archive import (
    ARCHIVE_PREVIEW_FILE_LIMIT,
    build_signed_bundle,
    render_archive_summary,
)
from ami.dataops.report.config import PeerEntry
from ami.dataops.report.manifest import verify_signature
from ami.dataops.report.models import (
    ArchiveSummary,
    PeerCredentials,
    SendPlan,
)
from ami.dataops.report.scanner import CandidateFile

_TEN_KIB = 10 * 1024
_SECRET = "hunter2"


def _make_candidate(root: Path, rel: str, body: str) -> CandidateFile:
    absolute = root / rel
    absolute.parent.mkdir(parents=True, exist_ok=True)
    absolute.write_text(body)
    return CandidateFile(
        absolute_path=absolute,
        relative_path=rel,
        size_bytes=len(body.encode("utf-8")),
        preflight="ok",
        mtime_epoch=absolute.stat().st_mtime,
    )


def _peer() -> PeerEntry:
    return PeerEntry.model_validate(
        {
            "name": "bravo",
            "endpoint": "https://intake.example.com/",
            "shared_secret_env_var": "SECRET_BRAVO",
        }
    )


class TestBuildSignedBundle:
    def test_signature_verifies_round_trip(self, tmp_path: Path) -> None:
        files = [_make_candidate(tmp_path, "a.log", "hello\n")]
        plan = SendPlan(
            sender_id="alpha",
            peer=_peer(),
            source_root=tmp_path,
            files=files,
        )
        creds = PeerCredentials(shared_secret=_SECRET, bearer_token="t")
        bundle = build_signed_bundle(plan, creds)
        assert verify_signature(bundle.canonical_bytes, bundle.signature, _SECRET)
        assert bundle.archive_summary.files == files
        assert bundle.archive_summary.uncompressed_bytes == files[0].size_bytes
        assert bundle.tar_bytes.startswith(b"\x1f\x8b")  # gzip magic

    def test_manifest_covers_every_file(self, tmp_path: Path) -> None:
        files = [
            _make_candidate(tmp_path, "a.log", "one\n"),
            _make_candidate(tmp_path, "nested/b.log", "two\n"),
        ]
        plan = SendPlan(
            sender_id="alpha",
            peer=_peer(),
            source_root=tmp_path,
            files=files,
        )
        bundle = build_signed_bundle(
            plan, PeerCredentials(shared_secret=_SECRET, bearer_token="t")
        )
        assert [e.relative_path for e in bundle.manifest.files] == [
            "a.log",
            "nested/b.log",
        ]


class TestRenderArchiveSummary:
    def test_lists_files_and_sizes(self, tmp_path: Path) -> None:
        cand = _make_candidate(tmp_path, "app.log", "body\n")
        summary = ArchiveSummary(
            compressed_bytes=_TEN_KIB,
            uncompressed_bytes=len("body\n"),
            files=[cand],
        )
        rendered = render_archive_summary(summary)
        assert "Archive:" in rendered
        assert "app.log" in rendered
        assert "Review complete?" in rendered

    def test_truncates_past_limit(self, tmp_path: Path) -> None:
        overflow = 3
        files = [
            _make_candidate(tmp_path, f"{i:03d}.log", "x\n")
            for i in range(ARCHIVE_PREVIEW_FILE_LIMIT + overflow)
        ]
        summary = ArchiveSummary(
            compressed_bytes=_TEN_KIB,
            uncompressed_bytes=_TEN_KIB,
            files=files,
        )
        rendered = render_archive_summary(summary)
        assert f"(+{overflow} more)" in rendered
