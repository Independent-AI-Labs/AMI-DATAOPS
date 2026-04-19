"""Unit tests for ami.dataops.intake.quarantine."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ami.dataops.intake.quarantine import (
    DATA_FILE_MODE,
    METADATA_FILE_MODE,
    QUARANTINE_DIR_MODE,
    FinalizeRequest,
    ReceiptFileEntry,
    ReceiptModel,
    bundle_exists,
    finalize_bundle,
    quarantine_path_for,
    read_receipt,
)


def _receipt(bundle_id: str, entries: list[tuple[str, str, int]]) -> ReceiptModel:
    return ReceiptModel(
        bundle_id=bundle_id,
        received_at="2026-04-19T08:12:01Z",
        per_file_sha256_verified=[
            ReceiptFileEntry(relative_path=p, sha256=h, size_bytes=s)
            for p, h, s in entries
        ],
        audit_log_offset=0,
    )


_FIXED_NOW = datetime(2026, 4, 19, 8, 12, 1, tzinfo=timezone.utc)
_EXPECTED_YEAR = "2026"
_EXPECTED_MONTH = "04"
_EXPECTED_DAY = "19"


class TestQuarantinePathFor:
    def test_path_is_dated_tree(self, tmp_path: Path) -> None:
        path = quarantine_path_for(tmp_path, "alpha", "bundle-abc", now=_FIXED_NOW)
        assert path == (
            tmp_path
            / "alpha"
            / _EXPECTED_YEAR
            / _EXPECTED_MONTH
            / _EXPECTED_DAY
            / "bundle-abc"
        )


class TestFinalizeBundle:
    def _staging(self, tmp_path: Path) -> Path:
        staging = tmp_path / "stage"
        (staging / "nested").mkdir(parents=True)
        (staging / "a.log").write_bytes(b"alpha\n")
        (staging / "nested" / "b.log").write_bytes(b"beta\n")
        return staging

    def _request(self, bundle_id: str, *, entries: list) -> FinalizeRequest:
        return FinalizeRequest(
            sender_id="alpha",
            bundle_id=bundle_id,
            manifest_bytes=b'{"sender_id":"alpha"}\n',
            receipt=_receipt(bundle_id, entries),
            now=_FIXED_NOW,
        )

    def test_finalize_moves_files_and_writes_metadata(self, tmp_path: Path) -> None:
        intake_root = tmp_path / "intake"
        staging = self._staging(tmp_path)
        request = self._request(
            "b-1", entries=[("a.log", "a" * 64, 6), ("nested/b.log", "b" * 64, 5)]
        )
        target = finalize_bundle(staging, intake_root, request)
        assert target.is_dir()
        assert (target / "a.log").read_bytes() == b"alpha\n"
        assert (target / "nested" / "b.log").read_bytes() == b"beta\n"
        assert (target / "manifest.json").read_bytes() == b'{"sender_id":"alpha"}\n'
        assert read_receipt(target).bundle_id == "b-1"
        assert not staging.exists()

    def test_permissions_applied(self, tmp_path: Path) -> None:
        intake_root = tmp_path / "intake"
        staging = self._staging(tmp_path)
        request = self._request("b-2", entries=[("a.log", "h" * 64, 1)])
        target = finalize_bundle(staging, intake_root, request)
        assert target.stat().st_mode & 0o777 == QUARANTINE_DIR_MODE
        assert (target / "a.log").stat().st_mode & 0o777 == DATA_FILE_MODE
        mf_mode = (target / "manifest.json").stat().st_mode & 0o777
        rc_mode = (target / "receipt.json").stat().st_mode & 0o777
        assert mf_mode == METADATA_FILE_MODE
        assert rc_mode == METADATA_FILE_MODE

    def test_collision_raises(self, tmp_path: Path) -> None:
        intake_root = tmp_path / "intake"
        staging = self._staging(tmp_path)
        finalize_bundle(
            staging,
            intake_root,
            self._request("b-3", entries=[]),
        )
        second_staging = self._staging(tmp_path / "round2")
        with pytest.raises(FileExistsError):
            finalize_bundle(
                second_staging,
                intake_root,
                self._request("b-3", entries=[]),
            )


class TestBundleExists:
    def test_returns_none_for_unknown(self, tmp_path: Path) -> None:
        assert bundle_exists(tmp_path, "alpha", "nope") is None

    def test_finds_existing_bundle(self, tmp_path: Path) -> None:
        intake_root = tmp_path / "intake"
        staging = tmp_path / "stage"
        staging.mkdir()
        (staging / "a.log").write_bytes(b"x")
        finalize_bundle(
            staging,
            intake_root,
            FinalizeRequest(
                sender_id="alpha",
                bundle_id="b-ok",
                manifest_bytes=b"{}",
                receipt=_receipt("b-ok", [("a.log", "h" * 64, 1)]),
                now=_FIXED_NOW,
            ),
        )
        found = bundle_exists(intake_root, "alpha", "b-ok")
        assert found is not None
        assert (found / "receipt.json").is_file()
