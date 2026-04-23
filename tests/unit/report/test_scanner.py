"""Unit tests for ami.dataops.report.scanner."""

from __future__ import annotations

import os
from pathlib import Path

from ami.dataops.report.scanner import (
    CandidateFile,
    FolderEntry,
    expand_selection,
    files_only,
    filter_by_window,
    scan_roots,
)

_EXPECTED_OK_COUNT = 3


def _pick_file(entries: list, name: str) -> CandidateFile:
    matches = [
        e
        for e in entries
        if isinstance(e, CandidateFile) and e.relative_path.endswith(name)
    ]
    return matches[0]


class TestScanRoots:
    def test_scans_directory_recursively(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("one\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("two\n")
        (tmp_path / "nested" / "c.log").write_text("trace\n")
        entries = scan_roots([tmp_path])
        files = files_only(entries)
        assert len([f for f in files if f.toggleable]) == _EXPECTED_OK_COUNT

    def test_missing_roots_skipped(self, tmp_path: Path) -> None:
        assert scan_roots([tmp_path / "nope"]) == []

    def test_root_folder_entry_precedes_children(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("one\n")
        entries = scan_roots([tmp_path])
        assert isinstance(entries[0], FolderEntry)
        assert entries[0].absolute_path == tmp_path.resolve()

    def test_pre_order_traversal_with_depth(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("2\n")
        entries = scan_roots([tmp_path])
        depths = [(type(e).__name__, e.depth, e.relative_path) for e in entries]
        # root folder at depth 0, then its direct file, then nested folder at
        # depth 1 followed by its file at depth 2.
        assert depths[0] == ("FolderEntry", 0, tmp_path.name)
        assert depths[1][0] == "CandidateFile"
        assert depths[1][1] == 1

    def test_disallowed_extension_files_are_skipped_entirely(
        self, tmp_path: Path
    ) -> None:
        """Extension mismatches drop out; size/text rejects still surface."""
        (tmp_path / "ok.log").write_text("ok\n")
        (tmp_path / "bad.exe").write_bytes(b"MZ")
        entries = scan_roots([tmp_path])
        files = {f.relative_path.rsplit("/", 1)[-1]: f for f in files_only(entries)}
        assert files["ok.log"].preflight == "ok"
        assert "bad.exe" not in files

    def test_folder_count_reflects_descendants(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("2\n")
        (tmp_path / "nested" / "c.log").write_text("3\n")
        entries = scan_roots([tmp_path])
        root = entries[0]
        assert isinstance(root, FolderEntry)
        assert root.descendant_file_count == _EXPECTED_OK_COUNT

    def test_direct_file_path_scanned(self, tmp_path: Path) -> None:
        file_path = tmp_path / "single.log"
        file_path.write_text("solo\n")
        entries = scan_roots([file_path])
        assert len(entries) == 1
        assert isinstance(entries[0], CandidateFile)

    def test_symlinks_are_skipped(self, tmp_path: Path) -> None:
        real = tmp_path / "real.log"
        real.write_text("real\n")
        (tmp_path / "link.log").symlink_to(real)
        files = files_only(scan_roots([tmp_path]))
        names = {f.relative_path.rsplit("/", 1)[-1] for f in files}
        assert "link.log" not in names
        assert "real.log" in names


class TestMtimePopulation:
    def test_candidate_files_carry_mtime(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("alpha\n")
        files = files_only(scan_roots([tmp_path]))
        assert len(files) == 1
        assert files[0].mtime_epoch > 0


class TestFilterByWindow:
    def test_none_cutoff_returns_input_unchanged(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("alpha\n")
        entries = scan_roots([tmp_path])
        assert filter_by_window(entries, None) == entries

    def test_drops_stale_files_and_prunes_empty_folders(self, tmp_path: Path) -> None:
        fresh = tmp_path / "fresh.log"
        fresh.write_text("new\n")
        (tmp_path / "stale_dir").mkdir()
        stale = tmp_path / "stale_dir" / "old.log"
        stale.write_text("old\n")
        old_timestamp = fresh.stat().st_mtime - 3600  # 1h ago
        os.utime(stale, (old_timestamp, old_timestamp))
        entries = scan_roots([tmp_path])
        cutoff = fresh.stat().st_mtime - 60  # keep only last minute
        kept = filter_by_window(entries, cutoff)
        kept_paths = {e.relative_path for e in kept}
        assert "fresh.log" in {p for p in kept_paths if p.endswith(".log")}
        assert not any("old.log" in p for p in kept_paths)
        assert not any(
            isinstance(e, FolderEntry) and e.relative_path == "stale_dir" for e in kept
        )

    def test_root_folder_count_recomputed(self, tmp_path: Path) -> None:
        expected_kept = 2
        (tmp_path / "a.log").write_text("new\n")
        (tmp_path / "b.log").write_text("also new\n")
        (tmp_path / "c.log").write_text("old\n")
        fresh_mt = (tmp_path / "a.log").stat().st_mtime
        os.utime(tmp_path / "c.log", (fresh_mt - 7200, fresh_mt - 7200))
        entries = scan_roots([tmp_path])
        cutoff = fresh_mt - 60
        kept = filter_by_window(entries, cutoff)
        root_folder = next(
            e for e in kept if isinstance(e, FolderEntry) and e.depth == 0
        )
        assert root_folder.descendant_file_count == expected_kept


class TestExpandSelection:
    def test_folder_expands_to_descendants(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("2\n")
        (tmp_path / "nested" / "c.log").write_text("3\n")
        entries = scan_roots([tmp_path])
        root_folder = entries[0]
        expanded = expand_selection([root_folder], entries)
        assert len(expanded) == _EXPECTED_OK_COUNT

    def test_file_only_selection_passes_through(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "b.log").write_text("2\n")
        entries = scan_roots([tmp_path])
        one_file = _pick_file(entries, "a.log")
        expanded = expand_selection([one_file], entries)
        assert len(expanded) == 1
        assert expanded[0].relative_path.endswith("a.log")

    def test_folder_plus_overlapping_file_deduped(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "b.log").write_text("2\n")
        entries = scan_roots([tmp_path])
        root_folder = entries[0]
        a_file = _pick_file(entries, "a.log")
        expanded = expand_selection([root_folder, a_file], entries)
        paths = [f.relative_path for f in expanded]
        assert len(paths) == len(set(paths))
