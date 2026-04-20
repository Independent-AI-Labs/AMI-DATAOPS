"""Unit tests for ami.dataops.report.scanner."""

from __future__ import annotations

from pathlib import Path

from ami.dataops.report.scanner import (
    CandidateFile,
    FolderEntry,
    expand_selection,
    files_only,
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
        (tmp_path / "nested" / "c.txt").write_text("trace\n")
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

    def test_disallowed_extension_marked_reject(self, tmp_path: Path) -> None:
        (tmp_path / "ok.log").write_text("ok\n")
        (tmp_path / "bad.exe").write_bytes(b"MZ")
        entries = scan_roots([tmp_path])
        files = {f.relative_path.rsplit("/", 1)[-1]: f for f in files_only(entries)}
        assert files["ok.log"].preflight == "ok"
        assert files["bad.exe"].preflight == "ext_not_allowed"
        assert not files["bad.exe"].toggleable

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
