"""Unit tests for ami.dataops.report.tui helpers (no keyboard simulation)."""

from __future__ import annotations

from pathlib import Path

from ami.dataops.report.scanner import CandidateFile
from ami.dataops.report.tui import (
    _build_items,
    _extract_selection,
    _size_label,
    resolve_selection_from_defaults,
)


def _candidate(rel: str, *, ok: bool = True) -> CandidateFile:
    return CandidateFile(
        absolute_path=Path(f"/tmp/{rel}"),
        relative_path=rel,
        size_bytes=1024,
        preflight="ok" if ok else "ext_not_allowed",
        reject_detail=None if ok else "bad ext",
    )


class TestSizeLabel:
    def test_kib_for_small(self) -> None:
        assert _size_label(512) == "0.5 KiB"

    def test_mib_for_large(self) -> None:
        assert _size_label(5 * 1024 * 1024) == "5.0 MiB"


class TestBuildItems:
    def test_emits_headers_per_directory(self) -> None:
        cands = [
            _candidate("nested/a.log"),
            _candidate("nested/b.log"),
            _candidate("c.log"),
        ]
        items = _build_items(cands)
        headers = [item for item in items if item.get("is_header")]
        ids = {h["id"] for h in headers}
        assert "_header_nested" in ids
        assert "_header_." in ids

    def test_rejected_candidates_disabled(self) -> None:
        items = _build_items([_candidate("ok.log"), _candidate("bad.exe", ok=False)])
        entries = {item["label"]: item for item in items if not item.get("is_header")}
        assert entries["ok.log"].get("disabled") is False
        assert entries["bad.exe"].get("disabled") is True


class TestExtractSelection:
    def test_returns_matching_candidates_in_dialog_order(self) -> None:
        cands = [_candidate("a.log"), _candidate("b.log"), _candidate("c.log")]
        raw = [
            {"id": "/tmp/c.log"},
            {"id": "/tmp/a.log"},
        ]
        result = _extract_selection(raw, cands)
        assert [c.relative_path for c in result] == ["c.log", "a.log"]

    def test_ignores_non_list_input(self) -> None:
        assert _extract_selection(None, [_candidate("a.log")]) == []

    def test_skips_unknown_ids(self) -> None:
        cands = [_candidate("a.log")]
        raw = [{"id": "/tmp/nope.log"}]
        assert _extract_selection(raw, cands) == []


class TestResolveFromDefaults:
    def test_matches_by_relative_path(self) -> None:
        cands = [_candidate("a.log"), _candidate("b.log"), _candidate("c.log")]
        defaults = {"files": ["a.log", "c.log"]}
        result = resolve_selection_from_defaults(defaults, cands)
        assert sorted(c.relative_path for c in result) == ["a.log", "c.log"]

    def test_empty_defaults_returns_empty(self) -> None:
        cands = [_candidate("a.log")]
        assert resolve_selection_from_defaults({}, cands) == []

    def test_disallowed_candidates_not_selected(self) -> None:
        cands = [_candidate("a.log", ok=False)]
        defaults = {"files": ["a.log"]}
        assert resolve_selection_from_defaults(defaults, cands) == []
