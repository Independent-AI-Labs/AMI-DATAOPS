"""Unit tests for ami.dataops.report.windows."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from ami.dataops.report.models import WindowKey
from ami.dataops.report.scanner import CandidateFile
from ami.dataops.report.windows import (
    VALID_KEYS,
    WINDOW_CATALOG,
    by_key,
    normalize_key,
    tallies_for,
)

_SEVEN = 7
_NOW = 1_700_000_000.0
_FIVE_MIN = 5 * 60
_ONE_DAY = 86400


def _cand(name: str, *, mtime: float, ok: bool = True) -> CandidateFile:
    return CandidateFile(
        absolute_path=Path(f"/tmp/{name}"),
        relative_path=name,
        size_bytes=42,
        preflight="ok" if ok else "ext_not_allowed",
        reject_detail=None if ok else "bad ext",
        mtime_epoch=mtime,
    )


class TestCatalog:
    def test_has_seven_entries(self) -> None:
        assert len(WINDOW_CATALOG) == _SEVEN

    def test_keys_are_unique(self) -> None:
        keys = [w.key for w in WINDOW_CATALOG]
        assert len(keys) == len(set(keys))

    def test_valid_keys_set_mirrors_catalog(self) -> None:
        assert frozenset(w.key for w in WINDOW_CATALOG) == VALID_KEYS


class TestByKey:
    def test_returns_matching_window(self) -> None:
        assert by_key("1h").label == "Last 1 hour"

    def test_all_time_has_no_delta(self) -> None:
        assert by_key("all").delta is None

    def test_raises_for_unknown_key(self) -> None:
        with pytest.raises(KeyError):
            # Literal-narrowed in production; at runtime a str slips through.
            by_key(cast(WindowKey, "42m"))


class TestNormalizeKey:
    def test_none_returns_none(self) -> None:
        assert normalize_key(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert normalize_key("   ") is None

    def test_valid_key_passes_through(self) -> None:
        assert normalize_key("1h") == "1h"

    def test_uppercase_is_normalised(self) -> None:
        assert normalize_key("1H") == "1h"

    def test_unknown_key_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown --since"):
            normalize_key("42m")


class TestTalliesFor:
    def test_counts_per_bucket(self) -> None:
        expected_1m = 2
        expected_5m = 3
        expected_1h = 3
        expected_1d = 3
        entries = [
            _cand("fresh.log", mtime=_NOW - 10),
            _cand("also_fresh.log", mtime=_NOW - 50),
            _cand("mid.log", mtime=_NOW - (_FIVE_MIN - 10)),
            _cand("old.log", mtime=_NOW - _ONE_DAY - 100),
        ]
        counts = {t.window.key: t.file_count for t in tallies_for(entries, _NOW)}
        assert counts["all"] == len(entries)
        assert counts["1m"] == expected_1m
        assert counts["5m"] == expected_5m
        assert counts["1h"] == expected_1h
        assert counts["1d"] == expected_1d

    def test_ignores_rejected_files(self) -> None:
        entries = [
            _cand("good.log", mtime=_NOW),
            _cand("bad.exe", mtime=_NOW, ok=False),
        ]
        counts = {t.window.key: t.file_count for t in tallies_for(entries, _NOW)}
        assert counts["all"] == 1

    def test_returns_tally_per_catalog_entry(self) -> None:
        result = tallies_for([], _NOW)
        assert len(result) == _SEVEN
        assert all(t.file_count == 0 for t in result)
