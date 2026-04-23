"""Unit tests for ami.dataops.report.models."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ami.dataops.report.models import (
    ArchiveSummary,
    CIDefaults,
    PeerCredentials,
    RunRequest,
    Scope,
    ScopeCandidate,
    TimeWindow,
    TimeWindowTally,
)
from ami.dataops.report.scanner import CandidateFile

_NOW = 1_700_000_000.0
_MINUTE_SECONDS = 60
_HOUR_SECONDS = 3600
_TWO = 2
_FOURTEEN = 14
_ONE_HUNDRED = 100


def _candidate(rel: str = "a.log") -> CandidateFile:
    return CandidateFile(
        absolute_path=Path(f"/tmp/{rel}"),
        relative_path=rel,
        size_bytes=42,
        preflight="ok",
        mtime_epoch=_NOW,
    )


class TestScopeCandidate:
    def test_label_formats_path_and_count(self) -> None:
        sc = ScopeCandidate(absolute_path=Path("/var/log"), file_count=7)
        assert sc.label == "/var/log (7)"

    def test_rejects_negative_count(self) -> None:
        with pytest.raises(ValidationError):
            ScopeCandidate(absolute_path=Path("/x"), file_count=-1)

    def test_round_trips_via_json(self) -> None:
        sc = ScopeCandidate(absolute_path=Path("/var/log"), file_count=3)
        revived = ScopeCandidate.model_validate_json(sc.model_dump_json())
        assert revived == sc


class TestScope:
    def test_defaults_to_empty(self) -> None:
        assert Scope().roots == []

    def test_holds_multiple_roots(self) -> None:
        scope = Scope(roots=[Path("/a"), Path("/b")])
        assert len(scope.roots) == _TWO


class TestTimeWindow:
    def test_all_time_cutoff_is_none(self) -> None:
        tw = TimeWindow(key="all", label="All time", delta=None)
        assert tw.cutoff(_NOW) is None

    def test_delta_cutoff_subtracts_seconds(self) -> None:
        tw = TimeWindow(
            key="1h", label="Last 1 hour", delta=timedelta(seconds=_HOUR_SECONDS)
        )
        assert tw.cutoff(_NOW) == pytest.approx(_NOW - _HOUR_SECONDS)

    def test_rejects_unknown_key(self) -> None:
        with pytest.raises(ValidationError):
            TimeWindow.model_validate(
                {"key": "42m", "label": "bogus", "delta": timedelta(minutes=42)}
            )

    def test_is_frozen(self) -> None:
        """Assert the model declares frozen=True so callers can share instances."""
        assert TimeWindow.model_config.get("frozen") is True


class TestTimeWindowTally:
    def test_bundles_window_and_count(self) -> None:
        tw = TimeWindow(key="all", label="All time", delta=None)
        tally = TimeWindowTally(window=tw, file_count=_FOURTEEN)
        assert tally.window.key == "all"
        assert tally.file_count == _FOURTEEN

    def test_rejects_negative_count(self) -> None:
        tw = TimeWindow(key="all", label="All time", delta=None)
        with pytest.raises(ValidationError):
            TimeWindowTally(window=tw, file_count=-1)


class TestPeerCredentials:
    def test_rejects_empty_secret(self) -> None:
        with pytest.raises(ValidationError):
            PeerCredentials(shared_secret="", bearer_token="t")


class TestArchiveSummary:
    def test_counts_and_files(self) -> None:
        summary = ArchiveSummary(
            compressed_bytes=_ONE_HUNDRED,
            uncompressed_bytes=500,
            files=[_candidate()],
        )
        assert summary.compressed_bytes == _ONE_HUNDRED
        assert summary.files[0].relative_path == "a.log"


class TestCIDefaults:
    def test_minimal_shape(self) -> None:
        d = CIDefaults(peer="bravo", files=["a.log"])
        assert d.peer == "bravo"
        assert d.since is None

    def test_rejects_unknown_since_key(self) -> None:
        with pytest.raises(ValidationError):
            CIDefaults.model_validate({"peer": "bravo", "files": [], "since": "42m"})

    def test_rejects_extra_keys(self) -> None:
        with pytest.raises(ValidationError):
            CIDefaults.model_validate(
                {"peer": "bravo", "files": [], "bogus_field": True}
            )


class TestRunRequest:
    def test_defaults_are_all_none(self) -> None:
        req = RunRequest()
        assert req.config_path is None
        assert req.since_key is None
        assert req.dry_run is False

    def test_carries_frozenset_extensions(self) -> None:
        req = RunRequest(extensions=frozenset({".log", ".txt"}))
        assert ".log" in (req.extensions or set())
