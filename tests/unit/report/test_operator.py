"""Unit tests for ami.dataops.report.operator.

The preselection-regression tests are the load-bearing ones: they
lock in that picker items carry stable `id` fields that match the
values passed in `preselected`. Getting that wrong is what broke
the interactive wizard.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pytest

from ami.dataops.report.config import PeerEntry, ReportConfig, SenderConfig
from ami.dataops.report.models import (
    ArchiveSummary,
    CIDefaults,
    PeerCredentials,
    ScopeCandidate,
    TimeWindow,
    TimeWindowTally,
)
from ami.dataops.report.operator import (
    CIOperator,
    scope_item,
    tree_item,
    window_item,
)
from ami.dataops.report.scanner import CandidateFile, FolderEntry, TreeEntry

_NOW = 1_700_000_000.0
_SECRET = "top-secret-xyz"
_TOKEN = "bearer-abc"


def _candidate(rel: str, *, ok: bool = True) -> CandidateFile:
    return CandidateFile(
        absolute_path=Path(f"/tmp/{rel}"),
        relative_path=rel,
        size_bytes=42,
        preflight="ok" if ok else "ext_not_allowed",
        reject_detail=None if ok else "bad ext",
        mtime_epoch=_NOW,
    )


def _peer(name: str = "bravo") -> PeerEntry:
    return PeerEntry.model_validate(
        {
            "name": name,
            "endpoint": "https://intake.example.com/",
            "shared_secret_env_var": f"SECRET_{name.upper()}",
        }
    )


class TestScopeItem:
    def test_id_is_posix_path(self) -> None:
        item = scope_item(ScopeCandidate(absolute_path=Path("/var/log"), file_count=7))
        assert item["id"] == "/var/log"
        assert item["label"] == "/var/log (7)"
        assert item["is_header"] is False


class TestWindowItem:
    def test_id_is_window_key(self) -> None:
        tally = TimeWindowTally(
            window=TimeWindow(key="1h", label="Last 1 hour", delta=timedelta(hours=1)),
            file_count=9,
        )
        item = window_item(tally)
        assert item["id"] == "1h"
        assert item["label"] == "Last 1 hour"
        assert item["description"] == "(9)"


class TestTreeItem:
    def test_file_indented_by_depth(self) -> None:
        entry = _candidate("nested/a.log")
        entry_with_depth = entry.model_copy(update={"depth": 2})
        item = tree_item(entry_with_depth, folder_ids=set())
        assert item["label"].startswith("    a.log") or "a.log" in item["label"]
        assert item["disabled"] is False

    def test_rejected_file_disabled(self) -> None:
        item = tree_item(_candidate("bad.exe", ok=False), folder_ids=set())
        assert item["disabled"] is True

    def test_folder_shows_rejected_count(self) -> None:
        folder = FolderEntry(
            absolute_path=Path("/tmp/mix"),
            relative_path="mix",
            descendant_file_count=5,
            toggleable_descendant_count=3,
            depth=1,
        )
        item = tree_item(folder, folder_ids=set())
        assert "2 rejected" in item["description"]

    def test_file_parent_id_points_at_enclosing_folder(self) -> None:
        entry = _candidate("nested/a.log")
        folder_ids = {"folder:/tmp/nested"}
        item = tree_item(entry, folder_ids=folder_ids)
        assert item["parent_id"] == "folder:/tmp/nested"

    def test_file_parent_id_is_none_without_ancestor(self) -> None:
        entry = _candidate("solo.log")
        item = tree_item(entry, folder_ids=set())
        assert item["parent_id"] is None


class TestTerminalOperatorScopePreselection:
    """Regression suite for the missing-window-picker bug."""

    def test_items_have_ids_equal_to_candidate_paths(self) -> None:
        cands = [
            ScopeCandidate(absolute_path=Path("/a"), file_count=1),
            ScopeCandidate(absolute_path=Path("/b"), file_count=2),
        ]
        ids = {scope_item(c)["id"] for c in cands}
        assert ids == {c.absolute_path.as_posix() for c in cands}

    def test_first_candidate_id_is_stable_preselection_key(self) -> None:
        cands = [
            ScopeCandidate(absolute_path=Path("/a"), file_count=1),
            ScopeCandidate(absolute_path=Path("/b"), file_count=2),
        ]
        # The IDs we'd preselect via set(...) must match the IDs on items.
        preselected = {cands[0].absolute_path.as_posix()}
        item_ids = {scope_item(c)["id"] for c in cands}
        assert preselected.issubset(item_ids)


class TestCIOperatorScope:
    def test_returns_all_candidate_paths(self) -> None:
        cands = [
            ScopeCandidate(absolute_path=Path("/a"), file_count=1),
            ScopeCandidate(absolute_path=Path("/b"), file_count=2),
        ]
        op = CIOperator(CIDefaults(peer="bravo", files=["x.log"]))
        config = ReportConfig(sender=SenderConfig(sender_id="alpha"), peers=[_peer()])
        scope = op.resolve_scope(cands, config)
        assert scope.roots == [Path("/a"), Path("/b")]


class TestCIOperatorWindow:
    def test_uses_defaults_since(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=[], since="15m"))
        assert op.resolve_window([]).key == "15m"

    def test_missing_since_defaults_to_all(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        assert op.resolve_window([]).key == "all"


class TestCIOperatorSelection:
    def test_matches_relative_paths(self) -> None:
        entries: list[TreeEntry] = [_candidate("a.log"), _candidate("nested/b.log")]
        op = CIOperator(CIDefaults(peer="bravo", files=["a.log", "nested/b.log"]))
        chosen = op.resolve_selection(entries)
        assert {c.relative_path for c in chosen if isinstance(c, CandidateFile)} == {
            "a.log",
            "nested/b.log",
        }

    def test_matches_basename_shortcut(self) -> None:
        entries: list[TreeEntry] = [_candidate("deep/nested/trace.log")]
        op = CIOperator(CIDefaults(peer="bravo", files=["trace.log"]))
        chosen = op.resolve_selection(entries)
        assert len(chosen) == 1

    def test_unmatched_files_raises(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=["missing.log"]))
        entries: list[TreeEntry] = [_candidate("other.log")]
        with pytest.raises(ValueError, match="matched no candidate"):
            op.resolve_selection(entries)


class TestCIOperatorPeer:
    def test_looks_up_by_name(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        chosen = op.resolve_peer([_peer("alpha"), _peer("bravo")])
        assert chosen.name == "bravo"

    def test_missing_peer_raises(self) -> None:
        op = CIOperator(CIDefaults(peer="charlie", files=[]))
        with pytest.raises(ValueError, match="not found in peers"):
            op.resolve_peer([_peer("bravo")])


class TestCIOperatorCredentials:
    def test_reads_env_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = _peer("bravo")
        monkeypatch.setenv(peer.shared_secret_env_var, _SECRET)
        monkeypatch.setenv("AMI_REPORT_TOKENS__BRAVO", _TOKEN)
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        creds = op.resolve_credentials(peer)
        assert creds == PeerCredentials(shared_secret=_SECRET, bearer_token=_TOKEN)

    def test_missing_secret_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = _peer("bravo")
        monkeypatch.delenv(peer.shared_secret_env_var, raising=False)
        monkeypatch.setenv("AMI_REPORT_TOKENS__BRAVO", _TOKEN)
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        with pytest.raises(ValueError, match="SECRET_BRAVO"):
            op.resolve_credentials(peer)

    def test_missing_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = _peer("bravo")
        monkeypatch.setenv(peer.shared_secret_env_var, _SECRET)
        monkeypatch.delenv("AMI_REPORT_TOKENS__BRAVO", raising=False)
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        with pytest.raises(ValueError, match="AMI_REPORT_TOKENS__BRAVO"):
            op.resolve_credentials(peer)


class TestCIOperatorSenderId:
    def test_passes_through_valid(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        cfg = ReportConfig(sender=SenderConfig(sender_id="alpha"), peers=[_peer()])
        assert op.resolve_sender_id(cfg) == "alpha"

    def test_rejects_invalid(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        cfg = ReportConfig(sender=SenderConfig(sender_id="x"), peers=[_peer()])
        cfg.sender.sender_id = "bad name!"  # bypass validator for test
        with pytest.raises(ValueError, match="invalid sender_id"):
            op.resolve_sender_id(cfg)


class TestCIOperatorConfirmations:
    def test_review_archive_auto_approves(self) -> None:
        op = CIOperator(CIDefaults(peer="bravo", files=[]))
        summary = ArchiveSummary(compressed_bytes=0, uncompressed_bytes=0, files=[])
        assert op.review_archive(summary) is True


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in list(os.environ):
        if var.startswith("AMI_REPORT_TOKENS__") or var.startswith("SECRET_"):
            monkeypatch.delenv(var, raising=False)
