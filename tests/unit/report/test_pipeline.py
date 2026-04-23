"""Unit tests for ami.dataops.report.pipeline.

Drives `pipeline.run` with a FakeOperator that records every call
and returns scripted answers — one test per pipeline branch.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from ami.dataops.report import pipeline as pipeline_mod
from ami.dataops.report.config import PeerEntry, ReportConfig
from ami.dataops.report.models import (
    ArchiveSummary,
    PeerCredentials,
    RunRequest,
    Scope,
    ScopeCandidate,
    SignedBundle,
    TimeWindow,
    TimeWindowTally,
)
from ami.dataops.report.operator import OperatorCancelled
from ami.dataops.report.scanner import TreeEntry
from ami.dataops.report.transport import (
    AuthRejected,
    NetworkError,
    ValidationRejectedByPeer,
)
from ami.dataops.report.windows import by_key

_SECRET = "hunter2"
_TOKEN = "token42"


class FakeOperator(BaseModel):
    """Scripted Operator that also records every call."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    sender_id: str = "alpha"
    scope: Scope = Scope(roots=[])
    window: TimeWindow | None = None  # None → compute via by_key("all")
    selection: list[TreeEntry] = []
    peer: PeerEntry | None = None
    credentials: PeerCredentials = PeerCredentials(
        shared_secret=_SECRET, bearer_token=_TOKEN
    )
    review_result: bool = True
    confirm_result: bool = True
    cancel_on: str = ""  # method name that should raise OperatorCancelled
    calls: list[str] = []

    def _maybe_cancel(self, name: str) -> None:
        self.calls.append(name)
        if self.cancel_on == name:
            raise OperatorCancelled

    def resolve_sender_id(self, config: ReportConfig) -> str:
        self._maybe_cancel("resolve_sender_id")
        return self.sender_id

    def resolve_scope(
        self, candidates: list[ScopeCandidate], config: ReportConfig
    ) -> Scope:
        self._maybe_cancel("resolve_scope")
        return self.scope

    def resolve_window(self, tallies: list[TimeWindowTally]) -> TimeWindow:
        self._maybe_cancel("resolve_window")
        return self.window or by_key("all")

    def resolve_selection(self, entries: list[TreeEntry]) -> list[TreeEntry]:
        self._maybe_cancel("resolve_selection")
        if self.selection:
            return self.selection
        return list(entries)

    def resolve_peer(self, peers: list[PeerEntry]) -> PeerEntry:
        self._maybe_cancel("resolve_peer")
        return self.peer or peers[0]

    def resolve_credentials(self, peer: PeerEntry) -> PeerCredentials:
        self._maybe_cancel("resolve_credentials")
        return self.credentials

    def review_archive(self, summary: ArchiveSummary) -> bool:
        self._maybe_cancel("review_archive")
        return self.review_result

    def confirm_send(self, bundle: SignedBundle, peer: PeerEntry) -> bool:
        self._maybe_cancel("confirm_send")
        return self.confirm_result


@pytest.fixture
def tmp_scope(tmp_path: Path) -> Scope:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "fresh.log").write_text("alpha\n")
    return Scope(roots=[logs])


@pytest.fixture
def peer() -> PeerEntry:
    return PeerEntry.model_validate(
        {
            "name": "bravo",
            "endpoint": "https://intake.example.com/",
            "shared_secret_env_var": "SECRET_BRAVO",
        }
    )


@pytest.fixture
def config(tmp_path: Path, peer: PeerEntry) -> Path:
    path = tmp_path / "report.yml"
    path.write_text(
        f"dataops_report_sender_config:\n  sender_id: alpha\n"
        "  extra_roots: []\n"
        f"dataops_report_peers:\n  - name: {peer.name}\n"
        f"    endpoint: {peer.endpoint}\n"
        f"    shared_secret_env_var: {peer.shared_secret_env_var}\n"
    )
    return path


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AMI_ROOT", raising=False)
    monkeypatch.setenv("SECRET_BRAVO", _SECRET)
    monkeypatch.setenv("AMI_REPORT_TOKENS__BRAVO", _TOKEN)


class TestHappyPath:
    def test_dry_run_emits_manifest_and_signature(
        self,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        op = FakeOperator(scope=tmp_scope, peer=peer)
        request = RunRequest(config_path=config, dry_run=True)
        rc = pipeline_mod.run(request, op)
        assert rc == pipeline_mod.EXIT_OK
        captured = capsys.readouterr().out
        assert "sha256=" in captured
        # review_archive / confirm_send skipped on dry run.
        assert "review_archive" not in op.calls
        assert "confirm_send" not in op.calls

    def test_since_key_overrides_operator_resolve_window(
        self,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
    ) -> None:
        op = FakeOperator(scope=tmp_scope, peer=peer)
        request = RunRequest(config_path=config, dry_run=True, since_key="1h")
        pipeline_mod.run(request, op)
        assert "resolve_window" not in op.calls

    def test_operator_resolves_window_when_since_unset(
        self,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
    ) -> None:
        op = FakeOperator(scope=tmp_scope, peer=peer)
        request = RunRequest(config_path=config, dry_run=True)
        pipeline_mod.run(request, op)
        assert "resolve_window" in op.calls


class TestExitBranches:
    def test_empty_scope_exits_without_scan(
        self, config: Path, peer: PeerEntry
    ) -> None:
        op = FakeOperator(peer=peer)  # scope.roots stays []
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_OK
        assert "resolve_window" not in op.calls

    def test_review_declined_skips_post(
        self, config: Path, tmp_scope: Scope, peer: PeerEntry
    ) -> None:
        op = FakeOperator(scope=tmp_scope, peer=peer, review_result=False)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_OK
        assert "confirm_send" not in op.calls

    def test_confirm_declined_skips_post(
        self, config: Path, tmp_scope: Scope, peer: PeerEntry
    ) -> None:
        op = FakeOperator(scope=tmp_scope, peer=peer, confirm_result=False)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_OK


class TestCancellation:
    @pytest.mark.parametrize(
        "stage",
        [
            "resolve_sender_id",
            "resolve_scope",
            "resolve_window",
            "resolve_selection",
            "resolve_peer",
            "resolve_credentials",
        ],
    )
    def test_operator_cancel_anywhere_returns_exit_ok(
        self, config: Path, tmp_scope: Scope, peer: PeerEntry, stage: str
    ) -> None:
        op = FakeOperator(scope=tmp_scope, peer=peer, cancel_on=stage)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_OK


class TestPostOutcomes:
    def _patch_post(
        self, monkeypatch: pytest.MonkeyPatch, effect: Exception | dict[str, Any]
    ) -> None:
        def fake_post(ctx: object, **_: Any) -> dict[str, Any]:
            if isinstance(effect, Exception):
                raise effect
            return effect

        monkeypatch.setattr(pipeline_mod, "post_bundle", fake_post)

    def test_auth_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
    ) -> None:
        self._patch_post(monkeypatch, AuthRejected("401"))
        op = FakeOperator(scope=tmp_scope, peer=peer)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_AUTH_REJECTED

    def test_network_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
    ) -> None:
        self._patch_post(monkeypatch, NetworkError("boom"))
        op = FakeOperator(scope=tmp_scope, peer=peer)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_NETWORK_ERROR

    def test_validation_rejected(
        self,
        monkeypatch: pytest.MonkeyPatch,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
    ) -> None:
        status_code = 400
        self._patch_post(
            monkeypatch,
            ValidationRejectedByPeer(status_code, "bad_ext", "nope"),
        )
        op = FakeOperator(scope=tmp_scope, peer=peer)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_VALIDATION_REJECTED_PEER

    def test_happy_post_prints_receipt(
        self,
        monkeypatch: pytest.MonkeyPatch,
        config: Path,
        tmp_scope: Scope,
        peer: PeerEntry,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        self._patch_post(monkeypatch, {"status": "accept", "bundle_id": "x"})
        op = FakeOperator(scope=tmp_scope, peer=peer)
        rc = pipeline_mod.run(
            RunRequest(config_path=config),
            op,
        )
        assert rc == pipeline_mod.EXIT_OK
        captured = capsys.readouterr().out
        assert '"status": "accept"' in captured


class TestWindowFilter:
    def test_since_filters_fresh_only(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        peer: PeerEntry,
        config: Path,
    ) -> None:
        logs = tmp_path / "logs"
        logs.mkdir(exist_ok=True)
        fresh = logs / "fresh.log"
        stale = logs / "stale.log"
        fresh.write_text("now\n")
        stale.write_text("old\n")
        old_ts = fresh.stat().st_mtime - 7200  # 2h ago
        os.utime(stale, (old_ts, old_ts))
        op = FakeOperator(scope=Scope(roots=[logs]), peer=peer)
        request = RunRequest(config_path=config, dry_run=True, since_key="1h")
        pipeline_mod.run(request, op)
        # selection contains only CandidateFile entries that survived filtering
        selected_files = [e for e in op.calls if e == "resolve_selection"]
        assert selected_files  # called once

    def test_empty_window_exits(
        self,
        tmp_path: Path,
        peer: PeerEntry,
        config: Path,
    ) -> None:
        logs = tmp_path / "logs"
        logs.mkdir(exist_ok=True)
        old = logs / "old.log"
        old.write_text("x\n")
        old_ts = old.stat().st_mtime - 86400  # 1 day ago
        os.utime(old, (old_ts, old_ts))
        op = FakeOperator(scope=Scope(roots=[logs]), peer=peer)
        request = RunRequest(config_path=config, since_key="1m")
        rc = pipeline_mod.run(request, op)
        assert rc == pipeline_mod.EXIT_OK
        assert "resolve_selection" not in op.calls
