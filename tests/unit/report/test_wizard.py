"""Unit tests for the ami-report interactive wizard.

Drive `wizard.run()` with fully-injected primitives so no real keyboard,
getpass, TUI dialog, or HTTP call is needed. Asserts the flow reaches
`post_bundle` with the correct manifest + secret + token when everything
goes well, and short-circuits cleanly on empty selections / cancels.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ami.dataops.report import wizard
from ami.dataops.report.config import PeerEntry
from ami.dataops.report.defaults import DEFAULT_PEER_NAME
from ami.dataops.report.scanner import (
    CandidateFile,
    FolderEntry,
    TreeEntry,
    scan_roots,
)
from ami.dataops.report.transport import PostContext


class _StubInputs:
    """Bundled stub answers for _build_primitives.

    Plain class (not Pydantic / dataclass) so the captured-dict reference
    passes by identity and the post stub can mutate it — Pydantic would
    deep-copy the dict and break the mutation round-trip.
    """

    def __init__(self, **overrides: object) -> None:
        defaults = {
            "sender_input": "",
            "scope_answers": [],
            "scope_labels": None,
            "select_all_tree": True,
            "pick_peer_name": None,
            "secret_values": {},
            "preview": True,
            "confirm": True,
            "captured": None,
        }
        defaults.update(overrides)
        for key, value in defaults.items():
            object.__setattr__(self, key, value)


def _make_prompt(stub: _StubInputs) -> wizard.Prompter:
    answers_iter = iter(stub.scope_answers or [""])

    def _prompt(question: str, default: str) -> str:
        if "Sender ID" in question:
            return stub.sender_input or default
        return next(answers_iter, "")

    return _prompt


def _make_secret(stub: _StubInputs) -> wizard.SecretPrompter:
    def _secret(question: str) -> str:
        for key, value in stub.secret_values.items():
            if key in question:
                return value
        return "default-secret"

    return _secret


def _make_pick_tree(stub: _StubInputs) -> wizard.PickTreeFn:
    def _pick_tree(entries: list[TreeEntry]) -> list[TreeEntry]:
        if not stub.select_all_tree:
            return []
        for entry in entries:
            if isinstance(entry, FolderEntry) and entry.toggleable:
                return [entry]
        return [e for e in entries if e.toggleable]

    return _pick_tree


def _make_pick_peer(stub: _StubInputs) -> wizard.PickPeerFn:
    def _pick_peer(peers: list[PeerEntry]) -> PeerEntry | None:
        if stub.pick_peer_name is None:
            return None
        for peer in peers:
            if peer.name == stub.pick_peer_name:
                return peer
        return None

    return _pick_peer


def _make_pick_scope(stub: _StubInputs) -> wizard.PickScopeFn:
    def _pick_scope(labels: list[str], _preselected: list[str]) -> list[str] | None:
        return stub.scope_labels if stub.scope_labels is not None else [labels[0]]

    return _pick_scope


def _make_post(stub: _StubInputs) -> wizard.PostBundleFn:
    def _post(ctx: PostContext) -> dict[str, object]:
        if stub.captured is not None:
            stub.captured["ctx"] = ctx
        return {"status": "accept", "bundle_id": ctx.manifest.bundle_id}

    return _post


def _build_primitives(stub: _StubInputs) -> wizard.WizardPrimitives:
    return wizard.WizardPrimitives(
        prompt=_make_prompt(stub),
        secret_prompt=_make_secret(stub),
        pick_scope=_make_pick_scope(stub),
        pick_tree=_make_pick_tree(stub),
        pick_peer=_make_pick_peer(stub),
        preview_archive=lambda _summary: stub.preview,
        confirm=lambda _message: stub.confirm,
        post_bundle=_make_post(stub),
    )


@pytest.fixture
def scratch_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "app.log").write_text("alpha\n")
    (logs / "trace.txt").write_text("trace entry\n")
    monkeypatch.setenv("AMI_ROOT", str(tmp_path))
    return logs


class TestResolveSenderId:
    def test_accepts_default(self) -> None:
        result = wizard._resolve_sender_id(None, lambda _question, default: default)
        assert result != ""

    def test_rejects_invalid_then_accepts(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        answers = iter(["bad name!", "good-name"])
        value = wizard._resolve_sender_id(
            None, lambda _question, _default: next(answers)
        )
        assert value == "good-name"
        assert "must match" in capsys.readouterr().err


class TestEnsurePeerCredentials:
    def test_prompts_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = PeerEntry.model_validate(
            {
                "name": "bravo",
                "endpoint": "https://b.example.com/",
                "shared_secret_env_var": "SECRET_B",
            }
        )
        monkeypatch.delenv("SECRET_B", raising=False)
        monkeypatch.delenv("AMI_REPORT_TOKENS__BRAVO", raising=False)
        answers = iter(["sec-val", "tok-val"])
        secret, token = wizard._ensure_peer_credentials(peer, lambda _q: next(answers))
        assert secret == "sec-val"
        assert token == "tok-val"

    def test_skips_when_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        peer = PeerEntry.model_validate(
            {
                "name": "bravo",
                "endpoint": "https://b.example.com/",
                "shared_secret_env_var": "SECRET_B",
            }
        )
        monkeypatch.setenv("SECRET_B", "env-secret")
        monkeypatch.setenv("AMI_REPORT_TOKENS__BRAVO", "env-token")
        calls: list[str] = []
        secret, token = wizard._ensure_peer_credentials(
            peer, lambda q: calls.append(q) or "never"
        )
        assert calls == []
        assert secret == "env-secret"
        assert token == "env-token"


class TestRunEndToEnd:
    def test_happy_path_reaches_post_bundle(
        self,
        scratch_tree: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AMI_REPORT_SECRET_REPORTS", raising=False)
        monkeypatch.delenv("AMI_REPORT_TOKENS__REPORTS", raising=False)
        captured: dict = {}
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                pick_peer_name=DEFAULT_PEER_NAME,
                secret_values={"Secret for reports": "sec", "Bearer token": "tok"},
                captured=captured,
            )
        )
        exit_code = wizard.run(config_path=None, primitives=primitives)
        assert exit_code == wizard.EXIT_OK
        ctx = captured["ctx"]
        assert isinstance(ctx, PostContext)
        assert ctx.manifest.sender_id == "alpha"
        assert len(ctx.manifest.files) >= 1
        assert ctx.bearer_token == "tok"

    def test_empty_tree_selection_exits_zero(self, scratch_tree: Path) -> None:
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                select_all_tree=False,
                pick_peer_name=DEFAULT_PEER_NAME,
            )
        )
        assert wizard.run(config_path=None, primitives=primitives) == wizard.EXIT_OK

    def test_cancelled_peer_exits_zero(self, scratch_tree: Path) -> None:
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                pick_peer_name=None,
            )
        )
        assert wizard.run(config_path=None, primitives=primitives) == wizard.EXIT_OK

    def test_cancelled_confirm_exits_zero(
        self, scratch_tree: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMI_REPORT_SECRET_REPORTS", "x")
        monkeypatch.setenv("AMI_REPORT_TOKENS__REPORTS", "y")
        primitives = _build_primitives(
            _StubInputs(
                sender_input="alpha",
                scope_answers=[""],
                pick_peer_name=DEFAULT_PEER_NAME,
                confirm=False,
            )
        )
        assert wizard.run(config_path=None, primitives=primitives) == wizard.EXIT_OK


class TestScanRootsIntegration:
    """Quick sanity that scanner input fed to wizard produces folder + files."""

    def test_scan_produces_folder_and_files(self, scratch_tree: Path) -> None:
        entries = scan_roots([scratch_tree])
        assert any(isinstance(e, FolderEntry) for e in entries)
        assert any(e.toggleable for e in entries)


class TestFindScopeCandidates:
    def test_lists_root_with_total_then_direct_dirs(self, tmp_path: Path) -> None:
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("2\n")
        (tmp_path / "nested" / "c.txt").write_text("3\n")
        (tmp_path / "empty_dir").mkdir()
        results = wizard.find_scope_candidates(tmp_path)
        paths = [str(p) for p, _ in results]
        assert paths[0] == str(tmp_path.resolve())
        assert str((tmp_path / "nested").resolve()) in paths
        assert str((tmp_path / "empty_dir").resolve()) not in paths

    def test_counts_match_direct_files(self, tmp_path: Path) -> None:
        expected_total = 3
        expected_nested = 2
        (tmp_path / "a.log").write_text("1\n")
        (tmp_path / "nested").mkdir()
        (tmp_path / "nested" / "b.log").write_text("2\n")
        (tmp_path / "nested" / "c.txt").write_text("3\n")
        results = dict(wizard.find_scope_candidates(tmp_path))
        assert results[tmp_path.resolve()] == expected_total
        assert results[(tmp_path / "nested").resolve()] == expected_nested

    def test_skips_hidden_junk_dirs(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "hook.log").write_text("1\n")
        (tmp_path / ".venv").mkdir()
        (tmp_path / ".venv" / "stub.log").write_text("1\n")
        (tmp_path / "real.log").write_text("1\n")
        results = wizard.find_scope_candidates(tmp_path)
        paths = [p for p, _ in results]
        assert tmp_path.resolve() in paths
        assert (tmp_path / ".git").resolve() not in paths
        assert (tmp_path / ".venv").resolve() not in paths

    def test_ignores_non_log_non_txt(self, tmp_path: Path) -> None:
        (tmp_path / "ok.log").write_text("1\n")
        (tmp_path / "data.json").write_text("{}\n")
        (tmp_path / "doc.md").write_text("# doc\n")
        results = dict(wizard.find_scope_candidates(tmp_path))
        assert results[tmp_path.resolve()] == 1

    def test_empty_workspace_returns_empty_list(self, tmp_path: Path) -> None:
        assert wizard.find_scope_candidates(tmp_path) == []


class TestRenderArchiveSummary:
    def test_shows_sizes_and_count(self) -> None:
        summary = wizard.ArchiveSummary(
            compressed_bytes=1024,
            uncompressed_bytes=4096,
            files=[
                CandidateFile(
                    absolute_path=Path("/tmp/a.log"),
                    relative_path="a.log",
                    size_bytes=2048,
                    preflight="ok",
                )
            ],
        )
        rendered = wizard.render_archive_summary(summary)
        assert "1.0 KiB compressed" in rendered
        assert "4.0 KiB uncompressed" in rendered
        assert "Files:    1" in rendered
        assert "a.log" in rendered

    def test_truncates_over_limit_with_plus_k_more(self) -> None:
        files = [
            CandidateFile(
                absolute_path=Path(f"/tmp/{i}.log"),
                relative_path=f"{i}.log",
                size_bytes=1,
                preflight="ok",
            )
            for i in range(25)
        ]
        summary = wizard.ArchiveSummary(
            compressed_bytes=100, uncompressed_bytes=25, files=files
        )
        rendered = wizard.render_archive_summary(summary)
        assert "(+5 more)" in rendered
