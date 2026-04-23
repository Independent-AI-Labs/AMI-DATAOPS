"""Operator adapter: one Protocol with two implementations.

`Operator` is the single interface the pipeline uses to resolve every
interactive or non-interactive input. `TerminalOperator` drives
`ami.cli_components.dialogs` for humans; `CIOperator` answers from a
typed `CIDefaults` model for scripted runs. The pipeline never imports
dialogs or YAML — everything goes through this adapter.

Picker items built here always carry stable `id` fields so
`dialogs.multiselect(..., preselected=...)` actually works; missing IDs
caused the earlier "Enter on an empty-looking picker skips the next
step with no message" regression.
"""

from __future__ import annotations

import getpass
import os
import re
import socket
import sys
from pathlib import Path
from typing import Protocol, runtime_checkable

from ami.cli_components import dialogs
from ami.cli_components.selection_dialog import (
    SelectableItemDict,
    SelectionDialog,
    SelectionDialogConfig,
)
from ami.dataops.report.archive import render_archive_summary
from ami.dataops.report.config import PeerEntry, ReportConfig
from ami.dataops.report.models import (
    ArchiveSummary,
    CIDefaults,
    PeerCredentials,
    Scope,
    ScopeCandidate,
    SignedBundle,
    TimeWindow,
    TimeWindowTally,
)
from ami.dataops.report.scanner import CandidateFile, FolderEntry, TreeEntry
from ami.dataops.report.windows import by_key

SENDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
INDENT_STEP = "  "
BYTES_PER_KIB = 1024.0
KIB_PER_MIB = 1024.0


class OperatorCancelled(Exception):
    """Raised by any Operator method when the user aborts a step (Esc)."""


@runtime_checkable
class Operator(Protocol):
    """The single adapter interface the pipeline drives."""

    def resolve_sender_id(self, config: ReportConfig) -> str: ...
    def resolve_scope(
        self, candidates: list[ScopeCandidate], config: ReportConfig
    ) -> Scope: ...
    def resolve_window(self, tallies: list[TimeWindowTally]) -> TimeWindow: ...
    def resolve_selection(self, entries: list[TreeEntry]) -> list[TreeEntry]: ...
    def resolve_peer(self, peers: list[PeerEntry]) -> PeerEntry: ...
    def resolve_credentials(self, peer: PeerEntry) -> PeerCredentials: ...
    def review_archive(self, summary: ArchiveSummary) -> bool: ...
    def confirm_send(self, bundle: SignedBundle, peer: PeerEntry) -> bool: ...


# ----- item builders (shared, pure) ------------------------------------------


def scope_item(candidate: ScopeCandidate) -> SelectableItemDict:
    """Selectable row for a scope candidate. `id` = absolute path as POSIX."""
    return {
        "id": candidate.absolute_path.as_posix(),
        "label": candidate.label,
        "value": candidate.absolute_path.as_posix(),
        "is_header": False,
    }


def window_item(tally: TimeWindowTally) -> SelectableItemDict:
    """Selectable row for a window bucket. `id` = the window key."""
    return {
        "id": tally.window.key,
        "label": tally.window.label,
        "description": f"({tally.file_count})",
        "value": tally.window.key,
        "is_header": False,
    }


def tree_entry_id(entry: TreeEntry) -> str:
    prefix = "folder:" if isinstance(entry, FolderEntry) else "file:"
    return f"{prefix}{entry.absolute_path.as_posix()}"


def _folder_parent_id(entry: TreeEntry, folder_ids: set[str]) -> str | None:
    """Return the id of the nearest folder ancestor also present in the tree."""
    parent_path = entry.absolute_path.parent
    while True:
        candidate = f"folder:{parent_path.as_posix()}"
        if candidate in folder_ids and candidate != tree_entry_id(entry):
            return candidate
        if parent_path == parent_path.parent:
            return None
        parent_path = parent_path.parent


def tree_item(entry: TreeEntry, folder_ids: set[str]) -> SelectableItemDict:
    """Selectable row for a TreeEntry. Disabled when pre-flight failed."""
    if isinstance(entry, FolderEntry):
        label = f"{INDENT_STEP * entry.depth}[dir] {entry.relative_path}/"
        detail = f"{entry.descendant_file_count} files"
        if entry.descendant_file_count != entry.toggleable_descendant_count:
            rejected = entry.descendant_file_count - entry.toggleable_descendant_count
            detail += f" ({rejected} rejected by pre-flight)"
    else:
        leaf = entry.relative_path.rsplit("/", 1)[-1]
        label = f"{INDENT_STEP * entry.depth}{leaf}"
        detail = _size_label(entry.size_bytes)
        if entry.preflight != "ok":
            detail = f"{detail} -- {entry.preflight}"
    return {
        "id": tree_entry_id(entry),
        "label": label,
        "description": detail,
        "value": tree_entry_id(entry),
        "is_header": False,
        "disabled": not entry.toggleable,
        "parent_id": _folder_parent_id(entry, folder_ids),
    }


def _size_label(size_bytes: int) -> str:
    kib = size_bytes / BYTES_PER_KIB
    if kib < KIB_PER_MIB:
        return f"{kib:.1f} KiB"
    return f"{kib / KIB_PER_MIB:.1f} MiB"


# ----- TerminalOperator ------------------------------------------------------


class TerminalOperator:
    """Interactive implementation backed by ami.cli_components.dialogs."""

    def resolve_sender_id(self, config: ReportConfig) -> str:
        default = config.sender.sender_id or socket.gethostname() or "anonymous"
        while True:
            raw = input(f"Sender ID [{default}]: ").strip()
            value = raw or default
            if SENDER_ID_PATTERN.match(value):
                return value
            print(
                "error: sender_id must match ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
                file=sys.stderr,
            )

    def resolve_scope(
        self, candidates: list[ScopeCandidate], config: ReportConfig
    ) -> Scope:
        by_id = {c.absolute_path.as_posix(): c for c in candidates}
        picked_ids: set[str] = set()
        if candidates:
            items = [scope_item(c) for c in candidates]
            preselected = {candidates[0].absolute_path.as_posix()}
            raw = dialogs.multiselect(
                list(items),
                title="Scope: which roots to scan (Space toggles, Enter confirms)",
                preselected=preselected,
            )
            picked_ids = _ids_from_result(raw)
        extra_paths = _prompt_extra_paths(list(config.sender.extra_roots))
        roots = [by_id[i].absolute_path for i in picked_ids if i in by_id]
        roots.extend(extra_paths)
        return Scope(roots=roots)

    def resolve_window(self, tallies: list[TimeWindowTally]) -> TimeWindow:
        items = [window_item(t) for t in tallies]
        by_id: dict[str, TimeWindow] = {t.window.key: t.window for t in tallies}
        chosen = dialogs.select(
            list(items),
            title="Time window: show logs modified since",
        )
        if chosen is None:
            raise OperatorCancelled
        chosen_id = _id_from_single(chosen)
        if chosen_id is None or chosen_id not in by_id:
            raise OperatorCancelled
        return by_id[chosen_id]

    def resolve_selection(self, entries: list[TreeEntry]) -> list[TreeEntry]:
        if not entries:
            return []
        folder_ids = {tree_entry_id(e) for e in entries if isinstance(e, FolderEntry)}
        items = [tree_item(e, folder_ids) for e in entries]
        dialog = SelectionDialog(
            items=list(items),
            config=SelectionDialogConfig(
                title="Select files + folders for the report",
                multi=True,
                width=100,
                max_height=20,
            ),
        )
        raw = dialog.run()
        if raw is None:
            raise OperatorCancelled
        picked_ids = _ids_from_result(raw)
        by_id = {tree_entry_id(entry): entry for entry in entries if entry.toggleable}
        return [by_id[i] for i in picked_ids if i in by_id]

    def resolve_peer(self, peers: list[PeerEntry]) -> PeerEntry:
        if not peers:
            msg = "no peers configured; nothing to send to"
            raise ValueError(msg)
        if len(peers) == 1:
            return peers[0]
        labels = [f"{p.name} -- {p.endpoint}" for p in peers]
        selection = dialogs.select(labels, title="Choose destination peer")
        if selection is None:
            raise OperatorCancelled
        for peer in peers:
            if f"{peer.name} -- {peer.endpoint}" == selection:
                return peer
        raise OperatorCancelled

    def resolve_credentials(self, peer: PeerEntry) -> PeerCredentials:
        secret = os.environ.get(peer.shared_secret_env_var) or getpass.getpass(
            f"Secret for {peer.name} ({peer.shared_secret_env_var}): "
        )
        token_env = f"AMI_REPORT_TOKENS__{peer.name.upper()}"
        token = os.environ.get(token_env) or getpass.getpass(
            f"Bearer token for {peer.name} ({token_env}): "
        )
        os.environ[peer.shared_secret_env_var] = secret
        os.environ[token_env] = token
        return PeerCredentials(shared_secret=secret, bearer_token=token)

    def review_archive(self, summary: ArchiveSummary) -> bool:
        return bool(
            dialogs.confirm(render_archive_summary(summary), title="Archive preview")
        )

    def confirm_send(self, bundle: SignedBundle, peer: PeerEntry) -> bool:
        body = (
            f"Destination: {peer.name} ({peer.endpoint})\n"
            f"Bundle id:   {bundle.manifest.bundle_id}\n"
            f"Files:       {len(bundle.archive_summary.files)}\n"
        )
        return bool(dialogs.confirm(body, title="Send report?"))


def _ids_from_result(raw: object) -> set[str]:
    """Extract the set of item IDs from a dialog's raw return value."""
    if not isinstance(raw, list):
        return set()
    picked: set[str] = set()
    for item in raw:
        ident = _id_from_single(item)
        if ident is not None:
            picked.add(ident)
    return picked


def _id_from_single(item: object) -> str | None:
    if isinstance(item, dict):
        value = item.get("id")
        return value if isinstance(value, str) else None
    ident = getattr(item, "id", None)
    return ident if isinstance(ident, str) else None


def _prompt_extra_paths(existing: list[Path]) -> list[Path]:
    result = list(existing)
    while True:
        raw = input("Add custom path (blank to finish) []: ").strip()
        if not raw:
            return result
        extra = Path(raw).expanduser().absolute()
        if not extra.exists():
            print(f"warning: {extra} does not exist; skipped", file=sys.stderr)
            continue
        if extra not in result:
            result.append(extra)


# ----- CIOperator ------------------------------------------------------------


class CIOperator:
    """Non-interactive implementation driven by a typed CIDefaults model."""

    def __init__(self, defaults: CIDefaults) -> None:
        self._defaults = defaults

    def resolve_sender_id(self, config: ReportConfig) -> str:
        value = config.sender.sender_id
        if not SENDER_ID_PATTERN.match(value):
            msg = f"invalid sender_id {value!r} in config"
            raise ValueError(msg)
        return value

    def resolve_scope(
        self, candidates: list[ScopeCandidate], config: ReportConfig
    ) -> Scope:
        roots = [c.absolute_path for c in candidates]
        for extra in config.sender.extra_roots:
            if extra not in roots:
                roots.append(extra)
        return Scope(roots=roots)

    def resolve_window(self, tallies: list[TimeWindowTally]) -> TimeWindow:
        key = self._defaults.since or "all"
        return by_key(key)

    def resolve_selection(self, entries: list[TreeEntry]) -> list[TreeEntry]:
        wanted = set(self._defaults.files)
        matches: list[TreeEntry] = []
        for candidate in entries:
            if not candidate.toggleable or not isinstance(candidate, CandidateFile):
                continue
            if candidate.relative_path in wanted:
                matches.append(candidate)
                continue
            short = candidate.relative_path.rsplit("/", 1)[-1]
            if short in wanted:
                matches.append(candidate)
        if not matches:
            msg = (
                f"defaults.files={sorted(wanted)!r} matched no candidate files in "
                f"{len(entries)} scanned entries"
            )
            raise ValueError(msg)
        return matches

    def resolve_peer(self, peers: list[PeerEntry]) -> PeerEntry:
        for peer in peers:
            if peer.name == self._defaults.peer:
                return peer
        msg = (
            f"defaults.peer={self._defaults.peer!r} not found in peers "
            f"{[p.name for p in peers]!r}"
        )
        raise ValueError(msg)

    def resolve_credentials(self, peer: PeerEntry) -> PeerCredentials:
        secret = os.environ.get(peer.shared_secret_env_var)
        token_env = f"AMI_REPORT_TOKENS__{peer.name.upper()}"
        token = os.environ.get(token_env)
        if not secret:
            msg = f"required env var {peer.shared_secret_env_var} is not set"
            raise ValueError(msg)
        if not token:
            msg = f"required env var {token_env} is not set"
            raise ValueError(msg)
        return PeerCredentials(shared_secret=secret, bearer_token=token)

    def review_archive(self, summary: ArchiveSummary) -> bool:
        return True

    def confirm_send(self, bundle: SignedBundle, peer: PeerEntry) -> bool:
        return True
