"""Interactive TUI for ami-report: pick files, pick peer, confirm.

Reuses the shared SelectionDialog + dialogs facade from ami-agents core;
introduces no new TUI primitives. Three screens in order:

  1. File multi-select   -- SelectionDialog with group-per-directory
  2. Peer single-select  -- dialogs.select over dataops_report_peers
  3. Confirmation        -- dialogs.confirm showing manifest summary
"""

from __future__ import annotations

from typing import cast

from pydantic import BaseModel, ConfigDict

from ami.cli_components import dialogs
from ami.cli_components.selection_dialog import (
    DialogItem,
    SelectionDialog,
    SelectionDialogConfig,
)
from ami.dataops.report.config import PeerEntry, ReportConfig
from ami.dataops.report.scanner import CandidateFile, group_by_directory


class TUIResult(BaseModel):
    """Outcome of the interactive flow: selected files + destination peer."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    selected: list[CandidateFile]
    peer: PeerEntry


BYTES_PER_KIB = 1024.0
KIB_PER_MIB = 1024.0


def _size_label(size_bytes: int) -> str:
    kib = size_bytes / BYTES_PER_KIB
    if kib < KIB_PER_MIB:
        return f"{kib:.1f} KiB"
    return f"{kib / KIB_PER_MIB:.1f} MiB"


def _build_items(
    candidates: list[CandidateFile],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    groups = group_by_directory(candidates)
    for directory, files in groups.items():
        header_label = f"[dir] {directory}" if directory != "." else "[dir] /"
        items.append(
            {
                "id": f"_header_{directory}",
                "label": header_label,
                "value": directory,
                "is_header": True,
            }
        )
        for candidate in files:
            detail = _size_label(candidate.size_bytes)
            if candidate.preflight != "ok":
                detail = f"{detail} -- {candidate.preflight}"
            items.append(
                {
                    "id": candidate.absolute_path.as_posix(),
                    "label": candidate.relative_path,
                    "description": detail,
                    "value": candidate,
                    "is_header": False,
                    "disabled": not candidate.toggleable,
                }
            )
    return items


def _extract_selection(
    raw: object, candidates: list[CandidateFile]
) -> list[CandidateFile]:
    if not isinstance(raw, list):
        return []
    by_path: dict[str, CandidateFile] = {
        c.absolute_path.as_posix(): c for c in candidates if c.toggleable
    }
    chosen: list[CandidateFile] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        identifier = item.get("id")
        if isinstance(identifier, str) and identifier in by_path:
            chosen.append(by_path[identifier])
    return chosen


def pick_files(candidates: list[CandidateFile]) -> list[CandidateFile]:
    """Render the multi-select file tree and return the operator's choices."""
    if not candidates:
        return []
    items = _build_items(candidates)
    dialog = SelectionDialog(
        items=cast("list[DialogItem]", items),
        config=SelectionDialogConfig(
            title="Select files to report",
            multi=True,
            width=100,
            max_height=18,
        ),
    )
    return _extract_selection(dialog.run(), candidates)


def pick_peer(peers: list[PeerEntry]) -> PeerEntry | None:
    """Render the single-select peer list and return the chosen entry."""
    if not peers:
        return None
    if len(peers) == 1:
        return peers[0]
    labels = [f"{p.name} -- {p.endpoint}" for p in peers]
    selection = dialogs.select(labels, title="Choose destination peer")
    if selection is None:
        return None
    for candidate in peers:
        label = f"{candidate.name} -- {candidate.endpoint}"
        if label == selection:
            return candidate
    return None


def confirm_send(
    selected: list[CandidateFile], peer: PeerEntry, bundle_id: str
) -> bool:
    """Show a summary screen; return True if the operator confirms."""
    total_bytes = sum(c.size_bytes for c in selected)
    message = (
        f"Destination: {peer.name} ({peer.endpoint})\n"
        f"Bundle id:   {bundle_id}\n"
        f"Files:       {len(selected)}\n"
        f"Total size:  {_size_label(total_bytes)}\n"
    )
    return bool(dialogs.confirm(message, title="Send report?"))


def run_interactive(
    config: ReportConfig,
    candidates: list[CandidateFile],
    bundle_id: str,
) -> TUIResult | None:
    """Drive the three-screen flow; return None on cancel/empty-selection."""
    selected = pick_files(candidates)
    if not selected:
        return None
    peer = pick_peer(config.peers)
    if peer is None:
        return None
    if not confirm_send(selected, peer, bundle_id):
        return None
    return TUIResult(selected=selected, peer=peer)


def resolve_selection_from_defaults(
    defaults: dict[str, object],
    candidates: list[CandidateFile],
) -> list[CandidateFile]:
    """Non-interactive path: `--ci --defaults FILE` lists relative paths."""
    raw = defaults.get("files", [])
    if not isinstance(raw, list):
        return []
    wanted = {str(entry) for entry in raw if isinstance(entry, str)}
    by_path = {c.absolute_path.as_posix(): c for c in candidates if c.toggleable}
    chosen = [c for c in candidates if c.relative_path in wanted and c.toggleable]
    if len(chosen) == len(wanted):
        return chosen
    for key, value in by_path.items():
        if key in wanted and value not in chosen:
            chosen.append(value)
    return chosen
