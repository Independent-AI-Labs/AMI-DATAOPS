"""One straight-line pipeline for ami-report.

Both `ami-report` (bare) and `ami-report send …` build a `RunRequest`
and call `run(request, operator)`. `--ci --defaults y.yml` swaps
`TerminalOperator` for `CIOperator`; the pipeline itself does not
branch on interactive vs non-interactive — every stage asks the
operator, and the operator decides whether to prompt or answer from
config.
"""

from __future__ import annotations

import json
import os
import socket
import sys
import time
from pathlib import Path

from ami.dataops.intake import validation
from ami.dataops.report.archive import build_signed_bundle
from ami.dataops.report.config import (
    PeerEntry,
    ReportConfig,
    SenderConfig,
    load_report_config,
)
from ami.dataops.report.defaults import merge_default_peer
from ami.dataops.report.discovery import candidates_for
from ami.dataops.report.models import (
    PeerCredentials,
    RunRequest,
    SendPlan,
    SignedBundle,
    TimeWindow,
    WindowKey,
)
from ami.dataops.report.operator import Operator, OperatorCancelled
from ami.dataops.report.scanner import (
    CandidateFile,
    TreeEntry,
    expand_selection,
    filter_by_window,
    scan_roots,
)
from ami.dataops.report.transport import (
    AuthRejected,
    NetworkError,
    PostContext,
    ValidationRejectedByPeer,
    post_bundle,
)
from ami.dataops.report.windows import by_key, tallies_for

EXIT_OK = 0
EXIT_NETWORK_ERROR = 3
EXIT_AUTH_REJECTED = 4
EXIT_VALIDATION_REJECTED_PEER = 5
EXIT_LOCAL_PREFLIGHT_FAILED = 6


def run(request: RunRequest, operator: Operator) -> int:
    """Drive every stage via `operator`; return the process exit code."""
    try:
        return _run_inner(request, operator)
    except OperatorCancelled:
        return EXIT_OK


def _run_inner(request: RunRequest, operator: Operator) -> int:
    config = _load_or_build_config(request.config_path)
    sender_id = operator.resolve_sender_id(config)
    entries = _collect_filtered_entries(request, operator, config)
    if entries is None:
        return EXIT_OK
    expanded = _pick_expanded(operator, entries)
    if expanded is None:
        return EXIT_OK
    preflight_rc = _local_preflight(expanded)
    if preflight_rc is not None:
        return preflight_rc
    peer = operator.resolve_peer(config.peers)
    creds = operator.resolve_credentials(peer)
    plan = SendPlan(
        sender_id=sender_id,
        peer=peer,
        source_root=_common_source_root(expanded),
        files=expanded,
    )
    bundle = build_signed_bundle(plan, creds)
    return _deliver_bundle(request, operator, bundle, peer, creds)


def _collect_filtered_entries(
    request: RunRequest, operator: Operator, config: ReportConfig
) -> list[TreeEntry] | None:
    suffixes = _suffixes(request.extensions)
    candidates = candidates_for(
        _ami_root(), list(config.sender.extra_roots), allowed_suffixes=suffixes
    )
    scope = operator.resolve_scope(candidates, config)
    if not scope.roots:
        print("no scan roots chosen; nothing to report", file=sys.stderr)
        return None
    entries = scan_roots(scope.roots, allowed_extensions=request.extensions)
    now = time.time()
    window = _resolve_window(operator, entries, request.since_key, now)
    entries = filter_by_window(entries, window.cutoff(now))
    if not entries:
        print("no candidate files in the selected window", file=sys.stderr)
        return None
    return entries


def _pick_expanded(
    operator: Operator, entries: list[TreeEntry]
) -> list[CandidateFile] | None:
    selection = operator.resolve_selection(entries)
    expanded = expand_selection(selection, entries)
    if not expanded:
        print("selection expanded to zero files", file=sys.stderr)
        return None
    return expanded


def _local_preflight(expanded: list[CandidateFile]) -> int | None:
    try:
        for candidate in expanded:
            validation.probe_text_content(candidate.absolute_path)
    except validation.ValidationRejected as exc:
        print(f"local pre-flight failed: {exc}", file=sys.stderr)
        return EXIT_LOCAL_PREFLIGHT_FAILED
    return None


def _deliver_bundle(
    request: RunRequest,
    operator: Operator,
    bundle: SignedBundle,
    peer: PeerEntry,
    creds: PeerCredentials,
) -> int:
    if request.dry_run:
        sys.stdout.buffer.write(bundle.canonical_bytes)
        print(bundle.signature)
        return EXIT_OK
    if not operator.review_archive(bundle.archive_summary):
        return EXIT_OK
    if not operator.confirm_send(bundle, peer):
        return EXIT_OK
    return _dispatch_post(bundle, peer, creds)


def _load_or_build_config(config_path: Path | None) -> ReportConfig:
    if config_path is not None and config_path.is_file():
        cfg = load_report_config(config_path)
    else:
        cfg = ReportConfig(
            sender=SenderConfig(sender_id=socket.gethostname() or "anonymous"),
            peers=[],
        )
    return merge_default_peer(cfg)


def _ami_root() -> Path | None:
    raw = os.environ.get("AMI_ROOT", "")
    if not raw:
        return None
    path = Path(raw).expanduser().absolute()
    return path if path.is_dir() else None


def _suffixes(extensions: frozenset[str] | None) -> tuple[str, ...] | None:
    if extensions is None:
        return None
    return tuple(sorted(extensions))


def _resolve_window(
    operator: Operator,
    entries: list[TreeEntry],
    since_key: WindowKey | None,
    now: float,
) -> TimeWindow:
    if since_key is not None:
        return by_key(since_key)
    return operator.resolve_window(tallies_for(entries, now))


def _common_source_root(files: list[CandidateFile]) -> Path:
    paths = [c.absolute_path for c in files]
    root = paths[0].parent
    while not all(_is_under(root, p) for p in paths):
        new_root = root.parent
        if new_root == root:
            break
        root = new_root
    return root


def _is_under(root: Path, path: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _dispatch_post(
    bundle: SignedBundle, peer: PeerEntry, creds: PeerCredentials
) -> int:
    ctx = PostContext(
        endpoint=f"{peer.endpoint}v1/bundles",
        bearer_token=creds.bearer_token,
        manifest=bundle.manifest,
        manifest_bytes=bundle.canonical_bytes,
        signature=bundle.signature,
        bundle_bytes=bundle.tar_bytes,
    )
    try:
        receipt = post_bundle(ctx)
    except AuthRejected as exc:
        print(f"auth rejected: {exc}", file=sys.stderr)
        return EXIT_AUTH_REJECTED
    except ValidationRejectedByPeer as exc:
        print(f"validation reject {exc.reason_code}: {exc.detail}", file=sys.stderr)
        return EXIT_VALIDATION_REJECTED_PEER
    except NetworkError as exc:
        print(f"network error: {exc}", file=sys.stderr)
        return EXIT_NETWORK_ERROR
    print(json.dumps(receipt, indent=2))
    return EXIT_OK
