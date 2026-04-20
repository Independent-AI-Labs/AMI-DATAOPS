"""argparse dispatcher for ami-report.

Subcommands: send (default), preview, peers. send accepts --ci --defaults
for non-interactive use and --dry-run to sign without posting.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path

import uuid_utils
import yaml

from ami.dataops.intake import validation
from ami.dataops.report import manifest as manifest_mod
from ami.dataops.report import tui, wizard
from ami.dataops.report.bundling import build_bundle_tarball
from ami.dataops.report.config import PeerEntry, ReportConfig, load_report_config
from ami.dataops.report.scanner import (
    CandidateFile,
    TreeEntry,
    expand_selection,
    scan_roots,
)
from ami.dataops.report.transport import (
    AuthRejected,
    NetworkError,
    PostContext,
    ValidationRejectedByPeer,
    post_bundle,
)

EXIT_OK = 0
EXIT_INVALID_ARGS = 2
EXIT_NETWORK_ERROR = 3
EXIT_AUTH_REJECTED = 4
EXIT_VALIDATION_REJECTED_PEER = 5
EXIT_LOCAL_PREFLIGHT_FAILED = 6
EXIT_UNEXPECTED = 10


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ami-report", description="ami-report")
    parser.add_argument(
        "--extensions",
        type=str,
        default=None,
        help="comma-separated allowlist override (e.g. log,txt,json). "
        "Applies to scope discovery + per-file pre-flight. Defaults to 'log'.",
    )
    sub = parser.add_subparsers(dest="command")

    send = sub.add_parser("send", help="select + sign + POST a bundle")
    send.add_argument("--config", required=True, type=Path)
    send.add_argument("--ci", action="store_true")
    send.add_argument("--defaults", type=Path, default=None)
    send.add_argument("--dry-run", action="store_true")

    preview = sub.add_parser("preview", help="print what would be sent")
    preview.add_argument("--config", required=True, type=Path)

    peers = sub.add_parser("peers", help="list configured peers")
    peers.add_argument("--config", required=True, type=Path)

    return parser


def main(argv: list[str] | None = None) -> int:
    effective = sys.argv[1:] if argv is None else argv
    parser = build_parser()
    if not effective:
        return wizard.run()
    args = parser.parse_args(effective)
    extensions = (
        wizard._normalize_extensions(args.extensions) if args.extensions else None
    )
    if args.command is None:
        return wizard.run(extensions=extensions)
    handler = _DISPATCH.get(args.command)
    if handler is None:
        print(f"error: unknown command {args.command}", file=sys.stderr)
        return EXIT_INVALID_ARGS
    args.extensions_frozen = extensions
    try:
        return handler(args)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INVALID_ARGS


def _resolve_roots(config: ReportConfig) -> list[Path]:
    roots: list[Path] = []
    ami_root = Path(os.environ.get("AMI_ROOT", "")).expanduser().absolute()
    if ami_root and (ami_root / "logs").is_dir():
        roots.append(ami_root / "logs")
    roots.extend(config.sender.extra_roots)
    return roots


def _cmd_send(args: argparse.Namespace) -> int:
    config = load_report_config(args.config)
    entries = scan_roots(
        _resolve_roots(config),
        allowed_extensions=getattr(args, "extensions_frozen", None),
    )
    bundle_id = str(uuid_utils.uuid7())
    selected_and_peer = _pick_selection_and_peer(args, config, entries, bundle_id)
    if selected_and_peer is None:
        return EXIT_OK
    selected, peer = selected_and_peer
    expanded = expand_selection(selected, entries)
    if not expanded:
        return EXIT_OK
    source_root = _common_source_root(expanded)
    try:
        for candidate in expanded:
            validation.probe_text_content(candidate.absolute_path)
    except validation.ValidationRejected as exc:
        print(f"local pre-flight failed: {exc}", file=sys.stderr)
        return EXIT_LOCAL_PREFLIGHT_FAILED
    manifest = manifest_mod.build_manifest(
        sender_id=config.sender.sender_id,
        source_root=source_root,
        files=[c.absolute_path for c in expanded],
        bundle_id=bundle_id,
    )
    manifest_bytes = manifest_mod.canonical_manifest_bytes(manifest)
    secret = _require_env(peer.shared_secret_env_var)
    token = _require_env(f"AMI_REPORT_TOKENS__{peer.name.upper()}")
    signature = manifest_mod.sign_manifest(manifest_bytes, secret)
    if args.dry_run:
        sys.stdout.buffer.write(manifest_bytes)
        print(signature)
        return EXIT_OK
    bundle = build_bundle_tarball(manifest, source_root)
    return _post_and_report(
        endpoint=f"{peer.endpoint}v1/bundles",
        bearer_token=token,
        manifest=manifest,
        manifest_bytes=manifest_bytes,
        signature=signature,
        bundle_bytes=bundle,
    )


def _cmd_preview(args: argparse.Namespace) -> int:
    config = load_report_config(args.config)
    entries = scan_roots(_resolve_roots(config))
    files = [e for e in entries if isinstance(e, CandidateFile)]
    ok_count = sum(1 for c in files if c.toggleable)
    print(f"sender_id:       {config.sender.sender_id}")
    print(f"candidate files: {ok_count} ok / {len(files)} total")
    for candidate in files:
        status = "ok" if candidate.toggleable else candidate.preflight
        print(f"  [{status:<18}] {candidate.relative_path} ({candidate.size_bytes} B)")
    print(f"peers:           {[p.name for p in config.peers]}")
    return EXIT_OK


def _cmd_peers(args: argparse.Namespace) -> int:
    config = load_report_config(args.config)
    for peer in config.peers:
        token_env = f"AMI_REPORT_TOKENS__{peer.name.upper()}"
        token_state = "set" if os.environ.get(token_env) else "MISSING"
        has_secret = os.environ.get(peer.shared_secret_env_var)
        secret_state = "set" if has_secret else "MISSING"
        print(
            f"{peer.name:<12} {peer.endpoint} "
            f"(token: {token_state}, secret: {secret_state})"
        )
    return EXIT_OK


def _pick_selection_and_peer(
    args: argparse.Namespace,
    config: ReportConfig,
    entries: list[TreeEntry],
    bundle_id: str,
) -> tuple[list[TreeEntry], PeerEntry] | None:
    if args.ci:
        return _ci_selection_and_peer(args, config, entries)
    result = tui.run_interactive(config, entries, bundle_id)
    if result is None:
        return None
    return result.selected, result.peer


def _ci_selection_and_peer(
    args: argparse.Namespace,
    config: ReportConfig,
    entries: list[TreeEntry],
) -> tuple[list[TreeEntry], PeerEntry] | None:
    defaults_path = args.defaults or config.sender.default_ci_defaults
    if defaults_path is None:
        print("error: --ci requires --defaults FILE", file=sys.stderr)
        return None
    raw = yaml.safe_load(Path(defaults_path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        print(f"error: {defaults_path} is not a YAML mapping", file=sys.stderr)
        return None
    peer_name = raw.get("peer")
    if not isinstance(peer_name, str):
        print("error: defaults file missing string 'peer'", file=sys.stderr)
        return None
    selected = tui.resolve_selection_from_defaults(raw, entries)
    return selected, config.peer(peer_name)


def _common_source_root(selected: list[CandidateFile]) -> Path:
    paths: list[Path] = [c.absolute_path for c in selected]
    root: Path = paths[0].parent
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


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        msg = f"required env var {name} is not set"
        raise ValueError(msg)
    return value


def _post_and_report(**kwargs: object) -> int:
    try:
        ctx = PostContext.model_validate(kwargs)
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


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "send": _cmd_send,
    "preview": _cmd_preview,
    "peers": _cmd_peers,
}
