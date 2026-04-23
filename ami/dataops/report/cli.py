"""argparse dispatcher for ami-report.

`ami-report` (bare) and `ami-report send …` both build a `RunRequest`
and hand it to `pipeline.run` together with the appropriate `Operator`
(TerminalOperator for humans, CIOperator for `--ci --defaults FILE`).
`preview` and `peers` are non-interactive read-only paths that don't
need the pipeline.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import yaml

from ami.dataops.report import pipeline
from ami.dataops.report.config import ReportConfig, load_report_config
from ami.dataops.report.models import CIDefaults, RunRequest
from ami.dataops.report.operator import CIOperator, Operator, TerminalOperator
from ami.dataops.report.scanner import CandidateFile, scan_roots
from ami.dataops.report.windows import normalize_key

EXIT_OK = 0
EXIT_INVALID_ARGS = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ami-report", description="ami-report")
    parser.add_argument(
        "--extensions",
        type=str,
        default=None,
        help="comma-separated allowlist override (e.g. log,txt,json). "
        "Applies to scope discovery + per-file pre-flight. Defaults to 'log'.",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="time window key: all | 1m | 5m | 15m | 1h | 8h | 1d. "
        "Filters candidates by file mtime; skips the interactive picker.",
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
    args = build_parser().parse_args(effective)
    try:
        request = _request_from_args(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INVALID_ARGS
    if args.command == "preview":
        return _cmd_preview(args)
    if args.command == "peers":
        return _cmd_peers(args)
    operator = _operator_for(args, request)
    if operator is None:
        return EXIT_INVALID_ARGS
    try:
        return pipeline.run(request, operator)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INVALID_ARGS


def _request_from_args(args: argparse.Namespace) -> RunRequest:
    extensions = _normalize_extensions(args.extensions) if args.extensions else None
    since_key = normalize_key(args.since)
    return RunRequest(
        config_path=getattr(args, "config", None),
        extensions=extensions,
        since_key=since_key,
        dry_run=getattr(args, "dry_run", False),
        ci_defaults_path=getattr(args, "defaults", None)
        if getattr(args, "ci", False)
        else None,
    )


def _operator_for(args: argparse.Namespace, request: RunRequest) -> Operator | None:
    if not getattr(args, "ci", False):
        return TerminalOperator()
    defaults_path = request.ci_defaults_path
    if defaults_path is None:
        print("error: --ci requires --defaults FILE", file=sys.stderr)
        return None
    if not defaults_path.is_file():
        print(f"error: defaults file not found: {defaults_path}", file=sys.stderr)
        return None
    raw = yaml.safe_load(defaults_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        print(f"error: {defaults_path} is not a YAML mapping", file=sys.stderr)
        return None
    try:
        defaults = CIDefaults.model_validate(raw)
    except ValueError as exc:
        print(f"error: {defaults_path} is invalid: {exc}", file=sys.stderr)
        return None
    return CIOperator(defaults)


def _normalize_extensions(raw: str) -> frozenset[str]:
    out: set[str] = set()
    for item in raw.split(","):
        trimmed = item.strip().lower()
        if not trimmed:
            continue
        out.add(trimmed if trimmed.startswith(".") else f".{trimmed}")
    return frozenset(out)


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


def _resolve_roots(config: ReportConfig) -> list[Path]:
    roots: list[Path] = []
    ami_root = Path(os.environ.get("AMI_ROOT", "")).expanduser().absolute()
    if ami_root and (ami_root / "logs").is_dir():
        roots.append(ami_root / "logs")
    roots.extend(config.sender.extra_roots)
    return roots
