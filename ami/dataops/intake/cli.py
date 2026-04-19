"""argparse dispatcher for ami-intake.

Subcommands:
    serve         run the FastAPI daemon (used by systemd ExecStart)
    status        report unit state, bind address, current audit.log size
    ls            list received bundles, filterable by sender/date/status
    show          print the manifest + receipt for a specific bundle_id
    verify        recompute SHA256 for every file in a quarantined bundle
    rotate-audit  seal current audit.log and start a fresh one
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path

import uvicorn

from ami.dataops.intake import audit, quarantine, validation
from ami.dataops.intake.app import create_app
from ami.dataops.intake.config import load_intake_config

EXIT_OK = 0
EXIT_INVALID_ARGS = 2
EXIT_VERIFY_MISMATCH = 5
EXIT_UNEXPECTED = 10


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ami-intake", description="ami-intake")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the intake daemon")
    serve.add_argument("--config", required=True, type=Path)

    status_cmd = sub.add_parser("status", help="report unit + audit state")
    status_cmd.add_argument("--config", required=True, type=Path)

    ls = sub.add_parser("ls", help="list received bundles")
    ls.add_argument("--config", required=True, type=Path)
    ls.add_argument("--sender", default=None)

    show = sub.add_parser("show", help="print manifest + receipt")
    show.add_argument("--config", required=True, type=Path)
    show.add_argument("bundle_id")

    verify = sub.add_parser("verify", help="recompute hashes for a bundle")
    verify.add_argument("--config", required=True, type=Path)
    verify.add_argument("bundle_id")

    rotate = sub.add_parser("rotate-audit", help="seal current audit.log")
    rotate.add_argument("--config", required=True, type=Path)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handler = _DISPATCH[args.command]
    try:
        return handler(args)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_INVALID_ARGS


def _cmd_serve(args: argparse.Namespace) -> int:
    config = load_intake_config(args.config)
    uvicorn.run(
        create_app(config),
        host="127.0.0.1",
        port=config.intake_port,
        log_level="info",
    )
    return EXIT_OK


def _cmd_status(args: argparse.Namespace) -> int:
    config = load_intake_config(args.config)
    active = config.intake_root / audit.AUDIT_LOG_NAME
    print(f"intake_root: {config.intake_root}")
    print(f"bind:        127.0.0.1:{config.intake_port}")
    print(f"audit.log:   {active} ({'present' if active.exists() else 'absent'})")
    if active.exists():
        print(f"audit_bytes: {active.stat().st_size}")
    return EXIT_OK


def _cmd_ls(args: argparse.Namespace) -> int:
    config = load_intake_config(args.config)
    root = config.intake_root
    if not root.is_dir():
        return EXIT_OK
    for sender_dir in sorted(root.iterdir()):
        if not sender_dir.is_dir() or sender_dir.name == audit.AUDIT_ARCHIVE_DIR:
            continue
        if args.sender and sender_dir.name != args.sender:
            continue
        for bundle_dir in sorted(sender_dir.rglob("receipt.json")):
            print(f"{sender_dir.name} {bundle_dir.parent}")
    return EXIT_OK


def _cmd_show(args: argparse.Namespace) -> int:
    config = load_intake_config(args.config)
    target = _find_bundle(config.intake_root, args.bundle_id)
    if target is None:
        print(f"bundle_id {args.bundle_id!r} not found", file=sys.stderr)
        return EXIT_INVALID_ARGS
    manifest = (target / "manifest.json").read_text(encoding="utf-8")
    receipt = (target / "receipt.json").read_text(encoding="utf-8")
    combined = {"manifest": json.loads(manifest), "receipt": json.loads(receipt)}
    print(json.dumps(combined, indent=2))
    return EXIT_OK


def _cmd_verify(args: argparse.Namespace) -> int:
    config = load_intake_config(args.config)
    target = _find_bundle(config.intake_root, args.bundle_id)
    if target is None:
        print(f"bundle_id {args.bundle_id!r} not found", file=sys.stderr)
        return EXIT_INVALID_ARGS
    receipt = quarantine.read_receipt(target)
    mismatches = _collect_hash_mismatches(target, receipt.per_file_sha256_verified)
    if mismatches:
        for m in mismatches:
            print(m, file=sys.stderr)
        return EXIT_VERIFY_MISMATCH
    print(f"ok ({len(receipt.per_file_sha256_verified)} files)")
    return EXIT_OK


def _cmd_rotate(args: argparse.Namespace) -> int:
    config = load_intake_config(args.config)
    sealed = audit.rotate_audit(config.intake_root)
    print(f"sealed -> {sealed}")
    return EXIT_OK


def _one_hash_mismatch(target: Path, entry: quarantine.ReceiptFileEntry) -> str | None:
    try:
        validation.verify_hash(target / entry.relative_path, entry.sha256)
    except validation.ValidationRejected as exc:
        return f"{entry.relative_path}: {exc.detail}"
    return None


def _collect_hash_mismatches(
    target: Path,
    entries: list[quarantine.ReceiptFileEntry],
) -> list[str]:
    return [m for m in (_one_hash_mismatch(target, e) for e in entries) if m]


def _find_bundle(intake_root: Path, bundle_id: str) -> Path | None:
    if not intake_root.is_dir():
        return None
    for sender_dir in intake_root.iterdir():
        if sender_dir.name == audit.AUDIT_ARCHIVE_DIR:
            continue
        candidate = quarantine.bundle_exists(intake_root, sender_dir.name, bundle_id)
        if candidate is not None:
            return candidate
    return None


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "serve": _cmd_serve,
    "status": _cmd_status,
    "ls": _cmd_ls,
    "show": _cmd_show,
    "verify": _cmd_verify,
    "rotate-audit": _cmd_rotate,
}
