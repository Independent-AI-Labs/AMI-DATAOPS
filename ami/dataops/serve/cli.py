"""CLI dispatcher for ami-serve."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from ami.dataops.serve.ansible import dataops_root, find_ami_root, run_playbook
from ami.dataops.serve.cloudflare_bootstrap import (
    CloudflareBootstrapError,
    CloudflareCredentials,
    TunnelBootstrapRequest,
    ensure_intake_tunnel,
)

_DEFAULT_CI_CONFIG = Path("ami/config/serve-defaults.yaml")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ami-serve",
        description="Publish local apps on Cloudflare Tunnel FQDNs.",
    )
    sub = p.add_subparsers(dest="command", required=False)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--limit", dest="tunnel_limit", metavar="TUNNEL")

    deploy = sub.add_parser("deploy", parents=[common], help="render + start tunnels")
    deploy.add_argument("--route-dns", dest="route_dns", action="store_true")
    deploy.add_argument("--check", action="store_true", help="Ansible check mode")

    sub.add_parser("stop", parents=[common], help="stop tunnels")
    sub.add_parser("restart", parents=[common], help="restart tunnels")

    status = sub.add_parser("status", parents=[common], help="report tunnel state")
    status.add_argument("--json", action="store_true")

    sub.add_parser("route-dns", parents=[common], help="create Cloudflare CNAMEs")

    bootstrap = sub.add_parser(
        "bootstrap-cloudflare",
        help="create tunnel + CNAME via Cloudflare API token (headless)",
    )
    bootstrap.add_argument("--tunnel", required=True, metavar="NAME")
    bootstrap.add_argument("--hostname", required=True, metavar="FQDN")
    bootstrap.add_argument(
        "--credentials-dir",
        type=Path,
        default=Path("~/.cloudflared").expanduser(),
        metavar="DIR",
    )

    logs = sub.add_parser("logs", help="tail tunnel logs")
    logs.add_argument("tunnel", help="tunnel name")

    p.add_argument(
        "--ci",
        action="store_true",
        help=(
            f"Non-interactive CI; uses {_DEFAULT_CI_CONFIG} unless --defaults is given"
        ),
    )
    p.add_argument("--defaults", type=Path, metavar="FILE")
    return p


def _cmd_logs(tunnel: str) -> int:
    unit = f"ami-serve-{tunnel}.service"
    try:
        result = subprocess.run(
            ["journalctl", "--user", "-u", unit, "-f"],
            check=False,
        )
    except (FileNotFoundError, PermissionError) as exc:
        print(f"ami-serve: cannot launch journalctl: {exc}", file=sys.stderr)
        return 1
    return result.returncode


def _cmd_status(args: argparse.Namespace) -> int:
    # Delegate to the ansible status task for a uniform source of truth; the
    # Python status.py module is used by tests and by the --json formatter.
    return run_playbook("status", tunnel_limit=args.tunnel_limit)


def _cmd_deploy(args: argparse.Namespace) -> int:
    rc = run_playbook("deploy", tunnel_limit=args.tunnel_limit, check=args.check)
    if rc != 0 or not args.route_dns:
        return rc
    return run_playbook("route-dns", tunnel_limit=args.tunnel_limit, check=args.check)


def _cmd_stop(args: argparse.Namespace) -> int:
    return run_playbook("stop", tunnel_limit=args.tunnel_limit)


def _cmd_restart(args: argparse.Namespace) -> int:
    return run_playbook("restart", tunnel_limit=args.tunnel_limit)


def _cmd_route_dns(args: argparse.Namespace) -> int:
    return run_playbook("route-dns", tunnel_limit=args.tunnel_limit)


def _cmd_bootstrap_cloudflare(args: argparse.Namespace) -> int:
    credentials = _load_cloudflare_credentials()
    if credentials is None:
        return 2
    request = TunnelBootstrapRequest(
        credentials=credentials,
        tunnel_name=args.tunnel,
        hostname=args.hostname,
        credentials_dir=args.credentials_dir,
    )
    try:
        result = ensure_intake_tunnel(request)
    except CloudflareBootstrapError as exc:
        print(f"ami-serve bootstrap-cloudflare: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "tunnel_id": result.tunnel_id,
                "credentials_path": str(result.credentials_path),
                "cname_fqdn": result.cname_fqdn,
                "cname_target": result.cname_target,
                "tunnel_created": result.tunnel_created,
                "cname_changed": result.cname_changed,
            },
            indent=2,
        )
    )
    return 0


def _load_cloudflare_credentials() -> CloudflareCredentials | None:
    required = ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_ZONE_ID")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        print(
            "ami-serve bootstrap-cloudflare: missing env vars: " + ", ".join(missing),
            file=sys.stderr,
        )
        return None
    return CloudflareCredentials(
        api_token=os.environ["CLOUDFLARE_API_TOKEN"],
        account_id=os.environ["CLOUDFLARE_ACCOUNT_ID"],
        zone_id=os.environ["CLOUDFLARE_ZONE_ID"],
    )


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "deploy": _cmd_deploy,
    "stop": _cmd_stop,
    "restart": _cmd_restart,
    "status": _cmd_status,
    "route-dns": _cmd_route_dns,
    "bootstrap-cloudflare": _cmd_bootstrap_cloudflare,
}


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns exit code."""
    ami_root = find_ami_root()
    os.chdir(dataops_root(ami_root))

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "logs":
        return _cmd_logs(args.tunnel)

    handler = _DISPATCH.get(args.command)
    if handler is None:
        parser.error(f"unknown command: {args.command}")
        return 2
    return handler(args)
