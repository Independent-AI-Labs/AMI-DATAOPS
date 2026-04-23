"""Headless Cloudflare Tunnel + DNS bootstrap.

Replaces the interactive `cloudflared tunnel login` / `cloudflared tunnel
create` / `cloudflared tunnel route dns` flow with a single API-token-driven
helper. Given a Cloudflare API token scoped to `Cloudflare Tunnel: Edit`
and `DNS: Edit`, this creates (or reuses) a locally-managed tunnel, writes
its credentials JSON to `~/.cloudflared/<uuid>.json`, and upserts a CNAME
from `<hostname>` to `<tunnel-uuid>.cfargotunnel.com`.

The credentials for a locally-managed tunnel are ONLY returned at creation
time — there is no Cloudflare endpoint to retrieve them later. If the named
tunnel already exists but its credentials file is missing locally, the
bootstrap raises a clear error and refuses to proceed (operator must delete
and re-create, or recover the credentials from a backup).
"""

from __future__ import annotations

import base64
import json
import secrets
from pathlib import Path

import httpx
from pydantic import BaseModel, ConfigDict, Field

CLOUDFLARE_API_BASE = "https://api.cloudflare.com/client/v4"
CNAME_TARGET_SUFFIX = "cfargotunnel.com"
TUNNEL_SECRET_BYTES = 32
DEFAULT_DNS_TTL = 1  # 1 = "automatic" per Cloudflare
CONFIG_SRC_LOCAL = "local"


class CloudflareBootstrapError(RuntimeError):
    """Raised when the bootstrap cannot safely proceed."""


class CloudflareCredentials(BaseModel):
    """Scoped API-token + account/zone identifiers for the bootstrap."""

    model_config = ConfigDict(extra="forbid")

    api_token: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    zone_id: str = Field(min_length=1)


class TunnelBootstrapRequest(BaseModel):
    """Inputs to `ensure_intake_tunnel` — one tunnel, one hostname."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    credentials: CloudflareCredentials
    tunnel_name: str = Field(min_length=1)
    hostname: str = Field(min_length=1)
    credentials_dir: Path


class TunnelProvisionResult(BaseModel):
    """Outcome of a successful bootstrap run."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    tunnel_id: str
    credentials_path: Path
    cname_fqdn: str
    cname_target: str
    tunnel_created: bool
    cname_changed: bool


class _TunnelCreated(BaseModel):
    """Fields we extract from `POST /cfd_tunnel` — ignore everything else."""

    model_config = ConfigDict(extra="ignore")

    id: str
    account_tag: str


class _DnsRecord(BaseModel):
    """Fields we extract from `GET /zones/{id}/dns_records`."""

    model_config = ConfigDict(extra="ignore")

    id: str
    content: str


def ensure_intake_tunnel(
    request: TunnelBootstrapRequest,
    *,
    client: httpx.Client | None = None,
) -> TunnelProvisionResult:
    """Create-or-reuse the named tunnel and upsert the CNAME. Idempotent."""
    owned = client is None
    http = client or _build_client(request.credentials.api_token)
    try:
        tunnel_id, tunnel_created = _ensure_tunnel(http, request)
        credentials_path = _credentials_path(request.credentials_dir, tunnel_id)
        if tunnel_created:
            # Credentials JSON was assembled by _ensure_tunnel; nothing to do.
            pass
        elif not credentials_path.is_file():
            msg = (
                f"tunnel {request.tunnel_name!r} already exists as {tunnel_id} but "
                f"{credentials_path} is missing; credentials can only be obtained "
                "at creation time. Delete the tunnel in Cloudflare or restore the "
                "credentials JSON from a backup before rerunning."
            )
            raise CloudflareBootstrapError(msg)
        cname_target = f"{tunnel_id}.{CNAME_TARGET_SUFFIX}"
        cname_changed = _ensure_cname(
            http,
            zone_id=request.credentials.zone_id,
            hostname=request.hostname,
            target=cname_target,
        )
        return TunnelProvisionResult(
            tunnel_id=tunnel_id,
            credentials_path=credentials_path,
            cname_fqdn=request.hostname,
            cname_target=cname_target,
            tunnel_created=tunnel_created,
            cname_changed=cname_changed,
        )
    finally:
        if owned:
            http.close()


def _build_client(api_token: str) -> httpx.Client:
    return httpx.Client(
        base_url=CLOUDFLARE_API_BASE,
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        timeout=httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0),
    )


def _credentials_path(directory: Path, tunnel_id: str) -> Path:
    return directory.expanduser() / f"{tunnel_id}.json"


def _ensure_tunnel(
    http: httpx.Client, request: TunnelBootstrapRequest
) -> tuple[str, bool]:
    """Return `(tunnel_id, created)` — reuse an existing tunnel by name."""
    existing = _find_tunnel_by_name(
        http, account_id=request.credentials.account_id, name=request.tunnel_name
    )
    if existing is not None:
        return existing, False
    secret = base64.b64encode(secrets.token_bytes(TUNNEL_SECRET_BYTES)).decode("ascii")
    created = _create_tunnel(
        http,
        account_id=request.credentials.account_id,
        name=request.tunnel_name,
        tunnel_secret=secret,
    )
    _write_credentials(
        request.credentials_dir,
        tunnel_id=created.id,
        account_tag=created.account_tag,
        tunnel_secret=secret,
    )
    return created.id, True


def _find_tunnel_by_name(
    http: httpx.Client, *, account_id: str, name: str
) -> str | None:
    response = http.get(
        f"/accounts/{account_id}/cfd_tunnel",
        params={"name": name, "is_deleted": "false"},
    )
    _raise_for_status(response, context=f"list tunnels name={name!r}")
    body = response.json()
    matches = [t for t in body.get("result", []) if t.get("name") == name]
    if not matches:
        return None
    return str(matches[0]["id"])


def _create_tunnel(
    http: httpx.Client, *, account_id: str, name: str, tunnel_secret: str
) -> _TunnelCreated:
    response = http.post(
        f"/accounts/{account_id}/cfd_tunnel",
        json={
            "name": name,
            "tunnel_secret": tunnel_secret,
            "config_src": CONFIG_SRC_LOCAL,
        },
    )
    _raise_for_status(response, context=f"create tunnel name={name!r}")
    body = response.json()
    raw = body.get("result")
    if not isinstance(raw, dict) or "id" not in raw:
        msg = f"create tunnel response missing 'result.id': {body}"
        raise CloudflareBootstrapError(msg)
    return _TunnelCreated.model_validate(raw)


def _write_credentials(
    directory: Path, *, tunnel_id: str, account_tag: str, tunnel_secret: str
) -> None:
    directory = directory.expanduser()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = directory / f"{tunnel_id}.json"
    payload = {
        "AccountTag": account_tag,
        "TunnelID": tunnel_id,
        "TunnelSecret": tunnel_secret,
    }
    path.write_text(json.dumps(payload))
    path.chmod(0o600)


def _ensure_cname(
    http: httpx.Client, *, zone_id: str, hostname: str, target: str
) -> bool:
    """Create or update the CNAME; return True iff a Cloudflare write happened."""
    existing = _find_cname(http, zone_id=zone_id, hostname=hostname)
    if existing is None:
        _create_cname(http, zone_id=zone_id, hostname=hostname, target=target)
        return True
    if existing.content == target:
        return False
    _update_cname(
        http,
        zone_id=zone_id,
        record_id=existing.id,
        hostname=hostname,
        target=target,
    )
    return True


def _find_cname(
    http: httpx.Client, *, zone_id: str, hostname: str
) -> _DnsRecord | None:
    response = http.get(
        f"/zones/{zone_id}/dns_records",
        params={"name": hostname, "type": "CNAME"},
    )
    _raise_for_status(response, context=f"list dns_records name={hostname!r}")
    body = response.json()
    matches = [r for r in body.get("result", []) if r.get("name") == hostname]
    if not matches:
        return None
    return _DnsRecord.model_validate(matches[0])


def _create_cname(
    http: httpx.Client, *, zone_id: str, hostname: str, target: str
) -> None:
    response = http.post(
        f"/zones/{zone_id}/dns_records",
        json={
            "type": "CNAME",
            "name": hostname,
            "content": target,
            "ttl": DEFAULT_DNS_TTL,
            "proxied": True,
        },
    )
    _raise_for_status(response, context=f"create dns_record {hostname} -> {target}")


def _update_cname(
    http: httpx.Client,
    *,
    zone_id: str,
    record_id: str,
    hostname: str,
    target: str,
) -> None:
    response = http.put(
        f"/zones/{zone_id}/dns_records/{record_id}",
        json={
            "type": "CNAME",
            "name": hostname,
            "content": target,
            "ttl": DEFAULT_DNS_TTL,
            "proxied": True,
        },
    )
    _raise_for_status(response, context=f"update dns_record {hostname} -> {target}")


def _raise_for_status(response: httpx.Response, *, context: str) -> None:
    if response.is_success:
        return
    try:
        detail = response.json()
    except ValueError:
        detail = response.text[:200]
    msg = f"Cloudflare {context} failed ({response.status_code}): {detail}"
    raise CloudflareBootstrapError(msg)
