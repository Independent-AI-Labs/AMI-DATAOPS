"""Unit tests for ami.dataops.serve.cloudflare_bootstrap.

Every Cloudflare HTTP interaction is mocked with respx so the helper runs
fully offline. One test per behaviour the operator relies on: fresh create,
reuse-on-rerun, CNAME already correct vs needs update, error surfaces.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from ami.dataops.serve.cloudflare_bootstrap import (
    CLOUDFLARE_API_BASE,
    CloudflareBootstrapError,
    CloudflareCredentials,
    TunnelBootstrapRequest,
    ensure_intake_tunnel,
)

_ACCOUNT = "acct-123"
_ZONE = "zone-456"
_TOKEN = "cf-token-xyz"
_TUNNEL_ID = "c1744f8b-faa1-48a4-9e5c-02ac921467fa"
_ACCOUNT_TAG = "699d98642c564d2e855e9661899b7252"
_HOSTNAME = "reports.ami-remote.work"
_CNAME_TARGET = f"{_TUNNEL_ID}.cfargotunnel.com"
_DNS_RECORD_ID = "dns-rec-789"


def _request(
    credentials_dir: Path, tunnel_name: str = "intake-main"
) -> TunnelBootstrapRequest:
    return TunnelBootstrapRequest(
        credentials=CloudflareCredentials(
            api_token=_TOKEN, account_id=_ACCOUNT, zone_id=_ZONE
        ),
        tunnel_name=tunnel_name,
        hostname=_HOSTNAME,
        credentials_dir=credentials_dir,
    )


def _build_client() -> httpx.Client:
    return httpx.Client(
        base_url=CLOUDFLARE_API_BASE,
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )


def _mock_list_tunnels_empty(mock: respx.MockRouter) -> respx.Route:
    return mock.get(f"/accounts/{_ACCOUNT}/cfd_tunnel").mock(
        return_value=httpx.Response(200, json={"result": [], "success": True})
    )


def _mock_list_tunnels_one(
    mock: respx.MockRouter, name: str = "intake-main"
) -> respx.Route:
    return mock.get(f"/accounts/{_ACCOUNT}/cfd_tunnel").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": [
                    {"id": _TUNNEL_ID, "name": name, "account_tag": _ACCOUNT_TAG}
                ],
                "success": True,
            },
        )
    )


def _mock_create_tunnel(mock: respx.MockRouter) -> respx.Route:
    return mock.post(f"/accounts/{_ACCOUNT}/cfd_tunnel").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": {
                    "id": _TUNNEL_ID,
                    "name": "intake-main",
                    "account_tag": _ACCOUNT_TAG,
                    "config_src": "local",
                },
                "success": True,
            },
        )
    )


def _mock_list_cname_none(mock: respx.MockRouter) -> respx.Route:
    return mock.get(f"/zones/{_ZONE}/dns_records").mock(
        return_value=httpx.Response(200, json={"result": [], "success": True})
    )


def _mock_list_cname_matching(mock: respx.MockRouter) -> respx.Route:
    return mock.get(f"/zones/{_ZONE}/dns_records").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": [
                    {
                        "id": _DNS_RECORD_ID,
                        "name": _HOSTNAME,
                        "type": "CNAME",
                        "content": _CNAME_TARGET,
                    }
                ],
                "success": True,
            },
        )
    )


def _mock_list_cname_stale(mock: respx.MockRouter) -> respx.Route:
    return mock.get(f"/zones/{_ZONE}/dns_records").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": [
                    {
                        "id": _DNS_RECORD_ID,
                        "name": _HOSTNAME,
                        "type": "CNAME",
                        "content": "old.cfargotunnel.com",
                    }
                ],
                "success": True,
            },
        )
    )


def _mock_create_cname(mock: respx.MockRouter) -> respx.Route:
    return mock.post(f"/zones/{_ZONE}/dns_records").mock(
        return_value=httpx.Response(200, json={"success": True, "result": {}})
    )


def _mock_update_cname(mock: respx.MockRouter) -> respx.Route:
    return mock.put(f"/zones/{_ZONE}/dns_records/{_DNS_RECORD_ID}").mock(
        return_value=httpx.Response(200, json={"success": True, "result": {}})
    )


class TestCreateTunnelFresh:
    def test_creates_tunnel_and_cname_from_scratch(self, tmp_path: Path) -> None:
        with respx.mock(base_url=CLOUDFLARE_API_BASE, assert_all_called=True) as mock:
            list_tunnels = _mock_list_tunnels_empty(mock)
            create_tunnel = _mock_create_tunnel(mock)
            list_cname = _mock_list_cname_none(mock)
            create_cname = _mock_create_cname(mock)

            result = ensure_intake_tunnel(_request(tmp_path), client=_build_client())

        assert result.tunnel_id == _TUNNEL_ID
        assert result.tunnel_created is True
        assert result.cname_changed is True
        assert result.cname_fqdn == _HOSTNAME
        assert result.cname_target == _CNAME_TARGET

        assert list_tunnels.called
        assert create_tunnel.called
        assert list_cname.called
        assert create_cname.called

        # Credentials file written with the three expected fields.
        creds_path = tmp_path / f"{_TUNNEL_ID}.json"
        assert creds_path.is_file()
        loaded = json.loads(creds_path.read_text())
        assert loaded["AccountTag"] == _ACCOUNT_TAG
        assert loaded["TunnelID"] == _TUNNEL_ID
        assert isinstance(loaded["TunnelSecret"], str)
        assert len(loaded["TunnelSecret"]) > 0

    def test_credentials_file_is_mode_0600(self, tmp_path: Path) -> None:
        expected_mode = 0o600
        with respx.mock(base_url=CLOUDFLARE_API_BASE) as mock:
            _mock_list_tunnels_empty(mock)
            _mock_create_tunnel(mock)
            _mock_list_cname_none(mock)
            _mock_create_cname(mock)
            ensure_intake_tunnel(_request(tmp_path), client=_build_client())
        creds_path = tmp_path / f"{_TUNNEL_ID}.json"
        assert creds_path.stat().st_mode & 0o777 == expected_mode


class TestReuseExistingTunnel:
    def test_skips_create_when_tunnel_exists_with_local_credentials(
        self, tmp_path: Path
    ) -> None:
        # Pre-seed the credentials file as if a previous run created the tunnel.
        (tmp_path / f"{_TUNNEL_ID}.json").write_text(
            json.dumps(
                {
                    "AccountTag": _ACCOUNT_TAG,
                    "TunnelID": _TUNNEL_ID,
                    "TunnelSecret": "x",
                }
            )
        )
        with respx.mock(base_url=CLOUDFLARE_API_BASE, assert_all_called=True) as mock:
            list_tunnels = _mock_list_tunnels_one(mock)
            list_cname = _mock_list_cname_matching(mock)

            result = ensure_intake_tunnel(_request(tmp_path), client=_build_client())

        assert result.tunnel_created is False
        assert result.cname_changed is False
        assert list_tunnels.called
        assert list_cname.called

    def test_updates_cname_when_stale(self, tmp_path: Path) -> None:
        (tmp_path / f"{_TUNNEL_ID}.json").write_text(
            json.dumps(
                {
                    "AccountTag": _ACCOUNT_TAG,
                    "TunnelID": _TUNNEL_ID,
                    "TunnelSecret": "x",
                }
            )
        )
        with respx.mock(base_url=CLOUDFLARE_API_BASE, assert_all_called=True) as mock:
            _mock_list_tunnels_one(mock)
            _mock_list_cname_stale(mock)
            update_cname = _mock_update_cname(mock)

            result = ensure_intake_tunnel(_request(tmp_path), client=_build_client())

        assert result.cname_changed is True
        assert update_cname.called


class TestMissingCredentialsRaises:
    def test_existing_tunnel_without_local_creds_raises(self, tmp_path: Path) -> None:
        with respx.mock(base_url=CLOUDFLARE_API_BASE) as mock:
            _mock_list_tunnels_one(mock)
            with pytest.raises(
                CloudflareBootstrapError, match="credentials can only be obtained"
            ):
                ensure_intake_tunnel(_request(tmp_path), client=_build_client())


class TestApiErrors:
    def test_403_bubbles_as_bootstrap_error(self, tmp_path: Path) -> None:
        with respx.mock(base_url=CLOUDFLARE_API_BASE) as mock:
            mock.get(f"/accounts/{_ACCOUNT}/cfd_tunnel").mock(
                return_value=httpx.Response(
                    403,
                    json={
                        "success": False,
                        "errors": [{"message": "insufficient permissions"}],
                    },
                )
            )
            with pytest.raises(CloudflareBootstrapError, match="403"):
                ensure_intake_tunnel(_request(tmp_path), client=_build_client())

    def test_create_tunnel_missing_result_id_raises(self, tmp_path: Path) -> None:
        with respx.mock(base_url=CLOUDFLARE_API_BASE) as mock:
            _mock_list_tunnels_empty(mock)
            mock.post(f"/accounts/{_ACCOUNT}/cfd_tunnel").mock(
                return_value=httpx.Response(
                    200, json={"success": True, "result": {"name": "x"}}
                )
            )
            with pytest.raises(CloudflareBootstrapError, match=r"missing 'result\.id'"):
                ensure_intake_tunnel(_request(tmp_path), client=_build_client())


class TestModels:
    def test_rejects_empty_token(self) -> None:
        with pytest.raises(ValueError, match="at least 1 character"):
            CloudflareCredentials(api_token="", account_id=_ACCOUNT, zone_id=_ZONE)

    def test_request_rejects_empty_tunnel_name(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="at least 1 character"):
            TunnelBootstrapRequest(
                credentials=CloudflareCredentials(
                    api_token=_TOKEN, account_id=_ACCOUNT, zone_id=_ZONE
                ),
                tunnel_name="",
                hostname=_HOSTNAME,
                credentials_dir=tmp_path,
            )
