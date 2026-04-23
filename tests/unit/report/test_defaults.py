"""Unit tests for ami.dataops.report.defaults."""

from __future__ import annotations

from ami.dataops.report.config import PeerEntry, ReportConfig, SenderConfig
from ami.dataops.report.defaults import (
    DEFAULT_PEER_ENDPOINT,
    DEFAULT_PEER_NAME,
    build_default_peer,
    default_config,
    merge_default_peer,
)


class TestDefaultPeer:
    def test_endpoint_is_public_reports(self) -> None:
        assert DEFAULT_PEER_ENDPOINT == "https://reports.ami-remote.work/"

    def test_build_default_peer_shape(self) -> None:
        peer = build_default_peer()
        assert peer.name == DEFAULT_PEER_NAME
        assert str(peer.endpoint).rstrip("/").endswith("ami-remote.work")
        assert peer.shared_secret_env_var == "AMI_REPORT_SECRET_REPORTS"


class TestMergeDefaultPeer:
    def test_merges_when_absent(self) -> None:
        cfg = ReportConfig(sender=SenderConfig(sender_id="alpha"), peers=[])
        merged = merge_default_peer(cfg)
        assert [peer.name for peer in merged.peers] == [DEFAULT_PEER_NAME]

    def test_preserves_when_present(self) -> None:
        existing = PeerEntry.model_validate(
            {
                "name": DEFAULT_PEER_NAME,
                "endpoint": "https://custom.example.com/",
                "shared_secret_env_var": "CUSTOM_SECRET",
            }
        )
        cfg = ReportConfig(sender=SenderConfig(sender_id="alpha"), peers=[existing])
        merged = merge_default_peer(cfg)
        assert len(merged.peers) == 1
        assert merged.peers[0].shared_secret_env_var == "CUSTOM_SECRET"

    def test_keeps_other_peers_when_prepending(self) -> None:
        other = PeerEntry.model_validate(
            {
                "name": "bravo",
                "endpoint": "https://bravo.example.com/",
                "shared_secret_env_var": "SECRET_BRAVO",
            }
        )
        cfg = ReportConfig(sender=SenderConfig(sender_id="alpha"), peers=[other])
        merged = merge_default_peer(cfg)
        assert [peer.name for peer in merged.peers] == [DEFAULT_PEER_NAME, "bravo"]


class TestDefaultConfig:
    def test_defaults_to_hostname_sender(self) -> None:
        cfg = default_config("my-host")
        assert cfg.sender.sender_id == "my-host"
        assert cfg.peers[0].name == DEFAULT_PEER_NAME
