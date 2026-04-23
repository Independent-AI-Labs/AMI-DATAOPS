"""Built-in peer defaults for ami-report.

Ships a single known destination — the Independent AI Labs public reports
endpoint — so a fresh install can dispatch a bundle without any config file.
Operators who declare `dataops_report_peers` in their config still keep every
peer they list; the default is merged (never overwritten) when absent.

Env-var contract for the default peer:
    AMI_REPORT_SECRET_REPORTS   -- HMAC shared secret
    AMI_REPORT_TOKENS__REPORTS  -- bearer token
Both are prompted interactively (getpass) by `TerminalOperator` when unset.
"""

from __future__ import annotations

from ami.dataops.report.config import PeerEntry, ReportConfig, SenderConfig

DEFAULT_PEER_NAME = "reports"
DEFAULT_PEER_ENDPOINT = "https://reports.ami-remote.work/"
DEFAULT_PEER_SECRET_ENV = "AMI_REPORT_SECRET_REPORTS"


def build_default_peer() -> PeerEntry:
    """Return the built-in `reports` peer entry."""
    return PeerEntry.model_validate(
        {
            "name": DEFAULT_PEER_NAME,
            "endpoint": DEFAULT_PEER_ENDPOINT,
            "shared_secret_env_var": DEFAULT_PEER_SECRET_ENV,
        }
    )


def merge_default_peer(config: ReportConfig) -> ReportConfig:
    """Prepend the built-in `reports` peer when the config does not name it."""
    names = {peer.name for peer in config.peers}
    if DEFAULT_PEER_NAME in names:
        return config
    return ReportConfig(
        sender=config.sender, peers=[build_default_peer(), *config.peers]
    )


def default_config(sender_id: str) -> ReportConfig:
    """Return a minimal `ReportConfig` built from the built-in peer + sender_id."""
    return ReportConfig(
        sender=SenderConfig(sender_id=sender_id),
        peers=[build_default_peer()],
    )
