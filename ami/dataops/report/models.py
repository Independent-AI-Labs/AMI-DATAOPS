"""Domain models for the ami-report sender pipeline.

Every cross-stage value in the sender flow is a Pydantic model defined
here so the pipeline, operator implementations, and tests all speak the
same typed vocabulary. No tuples, no dicts-of-object, no stringly-typed
IDs in motion between stages.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ami.dataops.report.config import PeerEntry
from ami.dataops.report.manifest import SenderManifest
from ami.dataops.report.scanner import CandidateFile

WindowKey = Literal["all", "1m", "5m", "15m", "1h", "8h", "1d"]


class ScopeCandidate(BaseModel):
    """A directory the operator may choose to scan for candidate files."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True, frozen=True)

    absolute_path: Path
    file_count: int = Field(ge=0)

    @property
    def label(self) -> str:
        """Human-readable `<path> (<count>)` string for picker rendering."""
        return f"{self.absolute_path} ({self.file_count})"


class Scope(BaseModel):
    """Resolved set of roots the scanner will walk."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    roots: list[Path] = Field(default_factory=list)


class TimeWindow(BaseModel):
    """One of the seven mtime buckets offered to the operator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: WindowKey
    label: str
    delta: timedelta | None

    def cutoff(self, now_epoch: float) -> float | None:
        """Return `now_epoch - delta` seconds, or None for 'all time'."""
        if self.delta is None:
            return None
        return now_epoch - self.delta.total_seconds()


class TimeWindowTally(BaseModel):
    """A window enriched with the count of qualifying files."""

    model_config = ConfigDict(extra="forbid")

    window: TimeWindow
    file_count: int = Field(ge=0)


class PeerCredentials(BaseModel):
    """Secrets required to sign + post to a specific peer."""

    model_config = ConfigDict(extra="forbid")

    shared_secret: str = Field(min_length=1)
    bearer_token: str = Field(min_length=1)


class ArchiveSummary(BaseModel):
    """Inputs to the archive-preview screen: compressed tar + per-file info."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    compressed_bytes: int = Field(ge=0)
    uncompressed_bytes: int = Field(ge=0)
    files: list[CandidateFile]


class SendPlan(BaseModel):
    """Everything collected interactively, before bundle build + post."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    sender_id: str = Field(min_length=1)
    peer: PeerEntry
    source_root: Path
    files: list[CandidateFile]


class SignedBundle(BaseModel):
    """Output of archive.build_signed_bundle — the signed + tarballed artifact."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    manifest: SenderManifest
    canonical_bytes: bytes
    signature: str
    tar_bytes: bytes
    archive_summary: ArchiveSummary


class CIDefaults(BaseModel):
    """Parsed `--defaults FILE` YAML for non-interactive runs."""

    model_config = ConfigDict(extra="forbid")

    peer: str = Field(min_length=1)
    files: list[str] = Field(default_factory=list)
    since: WindowKey | None = None


class RunRequest(BaseModel):
    """Top-level arguments assembled by `cli.main` and fed to `pipeline.run`."""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    config_path: Path | None = None
    extensions: frozenset[str] | None = None
    since_key: WindowKey | None = None
    dry_run: bool = False
    ci_defaults_path: Path | None = None
