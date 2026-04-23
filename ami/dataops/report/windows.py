"""Time-window catalog + tally computation for the ami-report sender.

Defines the seven fixed mtime buckets (all / 1m / 5m / 15m / 1h / 8h / 1d),
provides `by_key` + `normalize_key` parsing for the --since CLI flag, and
computes per-window file counts over an already-scanned tree.
"""

from __future__ import annotations

from datetime import timedelta

from ami.dataops.report.models import TimeWindow, TimeWindowTally, WindowKey
from ami.dataops.report.scanner import CandidateFile, TreeEntry

WINDOW_CATALOG: tuple[TimeWindow, ...] = (
    TimeWindow(key="all", label="All time", delta=None),
    TimeWindow(key="1m", label="Last 1 minute", delta=timedelta(minutes=1)),
    TimeWindow(key="5m", label="Last 5 minutes", delta=timedelta(minutes=5)),
    TimeWindow(key="15m", label="Last 15 minutes", delta=timedelta(minutes=15)),
    TimeWindow(key="1h", label="Last 1 hour", delta=timedelta(hours=1)),
    TimeWindow(key="8h", label="Last 8 hours", delta=timedelta(hours=8)),
    TimeWindow(key="1d", label="Last 1 day", delta=timedelta(days=1)),
)
VALID_KEYS: frozenset[str] = frozenset(w.key for w in WINDOW_CATALOG)


def by_key(key: WindowKey) -> TimeWindow:
    """Return the `TimeWindow` whose key matches `key` or raise KeyError."""
    for window in WINDOW_CATALOG:
        if window.key == key:
            return window
    msg = f"no TimeWindow with key {key!r}"
    raise KeyError(msg)


def normalize_key(raw: str | None) -> WindowKey | None:
    """Parse a --since value: None → None, valid key → key, else raise."""
    if raw is None:
        return None
    trimmed = raw.strip().lower()
    if not trimmed:
        return None
    for window in WINDOW_CATALOG:
        if window.key == trimmed:
            return window.key
    valid = ", ".join(sorted(VALID_KEYS))
    msg = f"unknown --since value {raw!r}; expected one of: {valid}"
    raise ValueError(msg)


def tallies_for(entries: list[TreeEntry], now_epoch: float) -> list[TimeWindowTally]:
    """Return one `TimeWindowTally` per catalog entry with its file count."""
    files = [e for e in entries if isinstance(e, CandidateFile) and e.toggleable]
    results: list[TimeWindowTally] = []
    for window in WINDOW_CATALOG:
        cutoff = window.cutoff(now_epoch)
        if cutoff is None:
            count = len(files)
        else:
            count = sum(1 for f in files if f.mtime_epoch >= cutoff)
        results.append(TimeWindowTally(window=window, file_count=count))
    return results
