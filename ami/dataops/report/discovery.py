"""Scope discovery for the ami-report sender.

Walks `AMI_ROOT` + any configured extra roots, finds every directory that
directly contains at least one allowed-extension file, and reports per-
directory counts so the operator can pick informed ("<root>/logs (401)"
rather than a blind path choice). Junk directories (`.git`, `.venv`,
`node_modules`, `projects`, …) are pruned.
"""

from __future__ import annotations

import os
from pathlib import Path

from ami.dataops.report.models import ScopeCandidate
from ami.dataops.report.scanner import SKIP_DIRS

DEFAULT_ALLOWED_SUFFIXES: tuple[str, ...] = (".log",)


def find_scope_candidates(
    root: Path,
    *,
    allowed_suffixes: tuple[str, ...] | None = None,
) -> list[ScopeCandidate]:
    """Return the list of scope candidates under `root`.

    The root itself is always reported first with its recursive file count
    (if non-zero); every descendant directory that directly contains at
    least one matching file follows, sorted by path.
    """
    suffixes = allowed_suffixes or DEFAULT_ALLOWED_SUFFIXES
    counts: dict[Path, int] = {}
    total = 0
    abs_root = root.resolve()
    for dirpath, dirnames, filenames in os.walk(abs_root, topdown=True):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        hits = sum(1 for f in filenames if f.lower().endswith(suffixes))
        if hits == 0:
            continue
        counts[Path(dirpath).resolve()] = hits
        total += hits
    result: list[ScopeCandidate] = []
    if total > 0:
        result.append(ScopeCandidate(absolute_path=abs_root, file_count=total))
    result.extend(
        ScopeCandidate(absolute_path=path, file_count=counts[path])
        for path in sorted(counts)
        if path != abs_root
    )
    return result


def candidates_for(
    ami_root: Path | None,
    extra_roots: list[Path],
    *,
    allowed_suffixes: tuple[str, ...] | None = None,
) -> list[ScopeCandidate]:
    """Collect candidates from `ami_root` and any `extra_roots`, deduped by path."""
    seen: set[Path] = set()
    result: list[ScopeCandidate] = []
    if ami_root is not None and ami_root.is_dir():
        for candidate in find_scope_candidates(
            ami_root, allowed_suffixes=allowed_suffixes
        ):
            seen.add(candidate.absolute_path)
            result.append(candidate)
    for extra in extra_roots:
        if not extra.is_dir():
            continue
        for candidate in find_scope_candidates(
            extra, allowed_suffixes=allowed_suffixes
        ):
            if candidate.absolute_path in seen:
                continue
            seen.add(candidate.absolute_path)
            result.append(candidate)
    return result
