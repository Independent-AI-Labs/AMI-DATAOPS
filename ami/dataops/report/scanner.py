"""Walk candidate log roots and emit a checkbox-friendly tree inventory.

The TUI renders both folders and files as individually-selectable
checkbox rows. A folder row, when toggled, means "include every file
under this folder" — expanded at manifest-build time, not at scan time,
so the operator sees compact "3 files" summaries instead of hundreds
of ticked boxes.

Each entry carries its `depth` in the tree so the TUI can indent the
label without a second walk. Traversal is pre-order (folder first, then
its children) and sorted alphabetically inside every directory so the
display is deterministic across runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from ami.dataops.intake import validation

PreflightStatus = Literal["ok", "ext_not_allowed", "not_text", "file_too_large"]

SKIP_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        ".venv",
        ".venvs",
        ".tox",
        ".boot-linux",
        ".gcloud",
        ".runtime",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "node_modules",
        "__pycache__",
        "build",
        "dist",
        "tmp",
        "projects",
    }
)


class CandidateFile(BaseModel):
    """One file the TUI may offer for selection."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    absolute_path: Path
    relative_path: str
    size_bytes: int
    preflight: PreflightStatus
    reject_detail: str | None = None
    depth: int = 0
    mtime_epoch: float = 0.0

    @property
    def toggleable(self) -> bool:
        return self.preflight == "ok"


class FolderEntry(BaseModel):
    """One directory row the TUI may offer as a bulk-select checkbox."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    absolute_path: Path
    relative_path: str
    descendant_file_count: int
    toggleable_descendant_count: int
    depth: int = 0

    @property
    def toggleable(self) -> bool:
        return self.toggleable_descendant_count > 0


TreeEntry = CandidateFile | FolderEntry


def _preflight_one(
    path: Path,
    *,
    max_file_bytes: int,
    allowed_extensions: frozenset[str] | None = None,
) -> tuple[PreflightStatus, str | None]:
    try:
        validation.validate_extension(path.name, allowed=allowed_extensions)
    except validation.ValidationRejected as exc:
        return "ext_not_allowed", exc.detail
    try:
        validation.validate_file_size(path.stat().st_size, max_file_bytes)
    except validation.ValidationRejected as exc:
        return "file_too_large", exc.detail
    try:
        validation.probe_text_content(path)
    except validation.ValidationRejected as exc:
        return "not_text", exc.detail
    return "ok", None


class _ScanOptions(BaseModel):
    """Grouped scan knobs passed through the recursive walker."""

    max_file_bytes: int
    rel_base: Path
    allowed_extensions: frozenset[str] | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


def _build_candidate(
    child: Path, depth: int, opts: _ScanOptions
) -> CandidateFile | None:
    """Return a CandidateFile or None when the file should be omitted entirely.

    Extension mismatches are dropped outright — with a whole-workspace
    scope the tree would otherwise render hundreds of .py/.md/.yaml rows.
    Size / text failures still surface as disabled rows because they
    apply to files the operator explicitly selected by extension.
    """
    preflight, detail = _preflight_one(
        child,
        max_file_bytes=opts.max_file_bytes,
        allowed_extensions=opts.allowed_extensions,
    )
    if preflight == "ext_not_allowed":
        return None
    stat = child.stat()
    return CandidateFile(
        absolute_path=child,
        relative_path=child.relative_to(opts.rel_base).as_posix(),
        size_bytes=stat.st_size,
        preflight=preflight,
        reject_detail=detail,
        depth=depth + 1,
        mtime_epoch=stat.st_mtime,
    )


class _ChildAccum(BaseModel):
    """Running counts produced while iterating a directory's children."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    files_here: list[CandidateFile] = []
    nested: list[TreeEntry] = []
    descendant_files: int = 0
    toggleable_descendants: int = 0


def _usable_child(child: Path) -> bool:
    if child.is_symlink() or not (child.is_dir() or child.is_file()):
        return False
    return not (child.is_dir() and child.name in SKIP_DIRS)


def _absorb_child(
    root: Path, child: Path, depth: int, opts: _ScanOptions, acc: _ChildAccum
) -> None:
    if child.is_dir():
        sub = _scan_directory(root, child, depth + 1, opts)
        acc.nested.extend(sub)
        for item in sub:
            if isinstance(item, CandidateFile):
                acc.descendant_files += 1
                if item.toggleable:
                    acc.toggleable_descendants += 1
        return
    candidate = _build_candidate(child, depth, opts)
    if candidate is None:
        return
    acc.files_here.append(candidate)
    acc.descendant_files += 1
    if candidate.toggleable:
        acc.toggleable_descendants += 1


def _scan_directory(
    root: Path,
    current: Path,
    depth: int,
    opts: _ScanOptions,
) -> list[TreeEntry]:
    entries: list[TreeEntry] = []
    try:
        children = sorted(current.iterdir()) if current.is_dir() else []
    except PermissionError:
        return entries
    acc = _ChildAccum()
    for child in children:
        if not _usable_child(child):
            continue
        _absorb_child(root, child, depth, opts, acc)
    files_here = acc.files_here
    nested = acc.nested
    descendant_files = acc.descendant_files
    toggleable_descendants = acc.toggleable_descendants
    if current != root:
        rel = current.relative_to(opts.rel_base).as_posix()
        entries.append(
            FolderEntry(
                absolute_path=current,
                relative_path=rel,
                descendant_file_count=descendant_files,
                toggleable_descendant_count=toggleable_descendants,
                depth=depth,
            )
        )
    entries.extend(files_here)
    entries.extend(nested)
    return entries


def scan_roots(
    roots: list[Path],
    *,
    max_file_bytes: int = validation.DEFAULT_MAX_FILE_BYTES,
    allowed_extensions: frozenset[str] | None = None,
) -> list[TreeEntry]:
    """Walk every root recursively and return a pre-order tree of entries.

    `roots` may contain directories or individual files. Missing roots are
    skipped with no error; the caller is responsible for reporting them.
    Duplicates across roots are deduplicated by absolute path.
    """
    entries: list[TreeEntry] = []
    seen: set[Path] = set()
    for raw_root in roots:
        root = raw_root.expanduser().absolute()
        if not root.exists():
            continue
        if root.is_file():
            if root in seen:
                continue
            seen.add(root)
            preflight, detail = _preflight_one(
                root,
                max_file_bytes=max_file_bytes,
                allowed_extensions=allowed_extensions,
            )
            stat = root.stat()
            entries.append(
                CandidateFile(
                    absolute_path=root,
                    relative_path=root.name,
                    size_bytes=stat.st_size,
                    preflight=preflight,
                    reject_detail=detail,
                    depth=0,
                    mtime_epoch=stat.st_mtime,
                )
            )
            continue
        opts = _ScanOptions(
            max_file_bytes=max_file_bytes,
            rel_base=root,
            allowed_extensions=allowed_extensions,
        )
        sub_entries = _scan_directory(root, root, 0, opts)
        deduped: list[TreeEntry] = []
        for item in sub_entries:
            if isinstance(item, CandidateFile):
                if item.absolute_path in seen:
                    continue
                seen.add(item.absolute_path)
            deduped.append(item)
        counted = _count_descendants(deduped)
        root_entry = FolderEntry(
            absolute_path=root,
            relative_path=root.name or str(root),
            descendant_file_count=counted[0],
            toggleable_descendant_count=counted[1],
            depth=0,
        )
        entries.append(root_entry)
        entries.extend(deduped)
    return entries


def _count_descendants(entries: list[TreeEntry]) -> tuple[int, int]:
    total = 0
    toggleable = 0
    for entry in entries:
        if isinstance(entry, CandidateFile):
            total += 1
            if entry.toggleable:
                toggleable += 1
    return total, toggleable


def files_only(entries: list[TreeEntry]) -> list[CandidateFile]:
    """Return only the CandidateFile entries (flattened across the tree)."""
    return [e for e in entries if isinstance(e, CandidateFile)]


def expand_selection(
    selected: list[TreeEntry], all_entries: list[TreeEntry]
) -> list[CandidateFile]:
    """Expand folder entries to every toggleable descendant file.

    Files in `selected` are passed through (as long as they are toggleable).
    Folders in `selected` are replaced with every toggleable CandidateFile
    whose `relative_path` starts with the folder's `relative_path + "/"`.
    Duplicates are removed; order follows `all_entries`.
    """
    file_entries = files_only(all_entries)
    selected_abs: set[Path] = {
        entry.absolute_path for entry in selected if isinstance(entry, CandidateFile)
    }
    selected_folders: list[FolderEntry] = [
        entry for entry in selected if isinstance(entry, FolderEntry)
    ]
    result: list[CandidateFile] = []
    already: set[Path] = set()
    for candidate in file_entries:
        if not candidate.toggleable:
            continue
        hit = candidate.absolute_path in selected_abs
        if not hit and any(
            _file_is_under_folder(candidate, folder) for folder in selected_folders
        ):
            hit = True
        if hit and candidate.absolute_path not in already:
            result.append(candidate)
            already.add(candidate.absolute_path)
    return result


def _file_is_under_folder(file_entry: CandidateFile, folder: FolderEntry) -> bool:
    try:
        file_entry.absolute_path.relative_to(folder.absolute_path)
    except ValueError:
        return False
    return file_entry.absolute_path != folder.absolute_path


def filter_by_window(
    entries: list[TreeEntry], since_epoch: float | None
) -> list[TreeEntry]:
    """Drop CandidateFile entries older than `since_epoch` and prune empty folders.

    `since_epoch=None` is a pass-through (no filter). Folder entries whose
    subtree holds zero qualifying files are removed; remaining folders have
    their descendant counts recomputed so the TUI labels stay accurate.
    """
    if since_epoch is None:
        return list(entries)
    kept_files: dict[Path, CandidateFile] = {
        entry.absolute_path: entry
        for entry in entries
        if isinstance(entry, CandidateFile) and entry.mtime_epoch >= since_epoch
    }
    output: list[TreeEntry] = []
    for entry in entries:
        if isinstance(entry, CandidateFile):
            if entry.absolute_path in kept_files:
                output.append(entry)
            continue
        descendants = [
            f
            for f in kept_files.values()
            if _file_is_under_folder(f, entry) or f.absolute_path == entry.absolute_path
        ]
        if not descendants:
            continue
        toggleable = sum(1 for f in descendants if f.toggleable)
        output.append(
            entry.model_copy(
                update={
                    "descendant_file_count": len(descendants),
                    "toggleable_descendant_count": toggleable,
                }
            )
        )
    return output
