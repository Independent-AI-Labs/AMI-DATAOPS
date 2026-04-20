"""Interactive wizard launched when `ami-report` is invoked with no args.

Drives the operator through every field `send` needs, filling gaps from
sensible defaults (hostname for sender_id, built-in `reports` peer for
destination) and prompting only for what cannot be inferred. Returns the
CLI exit code directly so the caller can `sys.exit(wizard.run())`.
"""

from __future__ import annotations

import getpass
import json
import os
import re
import socket
import sys
from collections.abc import Callable
from pathlib import Path

import uuid_utils
from pydantic import BaseModel, ConfigDict

from ami.cli_components import dialogs
from ami.dataops.intake import validation
from ami.dataops.report import manifest as manifest_mod
from ami.dataops.report.bundling import build_bundle_tarball
from ami.dataops.report.config import (
    PeerEntry,
    ReportConfig,
    SenderConfig,
    load_report_config,
)
from ami.dataops.report.defaults import merge_default_peer
from ami.dataops.report.scanner import (
    CandidateFile,
    TreeEntry,
    expand_selection,
    scan_roots,
)
from ami.dataops.report.transport import (
    AuthRejected,
    NetworkError,
    PostContext,
    ValidationRejectedByPeer,
    post_bundle,
)
from ami.dataops.report.tui import pick_peer, pick_tree

SENDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
EXIT_OK = 0
EXIT_INVALID_ARGS = 2
EXIT_NETWORK_ERROR = 3
EXIT_AUTH_REJECTED = 4
EXIT_VALIDATION_REJECTED_PEER = 5
EXIT_LOCAL_PREFLIGHT_FAILED = 6

Prompter = Callable[[str, str], str]
SecretPrompter = Callable[[str], str]
PickScopeFn = Callable[[list[str], list[str]], list[str] | None]
PickTreeFn = Callable[[list[TreeEntry]], list[TreeEntry]]
PickPeerFn = Callable[[list[PeerEntry]], PeerEntry | None]
ConfirmFn = Callable[[str], bool]
PostBundleFn = Callable[[PostContext], dict[str, object]]

ARCHIVE_PREVIEW_FILE_LIMIT = 20
BYTES_PER_KIB = 1024.0
KIB_PER_MIB = 1024.0

SCOPE_SKIP_DIRS: frozenset[str] = frozenset(
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
DEFAULT_SCOPE_ALLOWED_SUFFIXES: tuple[str, ...] = (".log",)


def _normalize_extensions(raw: str) -> frozenset[str]:
    """Parse a CSV list of extensions into a normalized frozenset.

    Accepts `"log,txt"`, `".log,.txt"`, or `"LOG, TXT"`; returns the
    lowercased, dot-prefixed frozenset (e.g. `{".log", ".txt"}`).
    """
    out: set[str] = set()
    for item in raw.split(","):
        trimmed = item.strip().lower()
        if not trimmed:
            continue
        out.add(trimmed if trimmed.startswith(".") else f".{trimmed}")
    return frozenset(out)


class ArchiveSummary(BaseModel):
    """Inputs to the archive-preview screen: compressed tar + per-file info."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    compressed_bytes: int
    uncompressed_bytes: int
    files: list[CandidateFile]


PreviewArchiveFn = Callable[[ArchiveSummary], bool]


class WizardPrimitives(BaseModel):
    """Injectable I/O primitives so tests can drive the wizard deterministically."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    prompt: Prompter
    secret_prompt: SecretPrompter
    pick_scope: PickScopeFn
    pick_tree: PickTreeFn
    pick_peer: PickPeerFn
    preview_archive: PreviewArchiveFn
    confirm: ConfirmFn
    post_bundle: PostBundleFn


def _default_prompt(question: str, default: str) -> str:
    raw = input(f"{question} [{default}]: ").strip()
    return raw or default


def _default_secret_prompt(question: str) -> str:
    return getpass.getpass(f"{question}: ")


def _default_confirm(message: str) -> bool:
    return bool(dialogs.confirm(message, title="Send report?"))


def _default_pick_scope(labels: list[str], preselected: list[str]) -> list[str] | None:
    result = dialogs.multiselect(
        labels, title="Scope: which roots to scan", preselected=preselected
    )
    if result is None:
        return None
    return [str(item) for item in result]


def _format_size(size_bytes: int) -> str:
    kib = size_bytes / BYTES_PER_KIB
    if kib < KIB_PER_MIB:
        return f"{kib:.1f} KiB"
    return f"{kib / KIB_PER_MIB:.1f} MiB"


def find_scope_candidates(
    root: Path, *, allowed_suffixes: tuple[str, ...] | None = None
) -> list[tuple[Path, int]]:
    """Walk `root`, return `[(abs_dir_path, direct_match_count), ...]`.

    The returned list starts with `root` itself and its *total recursive*
    count, followed by every descendant directory that directly contains
    at least one file with an allowed suffix, each with its direct-children
    count. Hidden/junk directories (.git, .venv, node_modules, projects,
    etc) are pruned to keep the walk fast and the picker readable.
    Operators wanting project logs pass the path via the custom-path
    prompt or `--config`'s `extra_roots`.
    """
    suffixes = allowed_suffixes or DEFAULT_SCOPE_ALLOWED_SUFFIXES
    counts: dict[Path, int] = {}
    total = 0
    abs_root = root.resolve()
    for dirpath, dirnames, filenames in os.walk(abs_root, topdown=True):
        dirnames[:] = [d for d in dirnames if d not in SCOPE_SKIP_DIRS]
        hits = sum(1 for f in filenames if f.lower().endswith(suffixes))
        if hits == 0:
            continue
        counts[Path(dirpath).resolve()] = hits
        total += hits
    result: list[tuple[Path, int]] = []
    if total > 0:
        result.append((abs_root, total))
    result.extend((path, counts[path]) for path in sorted(counts) if path != abs_root)
    return result


def render_archive_summary(summary: ArchiveSummary) -> str:
    """Format the archive-preview screen body. Pure function so tests can verify it."""
    head = (
        f"Archive:  {_format_size(summary.compressed_bytes)} compressed  /  "
        f"{_format_size(summary.uncompressed_bytes)} uncompressed\n"
        f"Files:    {len(summary.files)}\n\n"
    )
    shown = summary.files[:ARCHIVE_PREVIEW_FILE_LIMIT]
    lines = [
        f"  {candidate.relative_path:<60}{_format_size(candidate.size_bytes)}"
        for candidate in shown
    ]
    extras = len(summary.files) - len(shown)
    if extras > 0:
        lines.append(f"  (+{extras} more)")
    return head + "\n".join(lines) + "\n\nReview complete?"


def _default_preview_archive(summary: ArchiveSummary) -> bool:
    return bool(
        dialogs.confirm(render_archive_summary(summary), title="Archive preview")
    )


def default_primitives() -> WizardPrimitives:
    return WizardPrimitives(
        prompt=_default_prompt,
        secret_prompt=_default_secret_prompt,
        pick_scope=_default_pick_scope,
        pick_tree=pick_tree,
        pick_peer=pick_peer,
        preview_archive=_default_preview_archive,
        confirm=_default_confirm,
        post_bundle=post_bundle,
    )


def _resolve_sender_id(cfg: ReportConfig | None, prompt: Prompter) -> str:
    default = (
        cfg.sender.sender_id
        if cfg is not None and cfg.sender.sender_id
        else socket.gethostname() or "anonymous"
    )
    while True:
        value = prompt("Sender ID", default)
        if SENDER_ID_PATTERN.match(value):
            return value
        print(
            "error: sender_id must match ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$",
            file=sys.stderr,
        )


def _collect_scope_labels(
    ami_root: Path,
    extras: list[Path],
    allowed_suffixes: tuple[str, ...] | None = None,
) -> tuple[list[str], dict[str, Path]]:
    labels: list[str] = []
    by_label: dict[str, Path] = {}
    if ami_root and ami_root.is_dir():
        for path, count in find_scope_candidates(
            ami_root, allowed_suffixes=allowed_suffixes
        ):
            label = f"{path} ({count})"
            labels.append(label)
            by_label[label] = path
    for extra in extras:
        if not extra.is_dir():
            continue
        for path, count in find_scope_candidates(
            extra, allowed_suffixes=allowed_suffixes
        ):
            label = f"{path} ({count})"
            if label in by_label:
                continue
            labels.append(label)
            by_label[label] = path
    return labels, by_label


def _prompt_extra_paths(prompt: Prompter, existing: list[Path]) -> list[Path]:
    roots = list(existing)
    while True:
        extra_input = prompt("Add custom path (blank to finish)", "")
        if not extra_input:
            return roots
        extra_path = Path(extra_input).expanduser().absolute()
        if not extra_path.exists():
            print(f"warning: {extra_path} does not exist; skipped", file=sys.stderr)
            continue
        if extra_path not in roots:
            roots.append(extra_path)


def _resolve_scope(
    cfg: ReportConfig,
    prompt: Prompter,
    pick_scope: PickScopeFn,
    allowed_suffixes: tuple[str, ...] | None = None,
) -> list[Path]:
    ami_root = Path(os.environ.get("AMI_ROOT", "")).expanduser()
    labels, by_label = _collect_scope_labels(
        ami_root, cfg.sender.extra_roots, allowed_suffixes=allowed_suffixes
    )
    roots: list[Path] = []
    if labels:
        picked = pick_scope(labels, [labels[0]])
        if picked is None:
            return []
        roots = [by_label[label] for label in picked if label in by_label]
    return _prompt_extra_paths(prompt, roots)


def _load_or_default_config(
    config_path: Path | None, sender_id_fallback: str
) -> ReportConfig:
    if config_path is not None and config_path.is_file():
        cfg = load_report_config(config_path)
    else:
        cfg = ReportConfig(sender=SenderConfig(sender_id=sender_id_fallback), peers=[])
    return merge_default_peer(cfg)


def _ensure_peer_credentials(
    peer: PeerEntry, secret_prompt: SecretPrompter
) -> tuple[str, str]:
    token_env = f"AMI_REPORT_TOKENS__{peer.name.upper()}"
    secret_value = os.environ.get(peer.shared_secret_env_var)
    token_value = os.environ.get(token_env)
    if not secret_value:
        secret_value = secret_prompt(
            f"Secret for {peer.name} ({peer.shared_secret_env_var})"
        )
        os.environ[peer.shared_secret_env_var] = secret_value
    if not token_value:
        token_value = secret_prompt(f"Bearer token for {peer.name} ({token_env})")
        os.environ[token_env] = token_value
    return secret_value, token_value


def _common_source_root(files: list[CandidateFile]) -> Path:
    paths = [c.absolute_path for c in files]
    root = paths[0].parent
    while not all(_is_under(root, p) for p in paths):
        new = root.parent
        if new == root:
            break
        root = new
    return root


def _is_under(root: Path, path: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


class SendRequest(BaseModel):
    """Bundled inputs to `_send` so the signature stays under the arg cap."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    sender_id: str
    peer: PeerEntry
    expanded: list[CandidateFile]
    primitives: WizardPrimitives
    secret: str
    token: str


def _dispatch_post(
    primitives: WizardPrimitives, ctx: PostContext
) -> tuple[int, dict[str, object] | None]:
    try:
        return EXIT_OK, primitives.post_bundle(ctx)
    except AuthRejected as exc:
        print(f"auth rejected: {exc}", file=sys.stderr)
        return EXIT_AUTH_REJECTED, None
    except ValidationRejectedByPeer as exc:
        print(f"validation reject {exc.reason_code}: {exc.detail}", file=sys.stderr)
        return EXIT_VALIDATION_REJECTED_PEER, None
    except NetworkError as exc:
        print(f"network error: {exc}", file=sys.stderr)
        return EXIT_NETWORK_ERROR, None


def _send(request: SendRequest) -> int:
    sender_id = request.sender_id
    peer = request.peer
    expanded = request.expanded
    primitives = request.primitives
    secret = request.secret
    token = request.token
    source_root = _common_source_root(expanded)
    try:
        for candidate in expanded:
            validation.probe_text_content(candidate.absolute_path)
    except validation.ValidationRejected as exc:
        print(f"local pre-flight failed: {exc}", file=sys.stderr)
        return EXIT_LOCAL_PREFLIGHT_FAILED
    bundle_id = str(uuid_utils.uuid7())
    manifest = manifest_mod.build_manifest(
        sender_id=sender_id,
        source_root=source_root,
        files=[c.absolute_path for c in expanded],
        bundle_id=bundle_id,
    )
    manifest_bytes = manifest_mod.canonical_manifest_bytes(manifest)
    signature = manifest_mod.sign_manifest(manifest_bytes, secret)
    bundle_bytes = build_bundle_tarball(manifest, source_root)
    archive_summary = ArchiveSummary(
        compressed_bytes=len(bundle_bytes),
        uncompressed_bytes=sum(c.size_bytes for c in expanded),
        files=expanded,
    )
    if not primitives.preview_archive(archive_summary):
        return EXIT_OK
    confirm_body = (
        f"Destination: {peer.name} ({peer.endpoint})\n"
        f"Bundle id:   {bundle_id}\n"
        f"Files:       {len(expanded)}\n"
    )
    if not primitives.confirm(confirm_body):
        return EXIT_OK
    ctx = PostContext(
        endpoint=f"{peer.endpoint}v1/bundles",
        bearer_token=token,
        manifest=manifest,
        manifest_bytes=manifest_bytes,
        signature=signature,
        bundle_bytes=bundle_bytes,
    )
    exit_code, receipt = _dispatch_post(primitives, ctx)
    if receipt is None:
        return exit_code
    print(json.dumps(receipt, indent=2))
    return EXIT_OK


def run(
    config_path: Path | None = None,
    primitives: WizardPrimitives | None = None,
    extensions: frozenset[str] | None = None,
) -> int:
    """Run the interactive wizard. Returns the CLI exit code.

    `extensions` (when set) overrides the built-in `.log`-only allowlist
    for this run, including the scope picker's discovery suffixes and
    the per-file pre-flight check. Supplied by `--extensions` on the CLI.
    """
    prim = primitives or default_primitives()
    hostname = socket.gethostname() or "anonymous"
    initial_cfg = _load_or_default_config(config_path, hostname)
    sender_id = _resolve_sender_id(initial_cfg, prim.prompt)
    cfg = ReportConfig(
        sender=initial_cfg.sender.model_copy(update={"sender_id": sender_id}),
        peers=initial_cfg.peers,
    )
    suffixes = tuple(sorted(extensions)) if extensions is not None else None
    roots = _resolve_scope(cfg, prim.prompt, prim.pick_scope, suffixes)
    if not roots:
        print("no scan roots chosen; nothing to report", file=sys.stderr)
        return EXIT_OK
    entries = scan_roots(roots, allowed_extensions=extensions)
    if not entries:
        print("no candidate files found under the chosen roots", file=sys.stderr)
        return EXIT_OK
    selected = prim.pick_tree(entries)
    if not selected:
        return EXIT_OK
    expanded = expand_selection(selected, entries)
    if not expanded:
        print("selection expanded to zero files", file=sys.stderr)
        return EXIT_OK
    peer = prim.pick_peer(cfg.peers)
    if peer is None:
        return EXIT_OK
    secret, token = _ensure_peer_credentials(peer, prim.secret_prompt)
    return _send(
        SendRequest(
            sender_id=sender_id,
            peer=peer,
            expanded=expanded,
            primitives=prim,
            secret=secret,
            token=token,
        )
    )
