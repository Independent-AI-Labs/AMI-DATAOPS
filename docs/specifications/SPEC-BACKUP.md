# Backup & Sync: Technical Specification

**Date:** 2026-03-23
**Updated:** 2026-04-17
**Status:** ACTIVE
**Type:** Specification
**Requirements:** [REQ-BACKUP.md](../requirements/REQ-BACKUP.md)

## Implementation Status (2026-04-17)

| Area | Status | Details |
|------|--------|---------|
| Google Drive backup | DONE | tar.zst archives uploaded to Google Drive. Flags: `--keep-local`, `--include-all`, `--auth-mode`, `--verbose`. |
| Google Drive restore | DONE | Download by file ID (`--file-id`), revision (`--revision`), local path (`--local-path`/`--latest-local`). Interactive wizard (`--interactive`). |
| Local secondary copy | DONE | `shutil.copy2()` to `AMI_BACKUP_MOUNT` or `/media/backup`. |
| GDrive auth | DONE | 3 methods: OAuth (pickle token), service account impersonation (gcloud ADC), service account key file. Selectable via `GDRIVE_AUTH_METHOD`. |
| rsync backend | NOT BUILT | `--mode`/`--target` CLI flags not implemented. |
| Snapshot versioning | NOT BUILT | `--link-dest` hard-link dedup not implemented. |
| rsyncd daemon | NOT BUILT | Optional remote access not implemented. |
| Multi-target | NOT BUILT | Simultaneous local + remote not implemented. |

### Known Issues

1. **rsync is a system package**: Cannot be bootstrapped to `.boot-linux/` like uv/python. Must be a system dependency installed via `pre-req.sh`. REQ-BAK-030 updated accordingly.
2. **No OpenBao integration**: REQ-IAM FR-11 specifies backup credentials from OpenBao at runtime. GDrive auth currently uses `.env` + gcloud CLI.

### Current Code Map

All paths are under `projects/AMI-DATAOPS/ami/dataops/backup/` (AMI-DATAOPS repo, installed editable into AMI-AGENTS' venv).

| Path | Purpose |
|------|---------|
| `create/main.py` | Backup entry point |
| `create/cli.py` | CLI argument parsing (GDrive only) |
| `create/archiver.py` | tar.zst archive creation |
| `create/uploader.py` | Google Drive upload |
| `create/secondary.py` | Secondary copy to mount point |
| `restore/main.py` | Restore entry point |
| `restore/cli.py` | Restore CLI (GDrive + local) |
| `restore/drive_client.py` | Google Drive download |
| `restore/local_client.py` | Local file restore |
| `restore/wizard.py` | Interactive restore wizard |
| `common/auth.py` | 3-method Google auth |
| `extension.manifest.yaml` (at DATAOPS repo root) | Registers `ami-backup` + `ami-restore` extensions |

---

## Conventions

- RFC 2119 keywords: **MUST**, **SHOULD**, **MAY**
- Configuration examples use actual deployment values where known; placeholders use `<angle-brackets>`
- Shell code blocks assume bash. Python code blocks assume 3.11
- All paths are relative to `PROJECT_ROOT` (`/home/ami/AMI-AGENTS`) unless stated otherwise
- rsync target syntax follows rsync(1) conventions: local paths, `user@host:path` (SSH), `rsync://host:port/module` (daemon)

---

## 1. Architecture

### 1.1 Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        ami-backup CLI                           │
│  --mode rsync | archive | gdrive    --target <destination>      │
└────────────┬──────────────────┬──────────────────┬──────────────┘
             │                  │                  │
     ┌───────▼───────┐  ┌──────▼──────┐  ┌───────▼───────┐
     │ RsyncBackup   │  │ ArchiveOnly │  │ GDriveBackup  │
     │ Service       │  │ Service     │  │ Service       │
     │               │  │             │  │ (existing)    │
     │ rsync -a      │  │ tar.zst     │  │ tar.zst →     │
     │ --link-dest   │  │ to local    │  │ Google Drive  │
     │ --delete      │  │ drive       │  │               │
     └───────┬───────┘  └──────┬──────┘  └───────┬───────┘
             │                  │                  │
     ┌───────▼──────────────────▼──────────────────▼──────────────┐
     │                    Backup Targets                          │
     │  Local: /media/backup     SSH: user@host:/backup           │
     │  Daemon: rsync://nas:8873/backup                           │
     │  GDrive: folder_id (existing)                              │
     └────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                       ami-restore CLI                           │
│  --latest-snapshot | --snapshot <name> | --local-path | --file-id│
└────────────┬──────────────────┬──────────────────┬──────────────┘
             │                  │                  │
     ┌───────▼───────┐  ┌──────▼──────┐  ┌───────▼───────┐
     │ Rsync         │  │ Tar.zst     │  │ Google Drive  │
     │ Snapshot      │  │ Local       │  │ Download +    │
     │ Restore       │  │ Restore     │  │ Extract       │
     │ (new)         │  │ (existing)  │  │ (existing)    │
     └───────────────┘  └─────────────┘  └───────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     rsyncd-venv daemon                          │
│  Port 8873 · Non-privileged · Authenticated                    │
│  Exposes /media/backup as read-only module                     │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Backup Modes

| Mode | Target Type | Versioning | Auth Required | Archive Format |
|------|-------------|------------|---------------|----------------|
| `rsync` | Local path, SSH, rsync daemon | Hard-link snapshots | SSH key or rsync secrets | Directory tree (no archive) |
| `archive` | Local path | Timestamped `.tar.zst` files | None | `.tar.zst` |
| `gdrive` | Google Drive | Drive revision history | OAuth / Impersonation / Key | `.tar.zst` |

### 1.3 Snapshot Directory Layout

```
<backup_mount>/
├── latest -> 2026-03-23-132500/        # Symlink to newest (REQ-BAK-012)
├── 2026-03-23-132500/                  # Newest snapshot
│   └── AMI-AGENTS/
│       ├── ami/
│       ├── projects/
│       ├── pyproject.toml
│       └── ...
├── 2026-03-22-090000/                  # Previous (unchanged files hard-linked)
│   └── AMI-AGENTS/
│       └── ...
└── 2026-03-21-180000/                  # Oldest
    └── AMI-AGENTS/
        └── ...
```

---

## 2. Configuration

### 2.1 Environment Variables

| Variable | Default | Purpose | Modes |
|----------|---------|---------|-------|
| `BACKUP_MODE` | Auto-detect (§2.2) | Backup mode: `rsync`, `archive`, `gdrive` | All |
| `AMI_BACKUP_MOUNT` | `/media/backup` | Local backup target path | rsync, archive |
| `AMI_BACKUP_REMOTE` | *(none)* | Network target (see §2.3) | rsync |
| `BACKUP_MAX_SNAPSHOTS` | `10` | Maximum snapshots before rotation | rsync |
| `GDRIVE_AUTH_METHOD` | `oauth` | Google Drive auth method | gdrive |
| `GDRIVE_SERVICE_ACCOUNT_EMAIL` | *(none)* | Service account for impersonation | gdrive |
| `GDRIVE_CREDENTIALS_FILE` | *(none)* | Service account key path | gdrive |
| `GDRIVE_BACKUP_FOLDER_ID` | *(none)* | Drive folder ID | gdrive |
| `RSYNCD_PORT` | `8873` | rsync daemon listen port | daemon |
| `RSYNCD_SECRETS_FILE` | `.boot-linux/rsync/etc/rsyncd.secrets` | Daemon auth secrets | daemon |

### 2.2 Mode Auto-Detection (REQ-BAK-040)

When `BACKUP_MODE` is not set, the system MUST infer the mode:

```python
mode = os.getenv("BACKUP_MODE")
if not mode:
    if os.getenv("GDRIVE_BACKUP_FOLDER_ID"):
        mode = "gdrive"
    else:
        mode = "rsync"
```

### 2.3 Target Syntax (REQ-BAK-001, REQ-BAK-002, REQ-BAK-003)

The `AMI_BACKUP_REMOTE` variable and `--target` CLI flag accept three target formats:

| Format | Example | Transport | Auth |
|--------|---------|-----------|------|
| Local path | `/media/backup` | Filesystem | None (permissions) |
| SSH | `ami@nas:/backup/snapshots` | SSH | SSH key or password |
| rsync daemon | `rsync://nas:8873/backup` | rsync protocol | Secrets file |

Target type detection:

```python
def detect_target_type(target: str) -> TargetType:
    if target.startswith("rsync://"):
        return TargetType.RSYNC_DAEMON
    elif ":" in target and not target.startswith("/"):
        return TargetType.SSH
    else:
        return TargetType.LOCAL
```

### 2.4 BackupConfig Changes

**File:** `ami/dataops/backup/backup_config.py`

New fields on `BackupConfig`:

```python
class BackupConfig:
    # Existing fields...

    # New fields (REQ-BAK-040 through REQ-BAK-044)
    backup_mode: BackupMode          # rsync | archive | gdrive
    backup_mount: Path               # local target (default: /media/backup)
    backup_remote: str | None        # network target (SSH or rsync://)
    max_snapshots: int               # rotation limit (default: 10)
```

When `backup_mode` is `rsync` or `archive`, the `load()` method MUST skip all Google Drive auth configuration (`_configure_auth_method()` SHALL NOT be called). (REQ-BAK-043)

---

## 3. rsync Backup: Create Flow

### 3.1 Module Structure

**New file:** `ami/dataops/backup/create/rsync_backup.py`

```python
# Public API
async def run_rsync_backup(
    source_dir: Path,
    targets: list[BackupTarget],
    exclusion_patterns: list[str],
    max_snapshots: int = 10,
) -> list[SnapshotResult]:
    """Create rsync --link-dest snapshot on each target. (REQ-BAK-010, REQ-BAK-011)"""

# Internal
def _build_rsync_command(
    source_dir: Path,
    dest_dir: str,             # local path or remote target
    link_dest: str | None,     # --link-dest path (previous snapshot)
    exclusion_patterns: list[str],
    dry_run: bool = False,
) -> list[str]:
    """Build rsync command. (REQ-BAK-060, REQ-BAK-062)"""

async def _execute_rsync(cmd: list[str]) -> RsyncStats:
    """Run rsync subprocess, parse progress, raise RsyncError on failure. (REQ-BAK-050)"""

def _generate_snapshot_name() -> str:
    """YYYY-MM-DD-HHMMSS timestamp. (REQ-BAK-015)"""

async def update_latest_symlink(target_base: str, snapshot_name: str) -> None:
    """Atomic symlink update via tmp + rename. (REQ-BAK-012)"""

async def rotate_snapshots(target_base: str, max_snapshots: int) -> int:
    """Delete oldest snapshots beyond limit. Returns count deleted. (REQ-BAK-013, REQ-BAK-014)"""

def _find_existing_snapshots(target_base: Path) -> list[Path]:
    """Regex match YYYY-MM-DD-HHMMSS dirs, sorted newest first."""

def _resolve_link_dest(target_base: Path) -> Path | None:
    """Resolve 'latest' symlink for --link-dest. Returns None if no previous snapshot."""
```

### 3.2 rsync Command Construction

Base command for local target:

```bash
rsync -a --delete \
    --info=progress2 --no-inc-recursive \
    --exclude='.git/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='.env' \
    # ... all DEFAULT_EXCLUSION_PATTERNS ...
    --link-dest=/media/backup/latest/AMI-AGENTS \
    /home/ami/AMI-AGENTS/ \
    /media/backup/2026-03-23-132500/AMI-AGENTS/
```

For SSH targets, the destination becomes `user@host:/path/2026-03-23-132500/AMI-AGENTS/`.

For rsync daemon targets, the destination becomes `rsync://host:8873/module/2026-03-23-132500/AMI-AGENTS/`.

**Flags:**

| Flag | Purpose |
|------|---------|
| `-a` | Archive mode (recursive, permissions, timestamps, symlinks, groups, owner, devices) |
| `--delete` | Remove files from destination not in source |
| `--info=progress2` | Whole-transfer progress (not per-file) |
| `--no-inc-recursive` | Build full file list before transfer (required for accurate progress) |
| `--link-dest=<prev>` | Hard-link unchanged files from previous snapshot |
| `--exclude=<pattern>` | One per exclusion pattern |

**Trailing slash semantics:** Source path MUST end with `/` to copy contents (not the directory itself). Destination path MUST NOT end with `/`.

### 3.3 Progress Parsing (REQ-BAK-050)

rsync `--info=progress2` emits lines like:

```
    532,480,000  47%   24.83MB/s    0:00:10 (xfr#1234, to-chk=5678/12345)
```

The specification defines parsing as:

```python
RSYNC_PROGRESS_RE = re.compile(
    r"^\s*([\d,]+)\s+(\d+)%\s+([\d.]+\w+/s)\s+(\S+)"
)

def _parse_rsync_progress(line: str) -> RsyncProgress | None:
    match = RSYNC_PROGRESS_RE.match(line)
    if match:
        return RsyncProgress(
            bytes_transferred=int(match.group(1).replace(",", "")),
            percent=int(match.group(2)),
            speed=match.group(3),
            eta=match.group(4),
        )
    return None
```

Progress SHALL be displayed via tqdm, updating the bar from parsed percentage values.

### 3.4 Atomic Symlink Update (REQ-BAK-012)

```python
async def update_latest_symlink(target_base: str, snapshot_name: str) -> None:
    target_path = Path(target_base)
    latest = target_path / LATEST_SYMLINK_NAME
    tmp_link = target_path / f".latest_tmp_{os.getpid()}"

    # Create temp symlink (relative target for portability)
    tmp_link.symlink_to(snapshot_name)

    # Atomic rename (same filesystem guaranteed)
    os.rename(str(tmp_link), str(latest))
```

For network targets (SSH/daemon), symlink update MUST be performed via a remote command:
- SSH: `ssh user@host 'ln -sfn <snapshot> <target>/latest'`
- rsync daemon: symlink update is not supported; the daemon module root SHALL be the snapshot directory

### 3.5 Snapshot Rotation (REQ-BAK-013, REQ-BAK-014)

```python
async def rotate_snapshots(target_base: str, max_snapshots: int) -> int:
    snapshots = _find_existing_snapshots(Path(target_base))
    if len(snapshots) <= max_snapshots:
        return 0

    to_delete = snapshots[max_snapshots:]  # oldest beyond limit
    deleted = 0
    for snapshot in to_delete:
        shutil.rmtree(snapshot)
        deleted += 1
        logger.info(f"Rotated old snapshot: {snapshot.name}")
    return deleted
```

For SSH targets, rotation MUST use `ssh user@host 'rm -rf <path>'`.
For rsync daemon targets, rotation is not performed (server-side responsibility).

### 3.6 Pre-flight Validation (REQ-BAK-045)

Before starting backup, the system MUST validate each target:

| Target Type | Validation | Error |
|-------------|------------|-------|
| Local | Directory exists and is writable (test file) | "Backup mount not available: `<path>`" |
| SSH | `ssh -o ConnectTimeout=5 user@host true` | "Cannot reach SSH target: `<target>`" |
| rsync daemon | `rsync rsync://host:port/` (list modules) | "Cannot reach rsync daemon: `<target>`" |

---

## 4. rsync Backup: Restore Flow

### 4.1 Module Structure

**New file:** `ami/dataops/backup/restore/rsync_client.py`

```python
@dataclass
class SnapshotInfo:
    path: str              # full path or remote URI
    name: str              # timestamp directory name
    timestamp: datetime    # parsed from name
    is_latest: bool        # True if 'latest' symlink target

async def list_snapshots(target_base: str) -> list[SnapshotInfo]:
    """List snapshots from local or remote target, newest first. (REQ-BAK-022)"""

async def restore_snapshot(
    snapshot_path: str,
    restore_target: Path,
    specific_paths: list[Path] | None = None,
) -> bool:
    """Restore full or selective from snapshot. (REQ-BAK-020, REQ-BAK-021)"""

async def restore_latest_snapshot(
    target_base: str,
    restore_target: Path,
    specific_paths: list[Path] | None = None,
) -> bool:
    """Restore from 'latest' symlink. (REQ-BAK-020)"""
```

### 4.2 Restore rsync Command

Full restore:
```bash
rsync -a --info=progress2 \
    /media/backup/latest/AMI-AGENTS/ \
    /home/ami/AMI-AGENTS/
```

Selective restore (specific paths):
```bash
rsync -a --info=progress2 \
    --include='ami/dataops/backup/***' \
    --include='pyproject.toml' \
    --exclude='*' \
    /media/backup/2026-03-23-132500/AMI-AGENTS/ \
    /home/ami/AMI-AGENTS/
```

Note: `--delete` is NOT used on restore (preserves files not in backup).

### 4.3 Remote Snapshot Listing

For SSH targets:
```bash
ssh user@host 'ls -1d /backup/2[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9] 2>/dev/null && readlink /backup/latest 2>/dev/null'
```

For rsync daemon targets, listing is not directly supported. The system SHALL fall back to attempting `rsync rsync://host:port/module/` to list top-level directories.

---

## 5. rsync Daemon Bootstrap

### 5.1 Bootstrap Script

**New file:** `ami/scripts/bootstrap/bootstrap_rsync.sh`

| Step | Action | Idempotent |
|------|--------|------------|
| 1 | Validate platform is Linux | Yes |
| 2 | Check if already installed at `.boot-linux/rsync/bin/rsync` | Yes (exits 0) |
| 3 | Create temp directory with trap cleanup | Yes |
| 4 | `apt download rsync` to get `.deb` | Yes |
| 5 | Extract binary with `ar x` + `tar xf data.tar.*` | Yes |
| 6 | Place `rsync` binary in `.boot-linux/rsync/bin/` | Yes (overwrite) |
| 7 | Create dirs: `.boot-linux/rsync/{etc,var/run}` | Yes (mkdir -p) |
| 8 | Generate `rsyncd.conf` (§5.2) | Yes (overwrite) |
| 9 | Generate `rsyncd.secrets` with `chmod 600` (§5.3) | Yes (overwrite) |
| 10 | Generate `rsyncd-venv` daemon wrapper (§5.4) | Yes (overwrite) |
| 11 | Create symlink: `.boot-linux/bin/rsync` → `../rsync/bin/rsync` | Yes (ln -sf) |
| 12 | Verify: `rsync --version` | Yes |

### 5.2 Generated Configuration: rsyncd.conf

**Location:** `.boot-linux/rsync/etc/rsyncd.conf`

```ini
# Generated by bootstrap_rsync.sh. Do not edit manually.
pid file = <PROJECT_ROOT>/.boot-linux/rsync/var/run/rsyncd.pid
log file = <PROJECT_ROOT>/.boot-linux/rsync/var/run/rsyncd.log
port = 8873
use chroot = no
max connections = 4
uid = <current_user>
gid = <current_group>

[backup]
    path = /media/backup
    comment = AMI backup snapshots
    read only = yes
    list = yes
    auth users = ami
    secrets file = <PROJECT_ROOT>/.boot-linux/rsync/etc/rsyncd.secrets

[backup-rw]
    path = /media/backup
    comment = AMI backup snapshots (writable)
    read only = no
    list = no
    auth users = ami
    secrets file = <PROJECT_ROOT>/.boot-linux/rsync/etc/rsyncd.secrets
```

The `backup` module is read-only (for pulls from other machines). The `backup-rw` module is writable and unlisted (for pushes).

### 5.3 Generated Secrets File

**Location:** `.boot-linux/rsync/etc/rsyncd.secrets`
**Permissions:** `600` (MUST be strict; rsync refuses otherwise)

```
ami:<generated_password>
```

Password SHALL be generated via `openssl rand -base64 24` during bootstrap.

### 5.4 Daemon Wrapper: rsyncd-venv

**Location:** `.boot-linux/bin/rsyncd-venv`

```bash
#!/usr/bin/env bash
set -euo pipefail

RSYNC_DIR="<PROJECT_ROOT>/.boot-linux/rsync"
RSYNC_BIN="${RSYNC_DIR}/bin/rsync"
RSYNC_CONFIG="${RSYNC_DIR}/etc/rsyncd.conf"
PID_FILE="${RSYNC_DIR}/var/run/rsyncd.pid"

case "${1:-}" in
    start)
        if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
            echo "rsyncd already running (PID: $(cat "${PID_FILE}"))"
            exit 0
        fi
        echo "Starting rsyncd on port 8873..."
        "${RSYNC_BIN}" --daemon --config="${RSYNC_CONFIG}"
        echo "rsyncd started (PID: $(cat "${PID_FILE}"))"
        ;;
    stop)
        if [[ -f "${PID_FILE}" ]]; then
            echo "Stopping rsyncd (PID: $(cat "${PID_FILE}"))"
            kill "$(cat "${PID_FILE}")" 2>/dev/null || true
            rm -f "${PID_FILE}"
        else
            echo "rsyncd not running"
        fi
        ;;
    restart)
        "$0" stop
        sleep 1
        "$0" start
        ;;
    status)
        if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
            echo "rsyncd running (PID: $(cat "${PID_FILE}"))"
            exit 0
        else
            echo "rsyncd not running"
            exit 1
        fi
        ;;
    *)
        echo "Usage: rsyncd-venv {start|stop|restart|status}"
        exit 1
        ;;
esac
```

### 5.5 Component Registration

**File:** `ami/scripts/bootstrap_components.py`

```python
Component(
    name="rsync",
    label="rsync",
    description="File sync + backup daemon",
    type=ComponentType.SCRIPT,
    group="Development Tools",
    script="bootstrap_rsync.sh",
    detect_path=".boot-linux/bin/rsync",
    version_cmd=[".boot-linux/rsync/bin/rsync", "--version"],
    version_pattern=r"rsync\s+version (\d+\.\d+\.\d+)",
)
```

---

## 6. CLI Specification

### 6.1 Create CLI: ami-backup

**Modified file:** `ami/dataops/backup/create/cli.py`

New/modified arguments:

| Argument | Type | Default | Purpose |
|----------|------|---------|---------|
| `--mode` | `{rsync,archive,gdrive}` | Auto-detect (§2.2) | Backup mode (REQ-CLI-002, REQ-CLI-003) |
| `--target` | string | From config | Override backup destination (REQ-CLI-004, REQ-CLI-005, REQ-CLI-006) |
| `--dry-run` | flag | False | Show what would transfer (REQ-CLI-007) |

**Existing arguments retained:** `source`, `--config-path`, `--name`, `--include-all`, `--keep-local`, `--no-auth-retry`, `--auth-mode`, `--setup-auth`, `--verbose`

**Dispatch logic in `run()`:**

```python
if mode == "rsync":
    service = RsyncBackupService()
    snapshot_path = await service.run_backup(options)
    logger.info(f"✓ Snapshot created: {snapshot_path}")
elif mode == "archive":
    # Archive only: create tar.zst, copy to secondary, no upload
    zip_path = await create_zip_archive(...)
    await copy_to_secondary_backup(zip_path)
elif mode == "gdrive":
    # Existing flow
    file_id = await self.service.run_backup(options)
```

### 6.2 Restore CLI: ami-restore

**Modified file:** `ami/dataops/backup/restore/cli.py`

New arguments added to mutually exclusive source group:

| Argument | Type | Purpose |
|----------|------|---------|
| `--latest-snapshot` | flag | Restore from `latest` symlink (REQ-CLI-010) |
| `--snapshot` | string | Restore named snapshot (REQ-CLI-011) |
| `--list-snapshots` | flag | List available snapshots (REQ-CLI-012) |

**Existing arguments retained:** `--file-id`, `--local-path`, `--latest-local`, `--interactive`, `--revision`, `--list-revisions`, `--restore-path`, `--dest`, `paths`, `--verbose`

### 6.3 Daemon CLI: rsyncd-venv

See §5.4 for full implementation. Invocation:

```bash
rsyncd-venv start      # REQ-CLI-020
rsyncd-venv stop       # REQ-CLI-021
rsyncd-venv status     # REQ-CLI-022
rsyncd-venv restart    # REQ-CLI-023
```

---

## 7. Service Layer

### 7.1 RsyncBackupService

**New class in:** `ami/dataops/backup/create/service.py`

```python
class RsyncBackupOptions(BaseModel):
    source_dir: Path | None = None
    ignore_exclusions: bool = False
    config_path: Path | None = None
    target_override: str | None = None  # --target flag
    dry_run: bool = False

class RsyncBackupService:
    async def run_backup(self, options: RsyncBackupOptions) -> Path:
        """
        1. Load config (rsync mode, no GDrive auth)
        2. Pre-flight: validate target reachability (REQ-BAK-045)
        3. Resolve link_dest from 'latest' symlink
        4. Generate snapshot name (YYYY-MM-DD-HHMMSS)
        5. Run rsync with --link-dest (REQ-BAK-010, REQ-BAK-011)
        6. Update 'latest' symlink atomically (REQ-BAK-012)
        7. Rotate old snapshots (REQ-BAK-013, REQ-BAK-014)
        8. Report stats (REQ-BAK-052, REQ-BAK-053)
        Returns: Path to new snapshot
        """
```

### 7.2 Dependency Injection Updates

**Modified file:** `ami/dataops/backup/create/main.py`

```python
def main() -> None:
    config = BackupConfig.load(Path.cwd())

    if config.backup_mode == BackupMode.RSYNC:
        service = RsyncBackupService()
        cli = BackupCLI(rsync_service=service)
    elif config.backup_mode == BackupMode.GDRIVE:
        auth_manager = AuthenticationManager(config)
        uploader = BackupUploader(auth_manager)
        service = BackupService(uploader, auth_manager)
        cli = BackupCLI(gdrive_service=service)
    else:  # archive
        cli = BackupCLI(archive_mode=True)

    asyncio.run(cli.run(args))
```

---

## 8. Exception Hierarchy

**Modified file:** `ami/dataops/backup/backup_exceptions.py`

```
BackupError (base)
├── BackupConfigError
├── ArchiveError
├── UploadError
└── RsyncError          # NEW (REQ-BAK-* failures)
    ├── RsyncTargetError    # Target unreachable / permission denied
    └── RsyncTransferError  # Transfer failed mid-operation
```

---

## 9. File Manifest

### New Files

| File | Purpose | Requirements |
|------|---------|-------------|
| `ami/dataops/backup/create/rsync_backup.py` | Core rsync --link-dest logic | REQ-BAK-010 through 015 |
| `ami/dataops/backup/restore/rsync_client.py` | Snapshot listing + restore | REQ-BAK-020 through 024 |
| `ami/scripts/bootstrap/bootstrap_rsync.sh` | Bootstrap rsync + rsyncd | REQ-BAK-030 through 035 |
| `tests/unit/backup/create/test_rsync_backup.py` | Unit tests | N/A |
| `tests/unit/backup/restore/test_rsync_client.py` | Unit tests | N/A |

### Modified Files

| File | Changes | Requirements |
|------|---------|-------------|
| `ami/dataops/backup/common/constants.py` | Add `BackupMode` enum, snapshot constants | REQ-BAK-040 |
| `ami/dataops/backup/backup_config.py` | Add mode/mount/remote/max_snapshots fields, conditional auth | REQ-BAK-040 through 044 |
| `ami/dataops/backup/backup_exceptions.py` | Add `RsyncError` hierarchy | N/A |
| `ami/dataops/backup/create/service.py` | Add `RsyncBackupService` | REQ-BAK-010 |
| `ami/dataops/backup/create/cli.py` | Add `--mode`, `--target`, `--dry-run` | REQ-CLI-001 through 007 |
| `ami/dataops/backup/create/main.py` | Mode-aware DI | REQ-BAK-040 |
| `ami/dataops/backup/restore/cli.py` | Add `--snapshot`, `--latest-snapshot`, `--list-snapshots` | REQ-CLI-010 through 014 |
| `ami/dataops/backup/restore/service.py` | Add snapshot restore methods | REQ-BAK-020 through 024 |
| `ami/scripts/bootstrap_components.py` | Register rsync component | REQ-BAK-035 |

---

## 10. Traceability Matrix

| Requirement | Specification Section | Verification |
|-------------|----------------------|--------------|
| REQ-BAK-001 Local targets | §2.3, §3.2 | `ami-backup --target /media/backup` creates snapshot |
| REQ-BAK-002 rsync daemon targets | §2.3, §3.2 | `ami-backup --target rsync://host:8873/backup-rw` succeeds |
| REQ-BAK-003 SSH targets | §2.3, §3.2 | `ami-backup --target user@host:/backup` succeeds |
| REQ-BAK-004 Multiple targets | §3.1 | `AMI_BACKUP_MOUNT` + `AMI_BACKUP_REMOTE` both receive snapshots |
| REQ-BAK-005 GDrive retained | §1.2 | `ami-backup --mode gdrive` works unchanged |
| REQ-BAK-010 Timestamped snapshots | §1.3, §3.1 | `ls /media/backup/` shows `YYYY-MM-DD-HHMMSS/` dirs |
| REQ-BAK-011 Hard-link dedup | §3.2 | Second backup uses <1% additional space for unchanged project |
| REQ-BAK-012 Latest symlink | §3.4 | `readlink /media/backup/latest` → newest snapshot |
| REQ-BAK-013 Configurable retention | §3.5 | `BACKUP_MAX_SNAPSHOTS=5` keeps only 5 snapshots |
| REQ-BAK-014 Auto-rotation | §3.5 | 11th backup with max=10 deletes oldest |
| REQ-BAK-015 Timestamp format | §3.1 | Dirs match `\d{4}-\d{2}-\d{2}-\d{6}` |
| REQ-BAK-020 Full restore | §4.2 | `ami-restore --latest-snapshot` restores all files |
| REQ-BAK-021 Selective restore | §4.2 | `ami-restore --snapshot <name> ami/scripts/` restores subtree |
| REQ-BAK-022 List snapshots | §4.3 | `ami-restore --list-snapshots` shows table with dates/sizes |
| REQ-BAK-023 Remote restore | §4.2, §4.3 | Restore from SSH target works |
| REQ-BAK-024 Preserve perms | §3.2 | rsync `-a` flag preserves permissions/ownership/timestamps |
| REQ-BAK-030 Bootstrapped binary | §5.1 | `.boot-linux/rsync/bin/rsync --version` works |
| REQ-BAK-031 Non-privileged daemon | §5.2, §5.4 | `rsyncd-venv start` binds port 8873 without sudo |
| REQ-BAK-032 Default config | §5.2 | `rsyncd.conf` generated with backup module |
| REQ-BAK-033 Authenticated access | §5.3 | `rsync rsync://host:8873/backup` prompts for password |
| REQ-BAK-034 Daemon control | §5.4 | `rsyncd-venv start/stop/status/restart` work |
| REQ-BAK-035 Component registered | §5.5 | rsync appears in TUI installer |
| REQ-BAK-040 Mode auto-detect | §2.2 | No BACKUP_MODE + no GDRIVE config → rsync mode |
| REQ-BAK-041 Mode env var | §2.1 | `BACKUP_MODE=rsync` forces rsync |
| REQ-BAK-042 Mode CLI flag | §6.1 | `ami-backup --mode rsync` overrides env |
| REQ-BAK-043 No GDrive auth in rsync | §2.4 | rsync mode starts without any GDrive credentials |
| REQ-BAK-044 Target env vars | §2.1 | All env vars documented and functional |
| REQ-BAK-045 Pre-flight validation | §3.6 | Unreachable target fails before backup starts |
| REQ-BAK-050 Create progress | §3.3 | tqdm bar shows percentage during backup |
| REQ-BAK-051 Restore progress | §4.2 | tqdm bar shows percentage during restore |
| REQ-BAK-052 Completion report | §7.1 | Logs total size and elapsed time |
| REQ-BAK-053 Dedup savings | §7.1 | Logs new data vs total snapshot size |
| REQ-BAK-060 Exclusion patterns | §3.2 | `.git/`, `__pycache__/` etc. excluded |
| REQ-BAK-061 Include-all flag | §6.1 | `--include-all` disables exclusions |
| REQ-BAK-062 rsync-compatible | §3.2 | Existing patterns map to `--exclude` flags |
| REQ-CLI-001 through 023 | §6.1, §6.2, §6.3 | CLI smoke tests for all flags/subcommands |
