# Requirements: Backup & Sync System

**Date:** 2026-03-23
**Updated:** 2026-04-13
**Status:** ACTIVE
**Type:** Requirements
**Spec:** [SPEC-BACKUP](../specifications/SPEC-BACKUP.md)

## Overview
Replace the Google Drive-dependent backup system with a flexible rsync-based backup solution that supports local drives, network targets, and an optional rsync daemon for remote access. The existing tar.zst + Google Drive flow remains available as a fallback mode.

## Background
The current backup system uses tar.zst archives uploaded to Google Drive, with optional secondary copy to a mount point. Google Workspace policy blocks the OAuth client in some configurations, motivating a local-first rsync approach.

---

## Core Requirements

### 1. Backup Targets

- **REQ-BAK-001**: System shall support local filesystem targets (mounted drives, directories)
- **REQ-BAK-002**: System shall support network targets via rsync protocol (`rsync://host:port/module`)
- **REQ-BAK-003**: System shall support network targets via SSH (`user@host:/path`)
- **REQ-BAK-004**: System shall support multiple backup targets simultaneously (e.g., local drive + remote server)
- **REQ-BAK-005**: System shall retain the existing Google Drive upload mode as an optional backup target

### 2. Snapshot Versioning

- **REQ-BAK-010**: System shall create timestamped snapshot directories for each backup run
- **REQ-BAK-011**: System shall use rsync `--link-dest` to hard-link unchanged files from the previous snapshot, minimizing disk usage
- **REQ-BAK-012**: System shall maintain a `latest` symlink pointing to the most recent successful snapshot
- **REQ-BAK-013**: System shall support configurable snapshot retention (maximum number of snapshots to keep)
- **REQ-BAK-014**: System shall automatically rotate (delete) the oldest snapshots when the retention limit is exceeded
- **REQ-BAK-015**: Snapshot directory names shall use the format `YYYY-MM-DD-HHMMSS` for lexicographic sorting

### 3. Restore Operations

- **REQ-BAK-020**: System shall support full restore from any snapshot (latest or named)
- **REQ-BAK-021**: System shall support selective restore of specific files or directories from a snapshot
- **REQ-BAK-022**: System shall list available snapshots with timestamps and sizes
- **REQ-BAK-023**: System shall support restore from both local and network snapshot sources
- **REQ-BAK-024**: System shall preserve file permissions, ownership, and timestamps during restore

### 4. rsync Daemon (rsyncd)

- **REQ-BAK-030**: rsync SHALL be a system dependency installed via `pre-req.sh` (not bootstrapped to `.boot-linux/`)
- **REQ-BAK-031**: System shall provide an rsync daemon that runs on a non-privileged port (no root/sudo required)
- **REQ-BAK-032**: System shall generate a default `rsyncd.conf` exposing the backup snapshot directory as a read-only module
- **REQ-BAK-033**: System shall support authenticated access via rsync secrets file
- **REQ-BAK-034**: System shall provide a daemon control script (start/stop/restart/status)
- **REQ-BAK-035**: System shall register the rsync daemon as a bootstrap component in the TUI installer

### 5. Configuration

- **REQ-BAK-040**: System shall auto-detect backup mode: rsync when no Google Drive config is present, gdrive when it is
- **REQ-BAK-041**: System shall support explicit mode override via `BACKUP_MODE` environment variable (`rsync`, `archive`, `gdrive`)
- **REQ-BAK-042**: System shall support explicit mode override via CLI flag (`--mode`)
- **REQ-BAK-043**: System shall not require Google Drive credentials when operating in rsync or archive mode
- **REQ-BAK-044**: Backup targets shall be configurable via environment variables:
  - `AMI_BACKUP_MOUNT`: local mount path (no default — must be explicitly configured)
  - `AMI_BACKUP_REMOTE`: network target (e.g., `rsync://nas:8873/backup` or `user@host:/backup`)
  - `BACKUP_MAX_SNAPSHOTS`: retention limit (default: 10)
- **REQ-BAK-045**: System shall validate that at least one backup target is reachable before starting the backup

### 6. Google Drive Authentication

- **REQ-BAK-050**: System shall support three Google Drive authentication methods:
  - **Impersonation**: Service account impersonation via `gcloud` Application Default Credentials (ADC)
  - **Service Account Key**: Direct service account JSON key file
  - **OAuth**: User OAuth flow with token refresh (pickle-based token cache)
- **REQ-BAK-051**: Auth method shall be selectable via `GDRIVE_AUTH_METHOD` env var (`impersonation`, `key`, `oauth`)
- **REQ-BAK-052**: `gcloud` CLI shall be used for ADC credential management (refresh, impersonation). The bootstrapped `ami-gcloud` wrapper or system `gcloud` shall be supported.
- **REQ-BAK-053**: Google Drive credentials SHALL eventually migrate to OpenBao (per REQ-IAM FR-11). Until then, `.env` and service account key files remain the auth mechanism.
- **REQ-BAK-054**: OAuth token pickle file location shall be configurable via `GDRIVE_TOKEN_FILE` env var
- **REQ-BAK-055**: Service account email shall be configurable via `GDRIVE_SERVICE_ACCOUNT_EMAIL` env var
- **REQ-BAK-056**: Service account key file path shall be configurable via `GDRIVE_CREDENTIALS_FILE` env var
- **REQ-BAK-057**: Backup folder ID shall be configurable via `GDRIVE_BACKUP_FOLDER_ID` env var

### 7. Progress & Reporting

- **REQ-BAK-060**: System shall display real-time progress during backup (files transferred, bytes, percentage)
- **REQ-BAK-061**: System shall display real-time progress during restore
- **REQ-BAK-062**: System shall report total backup size and time elapsed on completion
- **REQ-BAK-063**: System shall report space savings from hard-link deduplication (new data vs. total snapshot size)

### 8. File Exclusions

- **REQ-BAK-070**: System shall reuse the existing exclusion patterns (`.git/`, `__pycache__/`, `*.pyc`, etc.)
- **REQ-BAK-071**: System shall support `--include-all` flag to disable exclusions
- **REQ-BAK-072**: Exclusion patterns shall be compatible with rsync `--exclude` syntax (existing patterns already are)

---

## CLI Requirements

### Create (ami-backup)

- **REQ-CLI-001**: `ami-backup` with no arguments shall auto-detect mode and back up to default target
- **REQ-CLI-002**: `ami-backup --mode rsync` shall explicitly use rsync mode
- **REQ-CLI-003**: `ami-backup --mode gdrive` shall use existing Google Drive flow
- **REQ-CLI-004**: `ami-backup --target /path` shall override the backup destination
- **REQ-CLI-005**: `ami-backup --target user@host:/path` shall back up to a network target via SSH
- **REQ-CLI-006**: `ami-backup --target rsync://host:port/module` shall back up to an rsync daemon
- **REQ-CLI-007**: `ami-backup --dry-run` shall show what would be transferred without actually doing it

### Restore (ami-restore)

- **REQ-CLI-010**: `ami-restore --latest-snapshot` shall restore from the most recent rsync snapshot
- **REQ-CLI-011**: `ami-restore --snapshot <name>` shall restore from a named snapshot
- **REQ-CLI-012**: `ami-restore --list-snapshots` shall list all available snapshots with dates and sizes
- **REQ-CLI-013**: `ami-restore --snapshot <name> path/to/file` shall selectively restore specific paths
- **REQ-CLI-014**: Existing `--local-path`, `--latest-local`, and `--file-id` restore modes shall continue to work

### Daemon (rsyncd-venv)

- **REQ-CLI-020**: `rsyncd-venv start` shall start the rsync daemon
- **REQ-CLI-021**: `rsyncd-venv stop` shall stop the daemon
- **REQ-CLI-022**: `rsyncd-venv status` shall report whether the daemon is running
- **REQ-CLI-023**: `rsyncd-venv restart` shall restart the daemon

---

## Implementation Constraints

### Technical Constraints
- Python 3.11
- rsync invoked as subprocess (no Python rsync library)
- Async I/O via asyncio
- Logging via loguru
- Progress display via tqdm
- rsync is a system dependency (installed via `pre-req.sh`), not bootstrapped
- No root/sudo required for backup/restore operations

### Code Ownership
- All backup code SHALL reside in AMI-DATAOPS
- Any legacy backup code in AMI-AGENTS SHALL be removed after migration
- CLI entry points (thin wrappers) remain in AMI-AGENTS

### Compatibility Constraints
- Existing tar.zst + Google Drive backup flow shall remain functional
- Existing tar.zst local restore (`--local-path`, `--latest-local`) shall remain functional
- Existing test suite shall continue to pass
- Existing `.env` configurations shall continue to work without modification

### Filesystem Constraints
- Hard-link snapshots require the backup drive to support hard links (ext4, xfs, btrfs, but not FAT32/NTFS)
- Network targets via rsync protocol do not use hard-link deduplication (server-side dedup is the server's responsibility)
- SSH targets support hard-links only if the remote filesystem supports them

---

## Success Criteria

### Functional
- `ami-backup` creates a snapshot on a local drive with zero Google Drive dependency
- Second backup run hard-links unchanged files (minimal additional disk usage)
- Snapshots are rotated when retention limit is exceeded
- `ami-restore --latest-snapshot` restores from the most recent snapshot
- Network targets (`rsync://`, `ssh://`) work for both backup and restore
- `rsyncd-venv start` starts a daemon accessible from other machines

### Performance
- Placeholder targets (no real-world performance data yet):
  - Incremental backup of a 10GB project with <1% changed files: target <30s on local drive
  - Snapshot rotation of 100+ snapshots: target <5s

### Reliability
- Pre-flight validation catches unreachable targets before backup starts
- Atomic symlink update ensures `latest` always points to a complete snapshot
- Failed backup does not corrupt or remove previous snapshots

---

## Dependencies

### Internal
- AMI-DATAOPS backup module
- AMI-AGENTS CLI entry points (thin wrappers)
- System pre-requisites installer (rsync)

### External
- rsync (system package)
- gcloud (for Google Drive auth)
- tqdm, loguru (Python)
- google-auth, google-auth-oauthlib, google-api-python-client (Python, GDrive mode)

### Related Documents
- [SPEC-BACKUP](../specifications/SPEC-BACKUP.md)
- [REQ-IAM](REQ-IAM.md) — FR-11 (backup credentials from OpenBao)
