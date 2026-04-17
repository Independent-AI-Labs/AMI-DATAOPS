# AMI-DATAOPS Backlog: Operations Toolkit

Tracks implementation progress and deferred work for the contracts defined in
[REQUIREMENTS-OPERATIONS.md](REQUIREMENTS-OPERATIONS.md). The requirements doc
describes *what must be true*; this backlog describes *where we are and what's
next*.

## Implementation Status (2026-04-17)

| Feature | Status | Notes |
|---------|--------|-------|
| Archive backup/restore (Google Drive) | IMPLEMENTED | `ami/dataops/backup/` — GDrive upload, 3 auth methods, restore wizard, selective restore |
| Docker Compose service deployment | IMPLEMENTED | `ansible/` playbooks |
| Ansible playbooks (deploy/stop/restart/status) | IMPLEMENTED | |
| Keycloak realm + OIDC client provisioning | IMPLEMENTED | |
| rclone transport backend (R-BACKUP-001) | PLANNED | Replace GDrive-specific upload with rclone remotes |
| Database-aware backup via borgmatic (§1.2) | PLANNED | pg_dump / mysqldump / mongodump + borg dedup |
| Docker volume backup (§1.3) | PLANNED | Helper-container + rclone transport |
| File synchronization subsystem (§2) | PLANNED | rclone sync / copy / bisync, profiles |
| Service catalog YAML structure (§3) | PLANNED | Per-instance config model |
| Multi-instance management CLI (§4.2) | PLANNED | `ami-dataops instance add/remove/upgrade` |
| Monitoring dashboards / Grafana (§5.2) | PLANNED | Prometheus exporters + Grafana JSON dashboards |
| Alerting / Alertmanager (§5.3) | PLANNED | Alert rules, routes, receivers |
| Notification webhooks (R-BACKUP-008) | PLANNED | Webhook / email / healthchecks.io on backup done |
| Bandwidth throttling (R-BACKUP-006) | PLANNED | rclone `--bwlimit` |
| Metadata manifests (R-BACKUP-007) | PLANNED | JSON manifest per backup run |

## Ordering (implementation priorities)

Roughly follows the order in which downstream systems will need them:

1. **rclone transport (R-BACKUP-001..010)** — unlocks multi-cloud, encryption, retention, verification.
2. **Database backup via borgmatic (R-DBBACKUP-001..005)** — required for data services in production.
3. **Docker volume backup (R-VOLBACKUP-001..006)** — covers stateful services without native dump tools.
4. **Service catalog + instance CLI (§3, §4.2)** — multi-instance lifecycle management.
5. **Monitoring + alerting (§5)** — visibility and paging.
6. **File synchronization (§2)** — peer-to-peer and cross-host file sharing.

## Non-goals captured here

Any item in [REQUIREMENTS-OPERATIONS.md §9 Non-Requirements](REQUIREMENTS-OPERATIONS.md) stays out of scope and is not tracked in this backlog.
