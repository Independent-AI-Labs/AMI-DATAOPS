# AMI-DATAOPS Requirements: Data Operations Toolkit

Requirements for AMI-DATAOPS as a data operations platform. Covers backup and restore, file synchronization, data service provisioning, monitoring, alerting, and maintenance automation.

## Implementation Status

| Feature | Status |
|---------|--------|
| Archive backup/restore (Google Drive) | IMPLEMENTED |
| Docker Compose service deployment | IMPLEMENTED |
| Ansible playbooks (deploy/stop/restart/status) | IMPLEMENTED |
| Keycloak realm + OIDC client provisioning | IMPLEMENTED |
| rclone transport backend (R-BACKUP-001) | PLANNED |
| Database-aware backup via borgmatic (1.2) | PLANNED |
| Docker volume backup (1.3) | PLANNED |
| File synchronization subsystem (2.x) | PLANNED |
| Service catalog YAML structure (3.x) | PLANNED |
| Multi-instance management CLI (4.2) | PLANNED |
| Monitoring dashboards / Grafana (5.x) | PLANNED |
| Alerting / Alertmanager (5.x) | PLANNED |
| Notification webhooks (R-BACKUP-008) | PLANNED |
| Bandwidth throttling (R-BACKUP-006) | PLANNED |
| Metadata manifests (R-BACKUP-007) | PLANNED |

## Scope

AMI-DATAOPS automates the operational lifecycle of data services: **provision, configure, monitor, alert, backup, restore, sync, upgrade, and maintain**. It does not implement business logic, DAO abstractions, or application-level persistence patterns.

### Design Principles

1. **Do not reinvent the wheel.** Use battle-tested tools (rclone, borgmatic, Prometheus, Grafana, Ansible) and provide orchestration glue.
2. **Configuration-driven.** All behavior controlled via Ansible inventory variables. One source of truth.
3. **Multi-instance by default.** Every service type supports N named instances. One PostgreSQL or five — same mechanism.
4. **Composable.** Each subsystem (backup, sync, monitoring, provisioning) works independently and composes with the others.
5. **Idempotent.** Running any operation twice produces the same result. Ansible convergence model.
6. **No root required.** All services run as user-scoped systemd units via `loginctl enable-linger`.

---

## 1. Backup and Restore

The backup subsystem handles three use cases: **full-archive backup** (tar + compress + push to cloud), **database-aware backup** (dump + dedup + push), and **Docker volume backup** (stop/snapshot + archive + push). All share the same rclone transport and scheduling infrastructure. All backup operations are per-instance — each named instance has its own backup config, schedule, and retention.

The backup/restore code currently lives in AMI-AGENTS at `ami/scripts/backup/`. It will be migrated into AMI-DATAOPS as part of P0 work.

### 1.1 Archive Backup

The existing archive backup system creates `.tar.zst` archives and uploads to Google Drive. It must be extended to support any cloud backend via rclone.

**Current capabilities (keep):**
- Multi-threaded Zstandard compression via `tar.zst`
- Google Drive upload with 3 auth methods (OAuth, service account impersonation, SA key)
- Restore from Google Drive by file ID, revision number, or interactive selection
- Selective restore (specific files/directories from archive)
- Local backup discovery and restore
- Interactive wizard for restore operations

**Required extensions:**

- **R-BACKUP-001**: Replace Google Drive-specific upload with rclone as the transport backend. rclone supports 50+ cloud backends (S3, GDrive, Azure Blob, Dropbox, OneDrive, Backblaze B2, SFTP, etc.) through a single configuration. The existing GDrive-specific auth and upload code is removed; GDrive becomes one rclone remote among many.
- **R-BACKUP-002**: Support multiple backup destinations simultaneously (e.g., upload to both GDrive and S3 in a single backup run). Configurable as a list of rclone remote targets in Ansible inventory.
- **R-BACKUP-003**: Client-side encryption before upload via rclone's `crypt` remote (AES-256-CTR + Poly1305). Configurable per destination. Encryption keys stored in Ansible Vault alongside other secrets.
- **R-BACKUP-004**: Retention policies. Configurable keep-last-N, daily/weekly/monthly rotation, and max-age pruning. Applied per destination.
- **R-BACKUP-005**: Backup verification. After upload, download the checksum manifest and verify integrity. Optionally perform periodic test restores.
- **R-BACKUP-006**: Bandwidth throttling. Configurable upload/download speed limits via rclone `--bwlimit`.
- **R-BACKUP-007**: Backup metadata manifest. Each backup produces a JSON manifest recording: timestamp, source path, archive size, SHA-256 checksum, destination(s), retention class, and contents listing.
- **R-BACKUP-008**: Notifications on backup success/failure. Configurable webhook, email, or healthchecks.io ping.
- **R-BACKUP-009**: Scheduling via systemd timers. Ansible generates timer units from inventory variables:
  ```yaml
  dataops_backup_schedule:
    archive:
      calendar: "daily 02:00"
      retention:
        daily: 7
        weekly: 4
        monthly: 3
    databases:
      calendar: "daily 01:00"
      retention:
        daily: 7
        weekly: 4
        monthly: 6
    volumes:
      calendar: "daily 03:00"
      retention:
        daily: 7
        weekly: 2
  ```
- **R-BACKUP-010**: Local backup rotation. Automatically prune local archives beyond configured retention.

### 1.2 Database Backup

Database-aware backup using borgmatic for dump orchestration and borg for deduplication. Each service instance has its own borgmatic config, generated from the instance definition in Ansible inventory.

- **R-DBBACKUP-001**: Integrate borgmatic for database dump orchestration. borgmatic provides built-in `pg_dump`, `mysqldump`, and `mongodump` hooks with pre/post-backup commands, retention policies, and health check integration. Ansible generates a borgmatic config file per database instance from inventory variables.
- **R-DBBACKUP-002**: Per-instance backup configuration declared in the instance definition:
  ```yaml
  dataops_postgres_instances:
    - name: main
      port: 5432
      image: pgvector/pgvector:pg16
      password: "{{ vault_postgres_main_password }}"
      backup:
        method: pg_dump
        databases: [postgres, keycloak]
        format: custom  # pg_dump -Fc
        retention:
          daily: 7
          weekly: 4
          monthly: 6
    - name: analytics
      port: 5433
      image: timescale/timescaledb:latest-pg16
      password: "{{ vault_postgres_analytics_password }}"
      backup:
        method: pg_dump
        databases: [metrics]
        format: custom
        retention:
          daily: 7
          weekly: 4

  dataops_redis_instances:
    - name: cache
      port: 6379
      image: redis:8.6.1
      backup:
        method: rdb_copy
        source: /data/dump.rdb

  dataops_dgraph_instances:
    - name: main
      port: 8081
      image: dgraph/standalone:v25.2.0
      backup:
        method: dgraph_export
        format: rdf
  ```
- **R-DBBACKUP-003**: Backup storage via borg (deduplication + compression) with rclone push to cloud. Pipeline: `borgmatic dump -> borg create -> rclone sync` to remote target. Borg encryption keys stored in Ansible Vault. Each instance gets its own borg repository for independent retention and restore.
- **R-DBBACKUP-004**: Restore verification. After restore, run basic health checks (pg_isready, `SELECT 1`, key count) to confirm data integrity.
- **R-DBBACKUP-005**: Backup status metrics. Expose last-backup timestamp, size, and success/failure per instance as Prometheus metrics for alerting (R-ALERT-002).

**Future work (not in scope for initial implementation):**
- Point-in-time restore for PostgreSQL via WAL archiving. This requires `archive_command` configuration, WAL segment shipping to borg/rclone target, periodic base backups, and `restore_command` / recovery targeting. It is a significant subsystem that will be designed separately once dump-based backup is proven.

### 1.3 Docker Volume Backup

Automated backup of Docker named volumes for services that don't have native dump tools, or as an additional safety net alongside database dumps. Volume names follow the instance naming convention (`ami-{type}-{name}-data`).

- **R-VOLBACKUP-001**: Volume backup via temporary helper container. For each configured volume, run a temporary container that mounts the volume read-only, creates a `.tar.zst` archive of its contents, and writes it to a staging directory.
- **R-VOLBACKUP-002**: Service quiescence before volume backup. For stateful services (databases, message brokers), the volume backup process must stop the service container before mounting its volume to ensure data consistency. After archiving, restart the service. Quiescence behavior is declared per-instance:
  ```yaml
  dataops_volume_backups:
    - instance: postgres-main
      volume: ami-postgres-main-data
      quiesce: stop
      container: ami-postgres-main
    - instance: postgres-analytics
      volume: ami-postgres-analytics-data
      quiesce: stop
      container: ami-postgres-analytics
    - instance: redis-cache
      volume: ami-redis-cache-data
      quiesce: none         # Redis RDB is crash-consistent
    - instance: keycloak-main
      volume: ami-keycloak-main-data
      quiesce: stop
      container: ami-keycloak-main
  ```
  Alternatively, Ansible auto-generates this list from the instance definitions. If the instance has `backup.quiesce: stop` or the service type is in a known-stateful list, quiescence is applied.
- **R-VOLBACKUP-003**: Volume backup uses the same rclone transport, encryption, retention, and scheduling infrastructure as archive and database backups (R-BACKUP-002 through R-BACKUP-010). No separate mechanisms.
- **R-VOLBACKUP-004**: Selective volume restore. Restore a specific instance volume from a specific backup timestamp:
  `ami-dataops backup restore --volume postgres-main --timestamp 2026-03-01T02:00:00`
  Restore creates a temporary container that extracts the archive into the target volume.
- **R-VOLBACKUP-005**: Volume inventory. `ami-dataops backup status --volumes` lists all managed volumes with instance name, last backup time, size, and age.
- **R-VOLBACKUP-006**: Integration with database backup. When both database dump (1.2) and volume backup (1.3) are configured for the same instance, the database dump runs first (clean logical backup), then the volume backup runs as a secondary physical backup. Both are independent and either can be used for restore.

### 1.4 Backup CLI

The backup CLI is part of the unified `ami-dataops` entry point. All commands that target a service accept `{type}-{name}` instance identifiers.

- **R-BCLI-001**: Subcommands under `ami-dataops backup`:
  - `backup create` -- full archive backup with rclone transport
  - `backup create --db <type-name>` -- database-specific backup for a named instance (e.g., `--db postgres-main`)
  - `backup create --db all` -- all configured database backups across all instances
  - `backup create --volume <type-name>` -- backup a specific Docker volume by instance (e.g., `--volume postgres-analytics`)
  - `backup create --volume all` -- all configured volume backups
  - `backup restore` -- archive restore (interactive wizard)
  - `backup restore --db <type-name>` -- restore database from dump for a specific instance
  - `backup restore --volume <type-name> [--timestamp <ISO8601>]` -- restore a Docker volume for a specific instance
  - `backup list [--db | --volume | --archive]` -- list available backups (grouped by instance)
  - `backup verify` -- verify integrity of most recent backup
  - `backup prune` -- apply retention policies and delete expired backups
  - `backup status` -- show last backup time, size, health per instance and volume
- **R-BCLI-002**: All subcommands support `--json` for machine-readable output and `--dry-run` for safe preview.

---

## 2. File Synchronization

rclone-based file and folder synchronization to/from cloud providers and remote hosts.

Peer-to-peer sync between AMI nodes is handled by configuring the remote peer as an rclone SFTP remote (SSH access required). No special P2P logic — a peer is just another sync destination.

### 2.1 Sync Modes

- **R-SYNC-001**: rclone-based synchronization supporting all rclone backends. Remote definitions stored in rclone native config, managed via Ansible.
- **R-SYNC-002**: Unidirectional sync (`rclone sync`): make destination match source. Deletes from destination.
- **R-SYNC-003**: Copy mode (`rclone copy`): copy new/changed files. Never deletes from destination.
- **R-SYNC-004**: Bidirectional sync (`rclone bisync`): two-way sync. Conflict resolution: newer-wins by default, configurable per profile.
- **R-SYNC-005**: File filtering. Include/exclude patterns, min/max size, min/max age. Configurable per profile.
- **R-SYNC-006**: Bandwidth throttling via rclone `--bwlimit`. Configurable per profile.
- **R-SYNC-007**: Dry-run mode for all sync operations.

### 2.2 Sync Profiles

- **R-SYNC-008**: Named sync profiles defined in Ansible inventory:
  ```yaml
  dataops_sync_profiles:
    config-backup:
      source: /etc/ami/
      destination: gdrive:ami-backups/config/
      mode: sync
      schedule: "daily 02:00"
      filters:
        include: ["*.yml", "*.yaml", "*.toml", "*.conf"]
        exclude: ["*.tmp", "*.log"]
    project-mirror:
      source: /home/ami/projects/
      destination: s3:ami-projects/
      mode: copy
      schedule: "hourly"
      bandwidth: 10M
    peer-sync:
      source: /home/ami/shared/
      destination: sftp-node2:shared/
      mode: bisync
      schedule: "*/15 * * * *"
  ```
- **R-SYNC-009**: Scheduling via systemd timers, generated by Ansible from profile definitions.
- **R-SYNC-010**: Sync status reporting. Track bytes transferred, files added/modified/deleted, errors, and duration per profile. Expose as Prometheus metrics.

### 2.3 Transfer Protocol Support

- **R-TRANSFER-001**: rclone as the primary transfer engine. Covers S3, GCS, Azure, SFTP, FTP, HTTP, WebDAV natively.
- **R-TRANSFER-002**: rsync fallback for hosts that only expose the rsync wire protocol (rsyncd). Shell out to system rsync. Configured as `type: rsync` in the sync profile.
- **R-TRANSFER-003**: Transfer progress reporting with ETA, speed, and percentage.

### 2.4 Sync CLI

- **R-SCLI-001**: Subcommands under `ami-dataops sync`:
  - `sync run <profile>` -- execute a named sync profile
  - `sync run --all` -- execute all enabled profiles
  - `sync list` -- list configured profiles with last-run status
  - `sync dry-run <profile>` -- preview what would be transferred
  - `sync status` -- show last sync time, bytes, errors per profile

**Future work (not in scope for initial implementation):**
- rclone mount (FUSE) and rclone serve (expose backends as SFTP/FTP/HTTP/WebDAV/NFS). These are rclone access modes, not sync operations. They may be useful later but are not part of the sync subsystem.

---

## 3. Data Service Catalog and Instance Model

A YAML-driven catalog of data service types. Each type supports multiple named instances. Instances are defined in Ansible inventory using list-of-dicts per service type.

### 3.1 Catalog Structure

- **R-CATALOG-001**: Each service type defined as a YAML template in a `catalog/` directory. The catalog describes the type — image, ports, health check, exporter, backup method. Individual instances are defined in inventory, not the catalog:
  ```yaml
  # catalog/postgres.yml
  service:
    type: postgres
    description: "PostgreSQL relational database"
    category: relational
    default_image: pgvector/pgvector:pg16
    ports:
      main: 5432
    volumes:
      data: /var/lib/postgresql/data
    environment:
      POSTGRES_PASSWORD: "{{ instance.password }}"
    health_check:
      test: "pg_isready -U postgres"
      interval: 10s
      retries: 5
    exporter:
      image: prometheuscommunity/postgres-exporter:latest
      port_offset: 100   # exporter port = instance port + offset
      environment:
        DATA_SOURCE_NAME: "postgresql://postgres:{{ instance.password }}@{{ instance.container_name }}:5432/postgres?sslmode=disable"
    backup:
      method: pg_dump
    grafana_dashboard: 9628
    depends_on: []
  ```
- **R-CATALOG-002**: Instance lists in Ansible inventory. Each service type has a `dataops_{type}_instances` list-of-dicts. An empty list or absent variable means no instances of that type:
  ```yaml
  # host_vars/localhost/instances.yml
  dataops_postgres_instances:
    - name: main
      port: 5432
      image: pgvector/pgvector:pg16
      password: "{{ vault_postgres_main_password }}"
      init_scripts:
        - 01-keycloak.sql
      backup:
        databases: [postgres, keycloak]
        format: custom
        retention: { daily: 7, weekly: 4, monthly: 6 }

    - name: analytics
      port: 5433
      image: timescale/timescaledb:latest-pg16
      password: "{{ vault_postgres_analytics_password }}"
      backup:
        databases: [metrics]
        format: custom
        retention: { daily: 7, weekly: 4 }

  dataops_redis_instances:
    - name: cache
      port: 6379
      image: redis:8.6.1

  dataops_dgraph_instances: []  # none deployed
  ```
- **R-CATALOG-003**: Service version pinning. Each instance may override the catalog's `default_image` with an explicit `image` field. Upgrades are explicit inventory changes, never implicit `latest` pulls.
- **R-CATALOG-004**: Service dependency declarations. Services can declare dependencies at the type level (e.g., Keycloak depends on PostgreSQL). Compose generation resolves dependencies across instance lists — if Keycloak depends on PostgreSQL, at least one PostgreSQL instance must exist.

### 3.2 Instance Naming Convention

- **R-INSTANCE-001**: Deterministic naming from `{type}` and `{name}`:

  | Resource | Pattern | Example |
  |----------|---------|---------|
  | Compose service | `{type}-{name}` | `postgres-main` |
  | Container name | `ami-{type}-{name}` | `ami-postgres-main` |
  | Data volume | `ami-{type}-{name}-data` | `ami-postgres-main-data` |
  | Exporter service | `{type}-exporter-{name}` | `postgres-exporter-main` |
  | Exporter container | `ami-{type}-exporter-{name}` | `ami-postgres-exporter-main` |
  | systemd unit | `ami-{type}-{name}.service` | `ami-postgres-main.service` |
  | Borg repository | `borg/{type}-{name}/` | `borg/postgres-main/` |
  | Borgmatic config | `borgmatic.d/{type}-{name}.yml` | `borgmatic.d/postgres-main.yml` |

- **R-INSTANCE-002**: Instance names must be unique within a service type. Names are lowercase alphanumeric plus hyphens. Validated at inventory load time and by CLI add commands.

### 3.3 Port Allocation

- **R-PORT-001**: Static port allocation. Each instance declares an explicit port in inventory. No dynamic port assignment.
- **R-PORT-002**: Recommended port ranges per service type to avoid collisions:

  | Service Type | Base Port | Range |
  |-------------|-----------|-------|
  | PostgreSQL | 5432 | 5432-5439 |
  | Redis | 6379 | 6379-6389 |
  | MongoDB | 27017 | 27017-27027 |
  | Dgraph | 8081 | 8081-8089 |
  | Keycloak | 8082 | 8082-8089 |

  These are conventions, not enforced hard limits.
- **R-PORT-003**: Port conflict detection. When adding a new instance (via CLI or manual inventory edit), validate that the requested port is not already claimed by another instance across all service types. Report conflicts at Ansible render time and in CLI add commands.

### 3.4 Initial Catalog (Phase 1)

Catalog entries for services already in the existing Docker Compose stack:

| Service Type | Default Image | Category |
|-------------|--------------|----------|
| PostgreSQL | pgvector/pgvector:pg16 | relational |
| Redis | redis:8.6.1 | cache |
| Dgraph | dgraph/standalone:v25.2.0 | graph |
| MongoDB | mongo:8.2.5 | document |
| Prometheus | prom/prometheus:v3.10.0 | monitoring |
| OpenBao | openbao/openbao:2.4.4 | secrets |
| Keycloak | quay.io/keycloak/keycloak:26.1 | identity |
| Vaultwarden | vaultwarden/server:1.35.4 | secrets |
| SearXNG | searxng/searxng:2025.12.17 | search |

Prometheus serves double duty: it is both a data service (time-series storage for applications) and the monitoring infrastructure. A single Prometheus instance handles both. If scrape target count grows beyond what one instance handles, this can be revisited.

### 3.5 Extended Catalog (Future Work)

Additional service types to be cataloged incrementally. Each requires a YAML definition, health check, exporter config, and backup method.

| Category | Planned Services |
|----------|-----------------|
| Relational | MySQL, MariaDB, CockroachDB |
| Document | CouchDB |
| Graph | Neo4j, ArangoDB |
| Cache/KV | Valkey, DragonflyDB, Memcached |
| Object Storage | SeaweedFS, Garage |
| Time-Series | VictoriaMetrics, TimescaleDB, QuestDB |
| Vector | Qdrant, Milvus, Weaviate |
| Search | OpenSearch, Meilisearch, Typesense |
| Message Broker | Redpanda, RabbitMQ, NATS, Mosquitto |

These are not required for initial implementation. They are added to the catalog as needed.

### 3.6 Service Configuration

- **R-CONFIG-001**: Jinja2 configuration templates per service type (e.g., `postgresql.conf.j2`, `redis.conf.j2`). Rendered per-instance from the instance dict during deployment.
- **R-CONFIG-002**: Environment presets (dev, prod) that set resource limits and security settings. Applied per-instance — different instances of the same type can have different presets:
  ```yaml
  dataops_postgres_instances:
    - name: main
      preset: prod    # shared_buffers: 4GB, max_connections: 200, ssl: true
      ...
    - name: scratch
      preset: dev     # shared_buffers: 128MB, max_connections: 20
      ...
  ```
- **R-CONFIG-003**: Volume mount management. Ansible creates host directories with correct permissions before container start. Volume paths derived from instance naming convention.
- **R-CONFIG-004**: Network isolation. Services grouped by profile share a Docker network. Inter-profile communication requires explicit network attachment. All instances of a type share the same network profile.

---

## 4. Provisioning and Lifecycle (Ansible)

Ansible playbooks automate the full service lifecycle: deploy, configure, upgrade, and decommission. All operations are instance-aware.

### 4.1 Deployment

- **R-DEPLOY-001**: Single-command deployment: `make deploy` or `ami-dataops deploy`. Generates Docker Compose from catalog templates + instance lists, creates systemd units, starts services, runs health checks.
- **R-DEPLOY-002**: Profile-based deployment: `make deploy PROFILE=data` deploys only data-profile instances.
- **R-DEPLOY-003**: Per-instance deployment: `make deploy INSTANCE=postgres-main` deploys only that instance and its dependencies.
- **R-DEPLOY-004**: Post-deployment provisioning hooks. Instances can declare initialization tasks (e.g., `init_scripts` for PostgreSQL, Keycloak realm creation). Idempotent — safe to run repeatedly.
- **R-DEPLOY-005**: Docker Compose generation from catalog + instances. Ansible iterates over each `dataops_{type}_instances` list, renders the catalog template per instance, and produces the final `docker-compose.yml`. Users never edit Compose files directly. Example Jinja2 pattern:
  ```jinja2
  {% for instance in dataops_postgres_instances %}
  {{ instance.type | default('postgres') }}-{{ instance.name }}:
    container_name: ami-postgres-{{ instance.name }}
    image: {{ instance.image | default(catalog.postgres.default_image) }}
    ports:
      - "{{ instance.port }}:5432"
    volumes:
      - ami-postgres-{{ instance.name }}-data:/var/lib/postgresql/data
    environment:
      POSTGRES_PASSWORD: {{ instance.password }}
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 10s
      retries: 5
  {% endfor %}
  ```
- **R-DEPLOY-006**: Container runtime detection. Support both Docker and Podman (rootless). Auto-detect which is available.

### 4.2 Instance Management via CLI

- **R-INSTMGMT-001**: Add a new instance: `make add-<type> NAME=<name> PORT=<port> [IMAGE=<image>]`. This modifies `instances.yml` in the Ansible inventory (via ruamel.yaml for comment-preserving YAML round-trip), validates the name is unique and port is not conflicting, then optionally runs `make deploy INSTANCE=<type>-<name>` to bring it up.
  ```bash
  make add-postgres NAME=analytics PORT=5433 IMAGE=timescale/timescaledb:latest-pg16
  make add-redis NAME=sessions PORT=6380
  ```
- **R-INSTMGMT-002**: Remove an instance: `make remove-<type> NAME=<name>`. Stops the container, removes from compose, removes from `instances.yml`. Does NOT delete the data volume — requires explicit `--delete-data` flag.
- **R-INSTMGMT-003**: Configure an instance: `make configure-<type> NAME=<name> KEY=VALUE`. Updates a field in the instance's dict in `instances.yml`. For complex nested values (backup config, etc.), direct inventory editing is expected.
- **R-INSTMGMT-004**: List instances: `make list-instances` or `ami-dataops instances list`. Shows all instances across all types with name, port, image, and status (running/stopped).
- **R-INSTMGMT-005**: Instance YAML management uses ruamel.yaml for round-trip editing. Comments, ordering, and formatting in `instances.yml` are preserved across CLI modifications.

### 4.3 Upgrades

- **R-UPGRADE-001**: Version tracking. Record deployed image tags in a state file. `ami-dataops status` shows current vs. catalog/inventory version per instance.
- **R-UPGRADE-002**: Upgrade procedure per instance: pull new image, stop container, start new container, verify health. There is brief downtime per instance — this is a single-node system, not a zero-downtime cluster.
- **R-UPGRADE-003**: Database upgrade safety. Before upgrading a stateful instance: take backup, verify backup integrity, then proceed with upgrade. If health checks fail after upgrade, the backup is available for manual restore. Automatic rollback is not feasible for database major version upgrades (data directory format changes are irreversible).
- **R-UPGRADE-004**: Upgrade dry-run. Show what would be upgraded without executing.

### 4.4 Maintenance

- **R-MAINT-001**: Service restart: `ami-dataops restart <type-name>` (e.g., `restart postgres-main`) or `ami-dataops restart --all`.
- **R-MAINT-002**: Service logs: `ami-dataops logs <type-name> [--follow] [--tail N]`.
- **R-MAINT-003**: Volume cleanup. Identify and optionally remove orphaned Docker volumes not attached to any running instance.
- **R-MAINT-004**: Image cleanup. Remove unused Docker images to reclaim disk space.
- **R-MAINT-005**: Health check on demand: `ami-dataops health` probes all instances and reports status.

---

## 5. Monitoring and Alerting

Automated monitoring stack deployment alongside data services. All monitoring is instance-aware — each instance gets its own exporter, scrape target, and alert rules.

### 5.1 Prometheus Exporters

- **R-MON-001**: Auto-deploy the appropriate Prometheus exporter as a sidecar container for each instance. Exporter image defined in the catalog, port derived from instance port + offset. Each exporter targets its specific instance.
- **R-MON-002**: Auto-generate Prometheus scrape configuration from all deployed instances. Each instance's exporter is a separate scrape target with labels:
  ```yaml
  - job_name: postgres
    static_configs:
      - targets: ['ami-postgres-exporter-main:9187']
        labels:
          instance_name: main
          service_type: postgres
      - targets: ['ami-postgres-exporter-analytics:9187']
        labels:
          instance_name: analytics
          service_type: postgres
  ```
- **R-MON-003**: Host-level monitoring via Node Exporter (CPU, memory, disk, network) and cAdvisor (container-level metrics). Deployed when monitoring profile is enabled.

### 5.2 Grafana Dashboards

- **R-DASH-001**: Deploy Grafana as part of the monitoring profile. Auto-provision Prometheus as a data source.
- **R-DASH-002**: Auto-provision Grafana dashboards for each service type from the Grafana dashboard library (by dashboard ID in catalog entry). Dashboards support filtering by `instance_name` label to view individual instances.
- **R-DASH-003**: System overview dashboard showing all instances: health status, resource usage, backup status, last backup time. Grouped by service type.
- **R-DASH-004**: Dashboard provisioning via Grafana's file-based provisioning (JSON files in a provisioning directory). Version-controlled, not manual import.

### 5.3 Alerting

- **R-ALERT-001**: Deploy Alertmanager as part of the monitoring profile. Configure routing, receivers, and inhibition rules via Ansible inventory variables.
- **R-ALERT-002**: Per-instance alert rules:
  - Instance down (target unreachable for > 5m)
  - High resource usage (CPU > 90%, memory > 85%, disk > 80%)
  - Backup failure or backup age exceeded retention window
  - Connection pool exhaustion (where applicable)
  - Slow queries (where applicable)
  Alert rules use `instance_name` and `service_type` labels to identify which instance is affected.
- **R-ALERT-003**: Notification channels: email (SMTP), Slack webhook, Discord webhook, generic webhook, healthchecks.io. Configurable per alert severity.
- **R-ALERT-004**: Alert silencing and maintenance windows via Alertmanager silence API.

---

## 6. CLI Interface

Unified command-line interface for all data operations. All commands that target services use `{type}-{name}` instance identifiers (e.g., `postgres-main`, `redis-cache`).

- **R-CLI-001**: Single entry point: `ami-dataops <command> [options]`.
- **R-CLI-002**: Top-level commands:

  | Command | Description | Defined in |
  |---------|-------------|------------|
  | `deploy` | Deploy instances | Section 4.1 |
  | `stop` | Stop instances | Section 4.4 |
  | `restart` | Restart instances | Section 4.4 |
  | `status` | Show instance status | Section 4.4 |
  | `health` | Run health checks | Section 4.4 |
  | `logs` | View instance logs | Section 4.4 |
  | `upgrade` | Upgrade instances | Section 4.3 |
  | `backup` | Backup operations | Section 1.4 |
  | `sync` | Sync operations | Section 2.4 |
  | `catalog` | List/inspect available service types | Section 3.1 |
  | `instances` | Instance management (list, inspect) | Section 4.2 |

  Subcommand details are defined in their respective sections above. This table is the authoritative index — no command definitions are duplicated.

- **R-CLI-003**: Global flags: `--json` (machine-readable output), `--dry-run` (preview), `--verbose` / `-v` (debug logging).

---

## 7. Configuration

### 7.1 Source of Truth

- **R-CFG-001**: **Ansible inventory is the single source of truth.** All configuration — instance lists, backup schedules, sync profiles, monitoring settings, credentials — lives in Ansible inventory variables under `host_vars/`. No separate `dataops.yml` configuration file.
- **R-CFG-002**: Secrets encrypted with Ansible Vault. Database passwords, API keys, rclone credentials, and borg encryption passphrases are vault-encrypted in the inventory.
- **R-CFG-003**: Runtime secrets injected via environment variables. Ansible renders `.env` files from vault-encrypted values during deployment. Docker Compose references these via `${VAR}` syntax.
- **R-CFG-004**: rclone remote definitions generated by Ansible from inventory variables into rclone config format at deploy time.
- **R-CFG-005**: Environment variable overrides for local development only. Convention: `DATAOPS_<KEY>`. These override inventory defaults when running CLI commands directly (not via Ansible). Ansible inventory always wins during deployment.

### 7.2 Instance Inventory File

- **R-CFG-006**: Per-host instance registry at `host_vars/localhost/instances.yml`. Contains all `dataops_{type}_instances` lists. This file is the canonical record of what is deployed on a host.
- **R-CFG-007**: CLI commands (`make add-*`, `make remove-*`, `make configure-*`) modify `instances.yml` via ruamel.yaml. Manual editing is also supported. Ansible reads this file at deploy time.
- **R-CFG-008**: Instance inventory validation at deploy time. Ansible checks: unique names within type, no port conflicts across all types, required fields present (name, port), image tag is not `latest`.

---

## 8. Existing Infrastructure

Components that already exist in the AMI-AGENTS workspace.

**Migrate into AMI-DATAOPS:**

| Component | Current Location | Action |
|-----------|-----------------|--------|
| Backup/restore CLI | `ami/scripts/backup/` (AMI-AGENTS) | Move into AMI-DATAOPS, extend with rclone transport |

**Integrate with (shared, not owned by DATAOPS):**

| Component | Location | Integration |
|-----------|----------|-------------|
| Ansible inventory | `ansible/inventory/` | Add `instances.yml` and dataops variables to existing host_vars |
| Systemd status display | `ami/cli_components/status_systemd.py` | Register dataops instances for TUI status |

**Owned by AMI-DATAOPS (evolve in place):**

| Component | Location | Evolution |
|-----------|----------|-----------|
| Docker Compose stack | `res/docker/docker-compose.yml` | Migrate to Jinja2 template generated from catalog + instance lists |
| Ansible lifecycle playbook | `res/ansible/compose.yml` | Extend with instance iteration, monitoring, backup tasks |

**Reference only (patterns to study, no code sharing):**

| Pattern | Location | Lesson |
|---------|----------|--------|
| AMI-STREAMS Alertmanager | `projects/AMI-STREAMS/alertmanager/` | Alert rule file format, systemd unit for containerized Alertmanager |
| AMI-STREAMS MDAD deployment | `projects/AMI-STREAMS/ansible/` | Variable-driven service enablement, tag-gated Ansible execution |

---

## 9. Non-Requirements (Out of Scope)

- **Application-level persistence abstractions** (DAO, ORM, query builders). Applications use native database clients directly.
- **Custom PaaS or deployment platform**. AMI-DATAOPS is a CLI toolkit and Ansible playbook collection, not Coolify or Dokploy.
- **Kubernetes.** Target is single-node Docker/Podman with systemd.
- **Multi-node replication or clustering.** Single-node deployment. No replica sets, no replication lag monitoring, no horizontal scaling.
- **Custom monitoring agents.** Use Prometheus + existing exporters.
- **Custom transfer protocols.** Use rclone and rsync.
- **Zero-downtime upgrades.** Single-node means brief downtime during upgrades. This is acceptable.
- **Dynamic port allocation.** Ports are always explicitly declared per instance.

---

## 10. Implementation Priorities

| Priority | Subsystem | Rationale |
|----------|-----------|-----------|
| P0 | Migrate backup CLI into DATAOPS, extend with rclone (1.1, 1.4) | Immediate value, existing code |
| P0 | Service catalog YAML structure (3.1) + instance model (3.2, 3.3) + initial 9 types (3.4) | Foundation for all automation |
| P0 | Instance management CLI (`make add-*`, `make remove-*`, `make list-instances`) (4.2) | Enables self-service instance provisioning |
| P1 | Database backup via borgmatic, per-instance (1.2) | Protect production data |
| P1 | Docker volume backup (1.3) | Physical backup safety net |
| P1 | Cloud sync profiles (2.1, 2.2, 2.4) | rclone-based file sync with scheduling |
| P1 | Monitoring exporters + Grafana, instance-aware (5.1, 5.2) | Visibility into deployed instances |
| P2 | Catalog-driven compose generation with instance iteration (4.1) | Replace hand-written compose |
| P2 | Alerting with per-instance rules (5.3) | Alertmanager + per-instance rules |
| P2 | Upgrade automation (4.3) | Version tracking, safe upgrades |
| P3 | Extended catalog entries (3.5) | Add service types as needed |
| P3 | Maintenance commands (4.4) | Log viewing, cleanup, health |
| P3 | rsync fallback (2.3) | Edge case for rsync-only hosts |

---

## 11. Tool Inventory

Tools AMI-DATAOPS orchestrates (USE, do not rebuild):

| Tool | Purpose | Integration |
|------|---------|-------------|
| **rclone** | Cloud sync, transfer, encryption | Transport backend for all backup and sync operations |
| **borgmatic + borg** | Database-aware backup with deduplication | Per-instance config generation from inventory, Ansible tasks |
| **Ansible** | Infrastructure automation | Playbooks, inventory (source of truth), vault, Jinja2 templates |
| **Docker Compose** | Container orchestration | Generated from catalog templates + instance lists |
| **systemd** | Service and timer lifecycle | Per-instance service units, timer units for scheduling |
| **Prometheus** | Metrics collection | Per-instance scrape config with instance labels |
| **Grafana** | Dashboards | File-based dashboard provisioning with instance filtering |
| **Alertmanager** | Alert routing and notification | Per-instance rule and receiver config generation |
| **Node Exporter** | Host metrics | Auto-deployed sidecar |
| **cAdvisor** | Container metrics | Auto-deployed sidecar |
| **ruamel.yaml** | YAML round-trip editing | CLI instance management preserves comments and formatting |

---

## References

- [rclone documentation](https://rclone.org/docs/)
- [borgmatic documentation](https://torsion.org/borgmatic/)
- [Prometheus exporters](https://prometheus.io/docs/instrumenting/exporters/)
- [Grafana dashboard library](https://grafana.com/grafana/dashboards/)
- [matrix-docker-ansible-deploy](https://github.com/spantaleev/matrix-docker-ansible-deploy) -- reference architecture for Ansible + Docker service deployment
