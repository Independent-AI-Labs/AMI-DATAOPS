# AMI-DATAOPS

Data operations toolkit for AMI infrastructure. Backup create/restore (Google Drive), Docker Compose service deployment via Ansible, and Keycloak realm/client provisioning. Sync, monitoring dashboards, alerting, and multi-instance management are planned but not yet implemented.

## Compose Stack

Managed via Ansible + Docker Compose + systemd.

```bash
make compose-deploy    # Deploy all services and enable on boot
make compose-stop      # Stop compose stack
make compose-restart   # Restart compose stack
make compose-status    # Show service status
```

## Services

| Service | Version | Profile | Port |
|---------|---------|---------|------|
| PostgreSQL (pgvector) | 16 | data | 5432 |
| Redis | 8.6.1 | data | 6379 |
| Dgraph | v25.3.0 | data | 8081 |
| MongoDB | 8.2.5 | data | 27017 |
| Prometheus | 3.10.0 | data | 9091 |
| OpenBao | 2.4.4 | secrets | 8200 |
| Keycloak | 26.1 | secrets | 8082 |
| Vaultwarden | 1.35.4 | secrets | 8083 |
| OpenVPN | latest | secrets | host |
| SearXNG | 2025.12.17-896863802 | dev | 8888 |

## Development

```bash
make install           # Full install: Python deps + pre-commit hooks
make lint              # Ruff linter + format check
make test              # Run tests
make check             # All checks (lint + type-check + test)
```

## Configuration

AMI-DATAOPS is a `uv` workspace member of AMI-AGENTS. It must be cloned inside the AMI-AGENTS repo at `projects/AMI-DATAOPS`.
