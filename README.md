# AMI-DATAOPS

Polyglot persistence framework for AMI services. Provides a unified async CRUD interface across 8 storage backends with built-in security, field-level encryption, and multi-storage orchestration.

## Architecture

```
ami/
├── core/               # Abstract DAO, factory, exception hierarchy, storage types
├── models/             # StorageModel base, security mixins, storage config
├── implementations/    # Backend-specific DAO implementations
│   ├── sql/            # PostgreSQL (asyncpg)
│   ├── graph/          # Dgraph (gRPC/pydgraph)
│   ├── mem/            # Redis (redis.asyncio)
│   ├── vec/            # PgVector (asyncpg + pgvector)
│   ├── rest/           # REST/HTTP APIs (aiohttp)
│   ├── vault/          # OpenBao/Vault KV-v2 (httpx)
│   └── timeseries/     # Prometheus (HTTP + remote-write)
├── security/           # Field-level encryption, PII masking, key management
├── secrets/            # Vault-backed sensitive field storage
├── services/           # Decorators (caching, events, sensitive fields)
├── storage/            # Registry, config discovery, validation
└── utils/              # HTTP retry client
```

## Storage Backends

| Backend | StorageType | Driver | Port | Capabilities |
|---------|-------------|--------|------|-------------|
| PostgreSQL | `RELATIONAL` | asyncpg | 5432 | ACID, schema introspection, bulk ops |
| Dgraph | `GRAPH` | pydgraph (gRPC) | 9080 | Traversal, shortest path, k-hop, components |
| Redis | `INMEM` | redis.asyncio | 6379 | TTL, key scanning, prefix namespacing |
| PgVector | `VECTOR` | asyncpg + pgvector | 5432 | Cosine similarity, HNSW indexes, auto-embedding |
| REST | `REST` | aiohttp | 443 | Bearer/API-key auth, field mapping, envelope extraction |
| OpenBao | `VAULT` | httpx | 8200 | KV-v2 secrets, versioning, path traversal protection |
| Prometheus | `TIMESERIES` | httpx | 9090 | PromQL queries, remote-write, series discovery |

## Core Abstractions

### BaseDAO

Abstract interface for all storage implementations. Defines connection lifecycle, full CRUD contract, bulk operations, schema introspection, and raw queries.

### StorageModel

Pydantic base class for all persisted entities. Provides UUID v7 identity, multi-storage config, DAO factory access, and serialization. Models declare their storage backends via `ModelMetadata`:

```python
class MyModel(StorageModel):
    name: str
    _model_meta = ModelMetadata(
        storage_configs={
            "primary": StorageConfig(storage_type=StorageType.RELATIONAL),
            "cache": StorageConfig(storage_type=StorageType.INMEM, ttl_seconds=3600),
        }
    )
```

### UnifiedCRUD

Persistence orchestrator that routes operations to the correct DAO based on model config. Caches DAO instances per (model, config) with event-loop safety detection.

### DAOFactory

Registry-based factory. Backends register via `register_dao(StorageType, DAOClass)` and are instantiated on demand.

## Security

### Access Control
- ACL-based permissions with DENY-first evaluation
- Role-based access (user, group, service principals)
- Expiring permission grants
- Row-level security for tenant isolation
- Data classification levels: PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED, TOP_SECRET

### Encryption
- Field-level Fernet encryption with PBKDF2-derived per-field keys
- PII auto-detection and masking (SSN, credit card, email, phone, address)
- Vault-backed sensitive field storage with integrity hashing
- Transparent encrypt-on-write, decrypt-on-read

### Audit
- Created/modified/accessed tracking on secured models
- Event recording via `@record_event` decorator
- Security context propagation

## Services

| Decorator | Purpose |
|-----------|---------|
| `@sensitive_field` | Mark fields for vault storage with masking and classification |
| `@record_event` | Capture function calls as auditable events |
| `@cached_result` | TTL-based result caching with custom key functions |
| `sanitize_for_mcp()` | Mask sensitive fields for MCP tool output |

## Configuration

AMI-DATAOPS is a `uv` workspace member of AMI-AGENTS. It must be cloned inside the AMI-AGENTS repo at `projects/AMI-DATAOPS`.

### Dependencies

**Runtime**: pydantic, asyncpg, pydgraph, redis, aiohttp, httpx, cryptography, uuid-utils, loguru

**Dev**: pytest, pytest-asyncio, pytest-cov, mypy, ruff, pre-commit

## Development

```bash
make install          # Full install: Python deps + pre-commit hooks
make lint             # Ruff linter + format check
make type-check       # mypy
make test             # All tests
make test-unit        # Unit tests only
make test-cov         # Tests with coverage report
make check            # All checks (lint + type-check + test)
```

## Tests

```
tests/
├── unit/
│   ├── test_security.py           # ACL, permission checking
│   ├── test_permission_deny.py    # DENY-first evaluation
│   ├── test_exceptions.py         # Exception hierarchy
│   ├── test_decorators.py         # Event recording, caching, sensitive fields
│   └── test_dgraph_injection.py   # Graph query injection prevention
└── integration/
    └── ...                        # Backend integration tests
```

Coverage thresholds enforced by pre-push hooks: Unit >90%, Integration >50%.
