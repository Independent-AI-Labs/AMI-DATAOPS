# AMI-DATAOPS Architecture Revision

**Date:** 2026-02-13
**Status:** Proposed
**Severity Legend:** CRITICAL = broken/security hole, HIGH = design flaw, MEDIUM = tech debt, LOW = polish

---

## Issue Index

| # | Severity | Title | Files Affected |
|---|----------|-------|----------------|
| 1 | CRITICAL | DAO registry never populated -- factory is dead code | `core/dao.py`, all DAO `__init__.py` |
| 2 | CRITICAL | OpenBaoDAO imports nonexistent package | `vault/openbao_dao.py` |
| 3 | CRITICAL | Test code ships in production module | `embedding_service.py` |
| 4 | CRITICAL | `bulk_delete()` return type varies across backends | `core/dao.py`, all DAOs |
| 5 | CRITICAL | Threading lock in async codebase | `secrets/client.py` |
| 6 | HIGH | StorageModel is a god class | `models/base_model.py`, mixins |
| 7 | HIGH | Async `load_related()` unreachable from sync descriptor | `core/graph_relations.py` |
| 8 | HIGH | Redis raw query parses unsanitized strings | `mem/redis_dao.py` |
| 9 | HIGH | BaseDAO mandates 23 abstract methods -- most are no-ops | `core/dao.py` |
| 10 | HIGH | DAO cache uses event loop identity -- fragile | `core/unified_crud.py` |
| 11 | MEDIUM | GraphQueryBuilder interpolates unescaped values into DQL | `core/graph_relations.py` |
| 12 | MEDIUM | Bulk ops are sequential loops, not batched | All DAOs |
| 13 | MEDIUM | YAML cache has no invalidation path | `models/storage_config_factory.py` |
| 14 | MEDIUM | PostgreSQL silently swallows hydration errors | `sql/postgresql_dao.py` |
| 15 | MEDIUM | Connection strings built with unescaped passwords | `models/storage_config.py` |
| 16 | MEDIUM | Prometheus DAO has update/delete on append-only DB | `timeseries/prometheus_dao.py` |
| 17 | MEDIUM | No shared connection pool across DAOs | All DAOs |
| 18 | MEDIUM | SecretPointerRecord hardcodes postgres backend | `models/secret_pointer.py` |
| 19 | LOW | No public API surface -- empty `__init__.py` everywhere | All `__init__.py` |
| 20 | LOW | TokenEncryption returns sentinel string instead of raising | `security/encryption.py` |
| 21 | LOW | secured_mixin has complex logic with minimal tests | `models/secured_mixin.py` |
| 22 | LOW | REST DAO guesses envelope patterns | `rest/rest_dao.py` |

---

## CRITICAL Issues

### 1. DAO Registry Never Populated -- Factory Is Dead Code

**Problem:**
`core/dao.py` defines a global registry and factory:

```python
_dao_registry: dict[StorageType, type[BaseDAO]] = {}

def register_dao(storage_type, dao_class):
    _dao_registry[storage_type] = dao_class

def get_dao_class(storage_type):
    if storage_type not in _dao_registry:
        raise StorageError(f"No DAO registered for {storage_type}")
    return _dao_registry[storage_type]
```

No implementation file ever calls `register_dao()`. The dict stays empty. `DAOFactory.create()` calls `get_dao_class()` which always raises. `UnifiedCRUD` depends on `DAOFactory` -- so the entire CRUD facade is broken.

**Root Cause:** Registration was supposed to happen at import time in each DAO module, but was never wired up.

**Fix:**
Each DAO module auto-registers on import. Add to each `__init__.py`:

```python
# ami/implementations/graph/__init__.py
from ami.core.dao import register_dao
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO

register_dao(StorageType.GRAPH, DgraphDAO)
```

Repeat for all 7 backends:
- `graph/__init__.py` -- `StorageType.GRAPH` -> `DgraphDAO`
- `mem/__init__.py` -- `StorageType.INMEM` -> `RedisDAO`
- `sql/__init__.py` -- `StorageType.RELATIONAL` -> `PostgreSQLDAO`
- `vec/__init__.py` -- `StorageType.VECTOR` -> `PgVectorDAO`
- `timeseries/__init__.py` -- `StorageType.TIMESERIES` -> `PrometheusDAO`
- `rest/__init__.py` -- `StorageType.REST` -> `RestDAO`
- `vault/__init__.py` -- `StorageType.VAULT` -> `OpenBaoDAO`

Then ensure `ami/implementations/__init__.py` imports all sub-packages so registration runs on first import.

**Verification:** `get_dao_class(StorageType.GRAPH)` returns `DgraphDAO`.

---

### 2. OpenBaoDAO Imports Nonexistent Package

**Problem:**
`vault/openbao_dao.py` line 26:
```python
from openbao import Client as _Cli
from openbao.exceptions import OpenBaoError as _Err
```

There is no `openbao` package on PyPI or in the project dependencies. The import always fails. `OpenBaoClient` stays `None`. `connect()` always raises. The entire vault DAO is a no-op.

**Root Cause:** The external client library was assumed to exist but was never published or identified. The original code used `importlib` to import from `ami.secrets.client` but that module has `SecretsBrokerClient`, not `OpenBaoClient`.

**Fix (Option A -- use hvac):**
`hvac` is the standard HashiCorp Vault Python client that also works with OpenBao (API-compatible). Replace:

```python
# Before
from openbao import Client as _Cli
from openbao.exceptions import OpenBaoError as _Err

# After
from hvac import Client as _Cli
from hvac.exceptions import VaultError as _Err

OpenBaoClient = _Cli
OpenBaoError = _Err
```

Add `hvac` as optional dependency in `pyproject.toml`:
```toml
[project.optional-dependencies]
vault = ["hvac>=2.1.0"]
```

Add to mypy overrides:
```toml
[[tool.mypy.overrides]]
module = ["hvac", "hvac.*"]
ignore_missing_imports = true
```

**Fix (Option B -- use SecretsBrokerClient from secrets layer):**
The `ami/secrets/client.py` already has `HTTPSecretsBrokerBackend` that talks to Vault-like APIs. Refactor `OpenBaoDAO` to use `SecretsBrokerClient` as its backend instead of a raw Vault client.

**Recommendation:** Option A. `hvac` is mature (3k+ GitHub stars), maintained, and API-compatible with both HashiCorp Vault and OpenBao.

---

### 3. Test Code Ships in Production Module

**Problem:**
`embedding_service.py` lines 200-236 contain:
```python
class TestEmbeddingService(EmbeddingService):
    """Test-only embedding service that returns deterministic vectors."""
    ...

def build_test_embedding_service(embedding_dim=32):
    """Factory helper for tests that require deterministic embeddings."""
    ...
```

This is test infrastructure in a production file. It gets deployed, adds attack surface (deterministic vectors), and violates separation of concerns.

**Fix:**
Move to `tests/unit/conftest.py` or `tests/helpers/embedding.py`:

```python
# tests/unit/conftest.py
from ami.implementations.embedding_service import EmbeddingService

class TestEmbeddingService(EmbeddingService):
    ...
```

Remove from `embedding_service.py`.

---

### 4. `bulk_delete()` Return Type Varies Across Backends

**Problem:**
`BaseDAO` abstract signature:
```python
async def bulk_delete(self, ids: list[str]) -> dict[str, Any] | int:
```

Actual returns:
- `PostgreSQLDAO` -> `int` (deleted count)
- `RedisDAO` -> `int` (deleted count)
- `DgraphDAO` -> `dict[str, Any]` (detailed result)
- `PrometheusDAO` -> `int`
- `RestDAO` -> `int`
- `OpenBaoDAO` -> `int`

The union return type makes callers write defensive code. Polymorphism is broken -- you can't treat all DAOs uniformly.

**Fix:**
Standardize to `int` (deleted count). This is what every caller actually needs.

```python
# core/dao.py
@abstractmethod
async def bulk_delete(self, ids: list[str]) -> int:
    """Bulk delete records. Returns number of successfully deleted records."""

# DgraphDAO -- extract count from result dict
async def bulk_delete(self, ids: list[str]) -> int:
    ...
    return deleted_count
```

---

### 5. Threading Lock in Async Codebase

**Problem:**
`secrets/client.py` line 86:
```python
class InMemorySecretsBackend:
    def __init__(self, master_key=None):
        self._lock = threading.Lock()

    def ensure_secret(self, ...):
        with self._lock:  # BLOCKS EVENT LOOP
            ...
```

`threading.Lock` blocks the calling thread. In an async application, this blocks the entire event loop during contention. Every coroutine waiting for I/O is frozen.

**Root Cause:** The `InMemorySecretsBackend` methods are synchronous (not `async def`). They use threading primitives because the `SecretsBrokerBackend` Protocol defines sync methods.

**Fix:**
Make the Protocol async:

```python
class SecretsBrokerBackend(Protocol):
    async def ensure_secret(self, ...) -> VaultFieldPointer: ...
    async def retrieve_secret(self, reference: str) -> tuple[str, str]: ...
    async def delete_secret(self, reference: str) -> None: ...
```

Use `asyncio.Lock` in `InMemorySecretsBackend`:
```python
class InMemorySecretsBackend:
    def __init__(self, master_key=None):
        self._lock = asyncio.Lock()

    async def ensure_secret(self, ...):
        async with self._lock:
            ...
```

Update all call sites to `await` the broker methods.

---

## HIGH Issues

### 6. StorageModel Is a God Class

**Problem:**
`StorageModel` inherits from `SecuredModelMixin` (ACL, audit, 233 lines) + `StorageConfigMixin` + `BaseModel`. Every model in the system carries:
- ACL permission checking
- Access audit logging (100-entry history)
- Ownership and tenant isolation
- Data classification
- Vault pointer hydration
- DAO factory methods
- Storage serialization

A simple data model like a metric label or a config record doesn't need ACL, audit trails, or vault integration.

**Fix:**
Split into composable layers:

```python
class StorageModel(BaseModel):
    """Base: just uid, timestamps, serialization."""
    uid: str | None = Field(default_factory=...)
    updated_at: datetime | None = ...

    def to_storage_dict(self) -> dict[str, Any]: ...
    def from_storage_dict(cls, data) -> StorageModel: ...

class SecuredStorageModel(StorageModel, SecuredModelMixin):
    """Add security for models that need it."""
    pass

class VaultStorageModel(SecuredStorageModel):
    """Add vault integration for models with sensitive fields."""
    pass
```

Models opt into the features they need. Most implementation models use plain `StorageModel`.

---

### 7. Async `load_related()` Unreachable From Sync Descriptor

**Problem:**
`RelationalField` is a Python descriptor:
```python
class RelationalField:
    def __get__(self, obj, objtype=None):
        # Sync method -- cannot await
        ...
        return cached_or_ids

    async def load_related(self, obj, dao):
        # Async -- but how does anyone call this?
        ...
```

`__get__` is sync (Python descriptors must be). It returns cached values or raw IDs. To actually load related objects, callers must manually call `await field.load_related(obj, dao)` -- but they need a reference to the descriptor instance, which means `Model.__dict__['field_name'].load_related(instance, dao)`. This is awkward and non-obvious.

**Fix:**
Replace with an explicit async method on the model or a loader service:

```python
class GraphLoader:
    """Explicit async loader for graph relations."""

    async def load_relations(self, instance: StorageModel, dao: BaseDAO,
                             relations: list[str] | None = None) -> None:
        schema = GraphSchemaAnalyzer.analyze_model(type(instance))
        for field_name, edge_config in schema["edges"].items():
            if relations and field_name not in relations:
                continue
            # Load and set on instance
            ...
```

Usage becomes explicit: `await loader.load_relations(user, dao, ["posts", "comments"])`.

---

### 8. Redis Raw Query Parses Unsanitized Strings

**Problem:**
`redis_dao.py` line 253:
```python
async def raw_read_query(self, query: str, params=None):
    parts = query.split()
    command = parts[0].upper()
    if command == "KEYS":
        pattern = parts[1] if len(parts) > 1 else "*"
        keys = await self.client.keys(pattern)
```

The `KEYS` command with a user-supplied pattern can enumerate the entire keyspace. While whitelisted to `GET/KEYS/INFO`, this still allows:
- `KEYS *` -- enumerate all keys
- `KEYS secret:*` -- enumerate secret keys
- `KEYS` runs in O(N) and blocks Redis for large datasets

**Fix:**
- Prefix-scope all KEYS queries: `await self.client.keys(f"{self._key_prefix}{pattern}")`
- Use `SCAN` instead of `KEYS` to avoid blocking
- Validate pattern against injection characters

```python
if command == "KEYS":
    pattern = parts[1] if len(parts) > 1 else "*"
    # Scope to this collection's prefix
    scoped = f"{self._key_prefix}{pattern}"
    cursor = 0
    keys = []
    while True:
        cursor, batch = await self.client.scan(cursor, match=scoped, count=100)
        keys.extend(batch)
        if cursor == 0:
            break
    return [{"key": key} for key in keys]
```

---

### 9. BaseDAO Mandates 23 Abstract Methods -- Most Are No-Ops

**Problem:**
Every DAO must implement all 23 methods:
- 3 connection lifecycle
- 8 CRUD
- 3 bulk operations
- 2 raw queries
- 7 schema introspection

Results:
- `OpenBaoDAO.get_model_indexes()` returns `[]`
- `OpenBaoDAO.create_indexes()` is `pass`
- `PrometheusDAO` returns empty lists for most introspection
- `RedisDAO.get_model_indexes()` returns `[]`

**Fix:**
Split into focused interfaces with default implementations:

```python
class BaseDAO(ABC):
    """Core: connection + basic CRUD only."""
    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def disconnect(self) -> None: ...
    @abstractmethod
    async def create(self, instance: Any) -> str: ...
    @abstractmethod
    async def find_by_id(self, item_id: str) -> Any | None: ...
    @abstractmethod
    async def update(self, item_id: str, data: dict) -> None: ...
    @abstractmethod
    async def delete(self, item_id: str) -> bool: ...

    # Default implementations (override if supported)
    async def test_connection(self) -> bool:
        return True

    async def find_one(self, query: dict) -> Any | None:
        results = await self.find(query, limit=1)
        return results[0] if results else None

    async def bulk_create(self, instances: list) -> list[str]:
        return [await self.create(i) for i in instances]

    async def create_indexes(self) -> None:
        pass  # No-op by default

    async def get_model_indexes(self, path, **kw) -> list[dict]:
        return []  # No-op by default

    # ... other defaults
```

Backends override only what they actually support.

---

### 10. DAO Cache Uses Event Loop Identity -- Fragile

**Problem:**
`unified_crud.py` caches DAOs keyed by `(model_class, config_index)` and tracks which event loop each DAO was created in. If the loop changes (e.g., test teardown, uvicorn reload), it disconnects and evicts.

```python
async def _evict_if_loop_changed(self, cache_key, current_loop):
    cached_loop = self._dao_loop_cache.get(cache_key)
    if cached_loop is not None and (cached_loop.is_closed() or cached_loop is not current_loop):
        # Evict
```

Issues:
- `is` identity comparison on loops is unreliable across frameworks
- Closed loop eviction has a race condition -- the DAO may still have pending operations
- No maximum cache size -- memory leak if many model/config combos are used

**Fix:**
Replace with explicit lifecycle management:

```python
class DAOPool:
    """Explicit DAO lifecycle manager."""

    async def get(self, model_cls: type, config: StorageConfig) -> BaseDAO:
        """Get or create a connected DAO."""
        ...

    async def close_all(self) -> None:
        """Disconnect all cached DAOs. Call on shutdown."""
        ...

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close_all()
```

Usage: `async with DAOPool() as pool: dao = await pool.get(MyModel, config)`

No event loop tracking needed -- the pool is scoped to the application lifecycle.

---

## MEDIUM Issues

### 11. GraphQueryBuilder Interpolates Unescaped Values Into DQL

**Problem:**
```python
def build(self) -> str:
    for f in self.filters:
        func_parts.append(f'eq({f["field"]}, "{f["value"]}")')
```

If `f["value"]` contains `"` or `)`, the DQL is malformed or injectable.

**Fix:**
Escape special characters or use Dgraph variables:
```python
def _escape_dql_value(value: str) -> str:
    return value.replace('\\', '\\\\').replace('"', '\\"')
```

---

### 12. Bulk Ops Are Sequential Loops, Not Batched

**Problem:**
Every `bulk_create`:
```python
async def bulk_create(self, instances):
    ids = []
    for instance in instances:
        result = await self.create(instance)  # One round trip per item
        ids.append(result)
    return ids
```

**Fix per backend:**
- **PostgreSQL:** Use `executemany()` or `COPY`
- **Redis:** Use `pipeline()` for atomic batch
- **Dgraph:** Single mutation with multiple nquads
- **REST:** POST array if API supports batch endpoint

---

### 13. YAML Cache Has No Invalidation Path

**Problem:**
`_yaml_cache = {"data": None}` -- once loaded, never cleared.

**Fix:**
Add `invalidate_yaml_cache()` and call it from `StorageRegistry.refresh()`:
```python
def invalidate_yaml_cache() -> None:
    _yaml_cache["data"] = None
```

---

### 14. PostgreSQL Silently Swallows Hydration Errors

**Problem:**
```python
def _row_to_model(self, row):
    try:
        return self.model_cls.from_storage_dict(row)
    except Exception:
        logger.debug("Could not hydrate...")
        return row  # Returns raw dict instead of model
```

Callers expect `StorageModel` instances but silently get `dict`.

**Fix:**
Log at WARNING level. Return the dict but also set a flag or use a wrapper type so callers know hydration failed. Or raise -- callers that want raw dicts can use `raw_read_query()`.

---

### 15. Connection Strings Built With Unescaped Passwords

**Problem:**
```python
f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"
```

Password `p@ss:w/rd` produces malformed URL.

**Fix:**
```python
from urllib.parse import quote_plus
f"postgresql+asyncpg://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}"
```

---

### 16. Prometheus DAO Has Update/Delete on Append-Only DB

**Problem:**
Semantically misleading. Prometheus doesn't support updates or deletes in normal operation.

**Fix:**
Raise `NotImplementedError` with clear message instead of silently appending or failing:
```python
async def update(self, item_id, data):
    msg = "Prometheus is append-only. Use create() to write new samples."
    raise NotImplementedError(msg)
```

Or better: define a `TimeSeriesDAO` interface that doesn't include `update`/`delete`.

---

### 17. No Shared Connection Pool Across DAOs

**Problem:**
5 DAOs to the same PostgreSQL instance = 5 pools = 50 connections.

**Fix:**
Connection pool registry keyed by `(host, port, database)`:
```python
class ConnectionPoolRegistry:
    _pools: dict[tuple, asyncpg.Pool] = {}

    @classmethod
    async def get_pool(cls, config: StorageConfig) -> asyncpg.Pool:
        key = (config.host, config.port, config.database)
        if key not in cls._pools:
            cls._pools[key] = await asyncpg.create_pool(...)
        return cls._pools[key]
```

---

### 18. SecretPointerRecord Hardcodes Postgres Backend

**Problem:**
```python
class SecretPointerRecord(StorageModel):
    _model_meta = ModelMetadata(
        storage_configs={"postgres": StorageConfigFactory.from_yaml("postgres")}
    )
```

Fails if YAML has no "postgres" entry. Not configurable.

**Fix:**
Make configurable via environment or defer config resolution:
```python
_model_meta = ModelMetadata(path="secret_pointer_records")
# Config resolved at runtime, not import time
```

---

## LOW Issues

### 19. No Public API Surface

**Fix:** Populate `ami/__init__.py` with key exports:
```python
from ami.core.dao import BaseDAO, DAOFactory, get_dao
from ami.core.exceptions import StorageError, NotFoundError
from ami.core.storage_types import StorageType
from ami.models.base_model import StorageModel
from ami.models.storage_config import StorageConfig
```

---

### 20. TokenEncryption Returns Sentinel String

**Fix:**
Raise `DecryptionError(StorageError)` instead of returning `"[DECRYPTION_FAILED]"`. Callers handle the exception explicitly.

---

### 21. secured_mixin Complex Logic With Minimal Tests

**Fix:** Address in the test plan. Not an architecture issue per se.

---

### 22. REST DAO Guesses Envelope Patterns

**Fix:**
Make envelope extraction configurable via `config.options`:
```python
config.options = {
    "response_data_key": "results",  # explicit extraction path
    "response_count_key": "total",
}
```

---

---

## Related Documents

- **[ARCHITECTURE-REVISION-AUDIT-FINDINGS.md](./ARCHITECTURE-REVISION-AUDIT-FINDINGS.md)** -- Full codebase audit (2026-02-14). Issues 23-70 covering security vulnerabilities, placebo/vibecoded functionality, broken logic, silent error swallowing, resource leaks, and testing gaps.

---

## Revision Priority Order

**Phase 1 -- Unblock the system (CRITICAL):**
1. Wire DAO registry (Issue 1)
2. Fix OpenBao to use hvac or remove (Issue 2)
3. Standardize bulk_delete return type (Issue 4)
4. Fix threading lock in async code (Issue 5)
5. Move test code out of production (Issue 3)

**Phase 2 -- Fix design flaws (HIGH):**
6. Split BaseDAO into focused interfaces (Issue 9)
7. Fix Redis raw query scoping (Issue 8)
8. Replace DAO cache with explicit pool (Issue 10)
9. Make graph loading explicit (Issue 7)
10. Split StorageModel into composable layers (Issue 6)

**Phase 3 -- Reduce tech debt (MEDIUM):**
11-18. Remaining medium issues in parallel

**Phase 4 -- Polish (LOW):**
19-22. Low priority cleanup
