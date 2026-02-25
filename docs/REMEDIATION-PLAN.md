# AMI-DATAOPS Remediation Plan

**Date:** 2026-02-14 (v3 -- all 16 audit findings fixed)
**Status:** PENDING APPROVAL -- No code changes until every issue is reviewed
**Source:** ARCHITECTURE-REVISION.md (Issues 1-22), ARCHITECTURE-REVISION-AUDIT-FINDINGS.md (Issues 23-70)

## Ground Rules

1. No code is written until this plan is approved issue-by-issue
2. Each fix shows EXACT current code and EXACT replacement code
3. No "while I'm here" side-changes
4. No new dependencies unless explicitly justified
5. Deferred items are clearly marked with rationale
6. ALL proposed code follows AMI-CI rules:
   - ALL `__init__.py` files remain empty (`ci_check_init_files` enforced)
   - No `# noqa` comments (banned)
   - No `type: ignore` comments (banned)
   - No `: Any` type annotations (banned)
   - No `dict[*, Any]` for structured data (banned)
   - Max 512 lines per .py file
7. Pre-fix checks are completed and results included inline

---

## Issue Index

| # | Phase | Severity | Title | Files |
|---|-------|----------|-------|-------|
| 1 | 1 | CRITICAL | DAO registry never populated | `core/dao.py`, NEW `bootstrap.py` |
| 2 | 1 | CRITICAL | OpenBaoDAO imports nonexistent package | `vault/openbao_dao.py` |
| 3 | 1 | CRITICAL | Test code in production module | `embedding_service.py` |
| 4 | 1 | CRITICAL | bulk_delete() return type varies | `core/dao.py`, `dgraph_dao.py`, `pgvector_dao.py` |
| 5 | 1 | CRITICAL | Threading lock in async codebase | `secrets/client.py` |
| 23 | 2 | CRITICAL | Hardcoded master key and salt | `secrets/client.py` |
| 24 | 2 | CRITICAL | Field name as PBKDF2 salt | `security/encryption.py` |
| 25 | 2 | CRITICAL | SHA-256 for password hashing | `security/encryption.py` |
| 26 | 2 | CRITICAL | Silent random key generation | `security/encryption.py` |
| 27 | 2 | CRITICAL | DQL injection across graph layer | `graph/dgraph_read.py`, `dgraph_util.py` |
| 28 | 2 | CRITICAL | PromQL injection via unescaped labels | `timeseries/prometheus_models.py` |
| 29 | 2 | CRITICAL | Vault HTTPS/HTTP logic inverted | `vault/openbao_dao.py` |
| 36 | 2 | CRITICAL | Sensitive fields plaintext on ImportError | `models/base_model.py` |
| 45 | 2 | HIGH | Decryption returns fake data | `security/encryption.py` |
| 30 | 3 | CRITICAL | record_event records nothing | `services/decorators.py` |
| 31 | 3 | CRITICAL | cached_result TTL ignored | `services/decorators.py` |
| 32 | 3 | CRITICAL | k_hop_query ignores _k and _edge_types | `graph/dgraph_graph.py` |
| 33 | 3 | CRITICAL | traverse() ignores _filters | `graph/dgraph_traversal.py` |
| 34 | 3 | CRITICAL | raw_write_query ignores _params | `graph/dgraph_update.py` |
| 35 | 3 | CRITICAL | _log_access discards permission/result | `models/secured_mixin.py` |
| 8 | 4 | HIGH | Redis raw query unsanitized | `mem/redis_dao.py` |
| 37 | 4 | HIGH | SQL parameter order loss | `sql/postgresql_dao.py` |
| 38 | 4 | HIGH | Empty SET clause invalid SQL | `sql/postgresql_update.py` |
| 39 | 4 | HIGH | UID registry false negatives | `core/unified_crud.py` |
| 40 | 4 | HIGH | _row_to_model returns wrong type | `sql/`, `vec/` |
| 41 | 4 | HIGH | query.pop() mutates caller dict | `timeseries/prometheus_dao.py` |
| 42 | 4 | HIGH | REST DAO returns "None" as ID | `rest/rest_dao.py` |
| 43 | 4 | HIGH | to_dgraph_rule() returns empty string | `models/security.py` |
| 44 | 4 | HIGH | Fragile asyncpg result parsing | `sql/`, `vec/` |
| 46 | 4 | HIGH | Embedding failure returns None | `vec/pgvector_dao.py` |
| 47 | 4 | HIGH | REST count falls back to full scan | `rest/rest_dao.py` |
| 48 | 4 | HIGH | Vault listing returns empty on error | `vault/openbao_dao.py` |
| 11 | 5 | MEDIUM | Graph relations unescaped DQL | `core/graph_relations.py` |
| 13 | 5 | MEDIUM | YAML cache no invalidation | `models/storage_config_factory.py` |
| 15 | 5 | MEDIUM | Unescaped passwords in conn strings | `models/storage_config.py` |
| 16 | 5 | MEDIUM | Prometheus update/delete on append-only | `timeseries/prometheus_dao.py` |
| 18 | 5 | MEDIUM | SecretPointerRecord hardcodes postgres | `models/secret_pointer.py` |
| 49 | 5 | MEDIUM | Production assert statements | 12+ files (36 sites) |
| 50 | 5 | MEDIUM | Broad except Exception catches | All DAOs (54+ sites) |
| 51 | 5 | MEDIUM | Index creation failures swallowed | `sql/`, `vec/` utils |
| 52 | 5 | MEDIUM | Double transaction discard | `graph/dgraph_update.py` |
| 53 | 5 | MEDIUM | ContextVar never cleaned up | `secrets/adapter.py` |
| 54 | 5 | MEDIUM | No graph transaction timeouts | `graph/` DAOs |
| 55 | 5 | MEDIUM | No HTTP timeout default | `utils/http_client.py` |
| 56 | 5 | MEDIUM | Silent field skipping | `sql/`, `vec/` |
| 57 | 5 | MEDIUM | Hardcoded datetime field list | `graph/dgraph_update.py` |
| 58 | 5 | MEDIUM | Redis error masking | `mem/redis_update.py` |
| 60 | 5 | MEDIUM | Incomplete vault path validation | `vault/openbao_dao.py` |
| 19 | -- | LOW | No public API surface | DEFERRED |
| 21 | 6 | LOW | secured_mixin minimal tests | `tests/` |
| 22 | 6 | LOW | REST DAO guesses envelope patterns | `rest/rest_dao.py` |
| 62 | 6 | LOW | Unreachable type(None) check | `core/graph_relations.py` |
| 63 | 6 | LOW | Always-true if vector: check | `embedding_service.py` |
| 64 | 6 | LOW | Hardcoded skip fields | `graph/dgraph_util.py` |
| 65 | 6 | LOW | Dead config values | `config/storage-config.yaml` |
| 66 | 6 | LOW | Coverage threshold at 50% | `res/config/coverage_thresholds.yaml` |
| 67 | 6 | LOW | Zero mock usage | `tests/` |
| 68 | 6 | LOW | Empty test directories | `tests/` |
| 69 | 6 | LOW | to_dgraph_rule() never tested | `models/security.py` |
| 70 | 6 | LOW | Incomplete type mappings | `sql/`, `graph/` |

### Deferred Issues

| # | Title | Reason |
|---|-------|--------|
| 6 | StorageModel god class | Architecture redesign, every model affected |
| 7 | Async load_related unreachable | Needs new GraphLoader service |
| 9 | BaseDAO 23 abstract methods | Interface redesign, every DAO affected |
| 10 | DAO cache event loop identity | Needs DAOPool replacement |
| 12 | Bulk ops sequential | Performance, not correctness |
| 17 | No shared connection pool | New infrastructure |
| 19 | No public API surface | Blocked by CI empty `__init__.py` rule; non-standard workarounds add confusion |
| 59 | Bulk create no transaction | Overlaps with #12 |

### Duplicate Issues

| # | Duplicate Of | Reason |
|---|-------------|--------|
| 14 | #40 | Same _row_to_model hydration bug |
| 20 | #45 | Same sentinel string pattern |
| 61 | #15 | Same connection string escaping |

---

## Phase 1: CRITICAL -- Unblock the System

### Issue #1: Wire DAO Registry

**File:** `ami/core/dao.py` (no change), NEW file `ami/bootstrap.py`

**Problem:** `_dao_registry` is always empty. `get_dao_class()` always raises. `DAOFactory.create()` is dead code.

**Constraint:** ALL `__init__.py` files must remain empty (AMI-CI `ci_check_init_files`).

**Root cause:** `register_dao()` exists (dao.py:232-237) but is never called anywhere.

**Fix:** `get_dao_class()` stays exactly as-is -- pure registry lookup, raises if not found. Create `ami/bootstrap.py` that calls `register_dao()` for each backend at application startup:

**New file `ami/bootstrap.py`:**
```python
"""Register all DAO implementations. Call register_all_daos() at application startup."""

from ami.core.dao import register_dao
from ami.core.storage_types import StorageType
from ami.implementations.graph.dgraph_dao import DgraphDAO
from ami.implementations.mem.redis_dao import RedisDAO
from ami.implementations.sql.postgresql_dao import PostgreSQLDAO
from ami.implementations.vec.pgvector_dao import PgVectorDAO
from ami.implementations.timeseries.prometheus_dao import PrometheusDAO
from ami.implementations.rest.rest_dao import RestDAO
from ami.implementations.vault.openbao_dao import OpenBaoDAO


def register_all_daos() -> None:
    """Register all storage backend implementations with the DAO registry."""
    register_dao(StorageType.GRAPH, DgraphDAO)
    register_dao(StorageType.INMEM, RedisDAO)
    register_dao(StorageType.RELATIONAL, PostgreSQLDAO)
    register_dao(StorageType.VECTOR, PgVectorDAO)
    register_dao(StorageType.TIMESERIES, PrometheusDAO)
    register_dao(StorageType.REST, RestDAO)
    register_dao(StorageType.VAULT, OpenBaoDAO)
```

**Application entrypoint** must call `register_all_daos()` before using `DAOFactory`:
```python
from ami.bootstrap import register_all_daos
register_all_daos()
```

**Implementation note:** The application entrypoint must be identified at implementation time. If no single entrypoint exists (library usage), consumers must call `register_all_daos()` before using `DAOFactory.create()`.

**No `__init__.py` changes.** All 8 files stay empty. No lazy imports. No conditional imports. No importlib. All imports happen eagerly when `register_all_daos()` is called.

**Verification:** After `register_all_daos()`, `get_dao_class(StorageType.GRAPH)` returns `DgraphDAO`.

---

### Issue #2: OpenBaoDAO Imports Nonexistent Package

**File:** `ami/implementations/vault/openbao_dao.py:23-32`

**Current:**
```python
OpenBaoClient: type[Any] | None = None
OpenBaoError: type[Exception] = Exception
try:
    from openbao import Client as _Cli
    from openbao.exceptions import OpenBaoError as _Err
    OpenBaoClient = _Cli
    OpenBaoError = _Err
except Exception:
    pass
```

**Replace with:**
```python
from hvac import Client as OpenBaoClient
from hvac.exceptions import VaultError as OpenBaoError
```

Direct imports. No try/except. If `hvac` is not installed, importing this module fails with a clear `ImportError`. This is correct -- you cannot use a vault DAO without the vault client library.

**Also:** Add `hvac>=2.1.0` to `pyproject.toml` dependencies (or optional `vault` extra).

**Note:** `bootstrap.py` imports this module eagerly. If vault is not needed, the application registers a subset of DAOs instead of calling `register_all_daos()`.

`hvac` is API-compatible with both HashiCorp Vault and OpenBao. All APIs used by the DAO (`client.secrets.kv.v2.*`, `client.sys.read_health_status()`, `client.is_authenticated()`) are standard `hvac`.

**Verification:** `from ami.implementations.vault.openbao_dao import OpenBaoDAO` succeeds with hvac installed. Fails with clear `ImportError` without hvac.

---

### Issue #3: Test Code in Production Module

**File:** `ami/implementations/embedding_service.py:195-231`

**Pre-fix result:** 0 external imports of `TestEmbeddingService`. 0 callers of `build_test_embedding_service`.

**Action:**
1. Create `tests/helpers/__init__.py` (empty)
2. Create `tests/helpers/embedding.py` -- move `TestEmbeddingService` and `build_test_embedding_service`
3. Delete lines 195-231 from `embedding_service.py`

**Verification:** `grep -r TestEmbeddingService ami/` returns 0 results.

---

### Issue #4: bulk_delete() Return Type Varies

**File:** `ami/core/dao.py:104-106`

**Pre-fix results:**
- DgraphDAO returns `dict` -- must extract count and return `int`
- PgVectorDAO returns `dict | int` -- must standardize to `int`
- All other DAOs already return `int`

**Current:**
```python
async def bulk_delete(self, ids: list[str]) -> dict[str, Any] | int:
```

**Replace with:**
```python
async def bulk_delete(self, ids: list[str]) -> int:
    """Bulk delete multiple records. Returns count of deleted records."""
```

**DgraphDAO fix (dgraph_dao.py, bulk_delete method):**
```python
# Current: returns raw mutation response dict
result = await txn.mutate(del_obj=delete_obj, commit_now=True)
return result  # dict

# Replace with:
await txn.mutate(del_obj=delete_obj, commit_now=True)
# Verify at implementation: pydgraph mutation response structure.
# If actual delete count is available, extract it. Otherwise:
return len(ids)
```

**PgVectorDAO fix (pgvector_dao.py, bulk_delete method):**
```python
# Current: returns dict or int depending on code path
# Replace: use parse_affected_count (from Issue #44) on the DELETE result
result = await conn.execute(query, *ids)
return parse_affected_count(result)
```

**Verification:** `mypy` passes. All callers treat result as `int`.

---

### Issue #5: Threading Lock in Async Codebase

**File:** `ami/secrets/client.py`

**Pre-fix results:** 5 call sites total:
- `adapter.py:75` (`ensure_secret`), `adapter.py:119` (`retrieve_secret`)
- `client.py:296`, `client.py:305`, `client.py:308` (passthroughs)

**Fix (5 parts):**

**A. InMemorySecretsBackend (client.py:80-86):** `threading.Lock` -> `asyncio.Lock`, methods become `async def`:
```python
import asyncio

class InMemorySecretsBackend:
    def __init__(self, master_key: bytes | None = None) -> None:
        self._master_key = master_key or DEFAULT_MASTER_KEY
        self._records: dict[str, _SecretRecord] = {}
        self._lock = asyncio.Lock()

    async def ensure_secret(self, ...) -> VaultFieldPointer:
        async with self._lock:
            ...

    async def retrieve_secret(self, reference: str) -> tuple[str, str]:
        async with self._lock:
            ...

    async def delete_secret(self, reference: str) -> None:
        async with self._lock:
            ...
```

**B. SecretsBrokerBackend Protocol (client.py:50-66):** All methods become `async def`.

**C. HTTPSecretsBrokerBackend (client.py:165+):** Replace sync `urllib.request` with `aiohttp` (already a project dependency in `utils/http_client.py`):
```python
import aiohttp

class HTTPSecretsBrokerBackend:
    async def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        url = f"{self._base_url}{path}"
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, url, json=payload, headers=self._headers,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def ensure_secret(self, ...) -> VaultFieldPointer:
        data = await self._request("POST", "/v1/secrets/ensure", payload)
        return VaultFieldPointer.model_validate(data)
```

No `asyncio.to_thread`. No sync `urllib`. Direct async HTTP with `aiohttp`.

**D. SecretsBrokerClient (client.py:287-308):** Passthroughs become `async def` with `await`.

**E. Update callers:** `adapter.py:75` and `adapter.py:119` add `await`.

**Verification:** `grep -n "threading.Lock" ami/secrets/client.py` returns 0 results.

---

## Phase 2: CRITICAL -- Security

### Issue #23: Hardcoded Cryptographic Master Key and Salt

**File:** `ami/secrets/client.py:24-31`

**Current:**
```python
DEFAULT_MASTER_KEY = os.getenv(
    "DATAOPS_VAULT_MASTER_KEY",
    "dev-master-key",
).encode()
_PUBLIC_INTEGRITY_SALT = os.getenv(
    "DATAOPS_VAULT_INTEGRITY_SALT",
    "ami-integrity-salt",
).encode()
```

**Replace with:**
```python
from ami.core.exceptions import ConfigurationError

def _get_master_key() -> bytes:
    value = os.getenv("DATAOPS_MASTER_KEY")
    if not value:
        msg = "Required: DATAOPS_MASTER_KEY environment variable"
        raise ConfigurationError(msg)
    return value.encode()


def _get_integrity_salt() -> bytes:
    value = os.getenv("DATAOPS_INTEGRITY_SALT")
    if not value:
        msg = "Required: DATAOPS_INTEGRITY_SALT environment variable"
        raise ConfigurationError(msg)
    return value.encode()
```

**Env var names unified with Issue #26:** `DATAOPS_MASTER_KEY` (not `DATAOPS_VAULT_MASTER_KEY`). One master key env var across the entire system. `DATAOPS_INTEGRITY_SALT` (not `DATAOPS_VAULT_INTEGRITY_SALT`).

These are module-level functions, not module-level constants. The env var is read when the function is called (at runtime), not at import time. `import ami.secrets.client` succeeds without env vars. Any operation that needs the key fails with `ConfigurationError`.

**Also update references:**
- `client.py:46` (`compute_integrity_hash`): `_PUBLIC_INTEGRITY_SALT` -> `_get_integrity_salt()`
- `client.py:84` (`InMemorySecretsBackend.__init__`): `DEFAULT_MASTER_KEY` -> `_get_master_key()`

**Test impact:** Tests set env vars via `monkeypatch.setenv("DATAOPS_MASTER_KEY", "test-key")`.

**Verification:** `import ami.secrets.client` succeeds without env vars. `_get_master_key()` raises `ConfigurationError`.

---

### Issue #24: Field Name Used as PBKDF2 Salt

**File:** `ami/security/encryption.py`

**Problem (2 parts):**
1. Field name used as PBKDF2 salt -- same field always produces same derived key
2. Per-field key derivation from single master key is security theater -- if master key is compromised, all derived keys are trivially computable

**Fix:** Replace per-field key derivation with a single application key derived from the master key. Use Fernet directly -- it already generates a random IV per encryption call. No per-field keys. No custom wire format. No salt storage.

**Replace `KeyManager` class (encryption.py:20-60):**
```python
import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from ami.core.exceptions import ConfigurationError


class KeyManager:
    _fernet: Fernet | None = None

    @classmethod
    def initialize(cls, master_key: str | None = None) -> None:
        raw = master_key or os.getenv("DATAOPS_MASTER_KEY")
        if not raw:
            msg = "Master encryption key required. Set DATAOPS_MASTER_KEY or pass master_key."
            raise ConfigurationError(msg)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b"ami-dataops-field-encryption-v1",
            iterations=600_000,
        )
        derived = kdf.derive(raw.encode())
        cls._fernet = Fernet(base64.urlsafe_b64encode(derived))

    @classmethod
    def get_fernet(cls) -> Fernet:
        if cls._fernet is None:
            cls.initialize()
        if cls._fernet is None:
            msg = "Encryption not initialized"
            raise ConfigurationError(msg)
        return cls._fernet
```

**Env var unified with Issue #23 and #26:** `DATAOPS_MASTER_KEY` everywhere.

The fixed salt `b"ami-dataops-field-encryption-v1"` is not a secret; it binds the derived key to this application. Per-encryption randomness comes from Fernet's built-in IV generation.

**Replace `FieldEncryption.encrypt_field` (encryption.py:102-113):**
```python
@staticmethod
def encrypt_field(value, field_name, classification) -> str:
    if classification >= DataClassification.CONFIDENTIAL:
        f = KeyManager.get_fernet()
        return f.encrypt(str(value).encode()).decode()
    return str(value)
```

**Replace `FieldEncryption.decrypt_field` (encryption.py:115-129):**
```python
@staticmethod
def decrypt_field(encrypted, field_name, context):
    if not hasattr(context, "permissions") or Permission.DECRYPT not in context.permissions:
        return "[ENCRYPTED]"
    f = KeyManager.get_fernet()
    return f.decrypt(encrypted.encode()).decode()
```

No custom wire format. Fernet tokens are self-contained (version + timestamp + IV + ciphertext + HMAC). No salt storage needed. Two encryptions of the same value produce different ciphertexts because Fernet generates a random IV each time.

**Delete:** `get_field_key()` method entirely. No per-field key derivation.

**Also update:** `TransparentEncryption.encrypt_model` and `decrypt_model` -- Fernet token replaces the old format. `[ENC:fernet_token]` marker if needed, or just the raw Fernet token.

**Data migration:** One-time script decrypts with old `get_field_key(field_name)` and re-encrypts with new Fernet. Separate maintenance script, not production code.

**Verification:** Two encryptions of same value produce different ciphertexts. `KeyManager.get_fernet()` without env var raises `ConfigurationError`.

---

### Issue #25: SHA-256 for Password Hashing

**File:** `ami/security/encryption.py:131-146`

**Current:**
```python
def hash_field(value: str, salt: str | None = None) -> str:
    if salt:
        value = f"{salt}{value}"
    hash_obj = hashlib.sha256(value.encode())
    return hash_obj.hexdigest()

def verify_hash(value: str, hashed: str, salt: str | None = None) -> bool:
    return FieldEncryption.hash_field(value, salt) == hashed
```

**Replace with:**
```python
import bcrypt

@staticmethod
def hash_field(value: str) -> str:
    """One-way hash using bcrypt."""
    return bcrypt.hashpw(value.encode(), bcrypt.gensalt()).decode()

@staticmethod
def verify_hash(value: str, hashed: str) -> bool:
    """Verify value against bcrypt hash. Returns False for non-bcrypt input."""
    try:
        return bcrypt.checkpw(value.encode(), hashed.encode())
    except ValueError:
        return False
```

**New dependency:** `bcrypt` in `pyproject.toml`.

**Signature change:** `salt` parameter removed. bcrypt manages its own salt internally.

No dual-format detection. No `startswith("$2")` check. No `needs_rehash()` helper. `bcrypt.checkpw` raises `ValueError` on invalid format; catching it and returning `False` is the clean behavior -- "this is not a valid hash" means verification fails.

Existing SHA-256 hashes are a data migration concern. A one-time migration script rehashes all stored values, run as an ops task before deployment. Not production code.

**Verification:** `hash_field("test")` starts with `$2b$`. `verify_hash("test", hash_field("test"))` returns `True`. `verify_hash("test", "abc123hex")` returns `False`.

---

### Issue #26: Silent Random Key Generation

**File:** `ami/security/encryption.py:29-37`

**Current:**
```python
if master_key:
    cls._master_key = master_key.encode()
else:
    cls._master_key = Fernet.generate_key()
    logger.warning("Using generated master key - not for production!")
```

**Replace with (already covered in Issue #24 rewrite):**
```python
raw = master_key or os.getenv("DATAOPS_MASTER_KEY")
if not raw:
    msg = "Master encryption key required. Set DATAOPS_MASTER_KEY or pass master_key."
    raise ConfigurationError(msg)
```

**Env var unified with Issue #23:** Both `secrets/client.py` and `security/encryption.py` use `DATAOPS_MASTER_KEY`. One env var, one master key, system-wide.

**Note:** `rotate_keys()` (line 63) still generates a key -- that's intentional for key rotation.

**Verification:** `KeyManager.initialize()` without env var or argument raises `ConfigurationError`.

---

### Issue #27: DQL Injection Across Graph Layer

**Known injection sites (must be verified and completed at implementation time):**
1. `dgraph_read.py:41` -- `f'eq({coll}.app_uid, "{item_id}")'`
2. `dgraph_read.py:~55` -- list query with f-string interpolation
3. `dgraph_util.py:~275-310` -- multiple DQL builder functions
4. `dgraph_traversal.py` -- traverse query builder
5-8. Remaining sites to be enumerated by reading all `graph/*.py` files at implementation time

**Fix -- Add validation helper (top of dgraph_util.py):**
```python
import re
from ami.core.exceptions import StorageValidationError

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")

def _validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        msg = f"Invalid DQL identifier: {name!r}"
        raise StorageValidationError(msg)
    return name
```

**Fix -- Parameterize user values (dgraph_read.py:39-42):**
```python
# Before:
query = f'    node(func: eq({coll}.app_uid, "{item_id}"))'
response = txn.query(query)

# After:
coll = _validate_identifier(dao.collection_name)
query = f'query find($id: string) {{ node(func: eq({coll}.app_uid, $id)) ... }}'
variables = {"$id": item_id}
response = txn.query(query, variables=variables)
```

**API verification required:** The `txn.query(query, variables=variables)` signature must be verified against the installed pydgraph version at implementation time. If pydgraph does not support `variables`, an alternative parameterization approach is needed.

Field/edge names validated with `_validate_identifier()`. User-supplied values use DQL `$variable` parameterization (pending API verification). Same pattern applied to all injection sites.

**Verification:** `item_id = '") { hack } #'` produces harmless parameterized query. `_validate_identifier("bad name!")` raises `StorageValidationError`.

---

### Issue #28: PromQL Injection via Unescaped Label Values

**File:** `ami/implementations/timeseries/prometheus_models.py:174`

**Current:**
```python
label_selectors.append(f'{key}="{value}"')
```

**Replace with:**
```python
import re
from ami.core.exceptions import StorageValidationError

_PROMQL_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

def _escape_promql_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

# In function body:
if not _PROMQL_LABEL_RE.match(key):
    msg = f"Invalid PromQL label name: {key!r}"
    raise StorageValidationError(msg)
label_selectors.append(f'{key}="{_escape_promql_value(value)}"')
```

**Verification:** Label name `"bad name!"` raises. Value `'x"}'` is properly escaped.

---

### Issue #29: Vault HTTPS/HTTP Logic Inverted

**File:** `ami/implementations/vault/openbao_dao.py:104-105`

**Current:**
```python
is_default = self.config.port == _VAULT_DEFAULT_PORT
protocol = "https" if is_default else "http"
```

**Replace with:**
```python
use_tls = True
if self.config.options:
    use_tls = self.config.options.get("tls", True)
protocol = "https" if use_tls else "http"
```

**Verification:** Default is HTTPS. `options = {"tls": False}` produces HTTP.

---

### Issue #36: Sensitive Fields Stored Plaintext on ImportError

**File:** `ami/models/base_model.py:186-189`

**Current:**
```python
try:
    from ami.secrets.adapter import prepare_instance_for_storage
    return prepare_instance_for_storage(self, data, context)
except ImportError:
    pass
return data
```

**Replace with:**
```python
try:
    from ami.secrets.adapter import prepare_instance_for_storage
except ImportError as e:
    msg = (
        f"Model {self.__class__.__name__} has sensitive fields "
        "but ami.secrets.adapter is not available"
    )
    raise ImportError(msg) from e
return prepare_instance_for_storage(self, data, context)
```

**Verification:** With adapter unavailable, raises `ImportError` instead of storing plaintext.

---

### Issue #45: Decryption Failure Returns Fake Data

**Files:** `ami/core/exceptions.py`, `ami/security/encryption.py:94-96`

**Add to exceptions.py:**
```python
class DecryptionError(StorageError):
    """Raised when field decryption fails."""
```

**Current (encryption.py:94-96):**
```python
except Exception:
    logger.exception("Decryption failed")
    return "[DECRYPTION_FAILED]"
```

**Replace with:**
```python
except Exception as e:
    msg = "Decryption failed"
    raise DecryptionError(msg) from e
```

**Pre-fix result:** 0 callers check for `"[DECRYPTION_FAILED]"` sentinel. No breakage.

**Verification:** Wrong key raises `DecryptionError`.

---

## Phase 3: CRITICAL -- Placebo Removal

### Issue #30: record_event Decorator Records Nothing

**File:** `ami/services/decorators.py:119-150`

**Pre-fix result:** 0 usages of `@record_event` in the entire codebase. `EventRecord` model (lines 26-63) also has 0 usages.

**Action:** Delete `record_event` function (lines 119-150). Delete `EventRecord` class (lines 26-63). Remove newly-unused imports.

**Verification:** `grep -rn "record_event\|EventRecord" ami/` returns 0 results.

---

### Issue #31: cached_result TTL Ignored for Non-Memory Backends

**File:** `ami/services/decorators.py:153-192`

**Pre-fix result:** 0 usages of `@cached_result` in the entire codebase.

**Action:** Delete `cached_result` function (lines 153-192) entirely.

**Verification:** `grep -rn "cached_result" ami/` returns 0 results.

---

### Issue #32: k_hop_query Ignores _k and _edge_types

**File:** `ami/implementations/graph/dgraph_graph.py:80-95`

**Pre-fix result:** 1 caller: `dgraph_dao.py:213`.

The function ignores `_k` and `_edge_types` and always returns one-hop neighbors.

**Replace with:**
```python
async def one_hop_neighbors(
    dao, start_id: str,
) -> dict:
```

Remove unused `_k` and `_edge_types` parameters. Rename to match actual behavior. Return type stays `dict` -- the exact structure must be verified from the function body at implementation time; do not fabricate a type annotation.

**Also update caller:** `dgraph_dao.py:213` changes `k_hop_query(self, start_id, k, edge_types)` to `one_hop_neighbors(self, start_id)`.

**Verification:** No unused parameters. Name matches behavior.

---

### Issue #33: traverse() Ignores _filters

**File:** `ami/implementations/graph/dgraph_traversal.py:~405`

**Pre-fix result:** 0 external callers.

**Action:** Remove `_filters` parameter from signature.

**Verification:** No unused parameters.

---

### Issue #34: raw_write_query Ignores _params

**File:** `ami/implementations/graph/dgraph_update.py:~384`

**Pre-fix result:** BaseDAO defines `params` in the abstract signature. Other DAOs use it. DQL raw mutations do not support parameterized variables.

**Replace:**
```python
async def raw_write_query(dao, query, params=None):
    if params is not None:
        msg = "DQL raw mutations do not support parameterized variables"
        raise NotImplementedError(msg)
    ...
```

**Verification:** `raw_write_query(dao, q, params={"k": "v"})` raises `NotImplementedError`.

---

### Issue #35: _log_access Discards Permission and Result

**File:** `ami/models/secured_mixin.py:124-135`

**Pre-fix result:** `accessed_by`/`accessed_at` read in 7 places: all in `secured_mixin.py` + 1 in `secrets/repository.py`.

**Current:**
```python
def _log_access(self, context, _permission, _result):
    if context.user_id not in self.accessed_by:
        self.accessed_by.append(context.user_id)
    self.accessed_at.append(datetime.now(UTC))
```

**Fix (3 parts):**

**A. Add TypedDict (top of secured_mixin.py):**
```python
from typing import TypedDict

class AccessLogEntry(TypedDict):
    user_id: str
    permission: str
    result: str
    timestamp: str

_MAX_ACCESS_ENTRIES = 1000
```

**B. Replace `_log_access`:**
```python
def _log_access(self, context, permission, result):
    entry: AccessLogEntry = {
        "user_id": context.user_id,
        "permission": permission.value,
        "result": result,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    self._access_log.append(entry)
    if len(self._access_log) > _MAX_ACCESS_ENTRIES:
        self._access_log = self._access_log[-_MAX_ACCESS_ENTRIES:]
```

**C. Update the 7 readers of `accessed_by`/`accessed_at`:**
Each reader must be updated to read from `_access_log` instead:
- **Unique users:** `{e["user_id"] for e in self._access_log}`
- **Last access time:** `self._access_log[-1]["timestamp"] if self._access_log else None`
- **Full history:** `self._access_log` directly

The exact mapping for each of the 7 sites must be determined at implementation time by reading each call site. `accessed_by` and `accessed_at` fields are removed entirely. No redundant dual storage.

**Verification:** After `check_permission(ctx, Permission.READ)`, `self._access_log[-1]` contains all four fields: `user_id`, `permission`, `result`, `timestamp`.

---

## Phase 4: HIGH -- Logic Fixes

### Issue #8: Redis Raw Query Unsanitized

**File:** `ami/implementations/mem/redis_dao.py:269-272`

**Replace KEYS block with SCAN + prefix scoping:**
```python
if command == "KEYS":
    pattern = parts[1] if len(parts) > 1 else "*"
    scoped = f"{self._key_prefix}{pattern}"
    cursor = 0
    results = []
    while True:
        cursor, batch = await self.client.scan(
            cursor, match=scoped, count=100,
        )
        results.extend(batch)
        if cursor == 0:
            break
    return [{"key": k} for k in results]
```

**Verification:** `KEYS *` returns only keys with this DAO's prefix. No direct `KEYS` call to Redis.

---

### Issue #37: SQL Parameter Order Loss

**File:** `ami/implementations/sql/postgresql_dao.py:209-211`

**Current:**
```python
param_values = list(params.values())
rows = await conn.fetch(query, *param_values)
```

**Replace with:**
```python
if isinstance(params, dict):
    msg = "raw_read_query requires positional parameters as list or tuple, not dict"
    raise StorageValidationError(msg)
param_values = list(params) if params else []
rows = await conn.fetch(query, *param_values)
```

Dict ordering is not guaranteed to match `$1, $2, ...` placeholder order in the query. Reject dicts entirely. Callers must pass a list or tuple in the correct positional order.

**Verification:** `raw_read_query(q, {"a": 1})` raises `StorageValidationError`. `raw_read_query(q, [1, 2])` works.

---

### Issue #38: Empty SET Clause Invalid SQL

**File:** `ami/implementations/sql/postgresql_update.py:~37`

**Insert before SQL construction:**
```python
if not set_clauses:
    msg = f"No valid fields to update: {list(data.keys())}"
    raise StorageValidationError(msg)
```

---

### Issue #39: UID Registry False Negatives

**File:** `ami/core/unified_crud.py:229-234`

**Current:**
```python
async def read_by_uid(self, uid):
    if uid in self._uid_registry:
        model_class, config_index = self._uid_registry[uid]
        return await self.read(model_class, uid, config_index)
    return None
```

**Replace with:**
```python
async def read_by_uid(self, uid):
    if uid in self._uid_registry:
        model_class, config_index = self._uid_registry[uid]
        return await self.read(model_class, uid, config_index)
    # Registry miss: linear scan over all registered models.
    # O(n) where n = number of registered model classes. Acceptable for
    # correctness; optimize with a UID bloom filter if profiling shows
    # this is a hot path.
    for model_class in self._registered_models:
        result = await self.read(model_class, uid)
        if result is not None:
            return result
    return None
```

---

### Issue #40: _row_to_model Returns Wrong Type

**Files:** `sql/postgresql_dao.py:~415`, `vec/pgvector_read.py:~322`

**Current:**
```python
except Exception:
    logger.debug("Could not hydrate...")
    return row
```

**Replace with (both files):**
```python
except Exception as e:
    msg = f"Failed to hydrate {self.model_cls.__name__} from row"
    raise QueryError(msg) from e
```

**Import:** `from ami.core.exceptions import QueryError`

Row hydration is a query-result processing failure. `QueryError` is the correct specific exception.

---

### Issue #41: query.pop() Mutates Caller Dict

**File:** `ami/implementations/timeseries/prometheus_dao.py:221`

**Insert before the pop:** `query = dict(query)`

---

### Issue #42: REST DAO Returns "None" as ID

**File:** `ami/implementations/rest/rest_dao.py:~178`

**Current:** `return str(extracted)`

**Replace with:**
```python
if extracted is None:
    msg = "REST API returned no ID for created record"
    raise QueryError(msg)
return str(extracted)
```

**Import:** `from ami.core.exceptions import QueryError`

A missing ID from a create operation is a query-level failure, not a storage infrastructure error.

---

### Issue #43: to_dgraph_rule() Returns Empty String

**File:** `ami/models/security.py:~152`

**Current:** `return ""`

**Replace with:**
```python
msg = f"Unsupported auth rule type: {self.rule_type!r}"
raise ValueError(msg)
```

---

### Issue #44: Fragile asyncpg Result Parsing

**Files:** `sql/postgresql_delete.py:39`, `sql/postgresql_update.py:60`, `vec/pgvector_delete.py:25`

**Add shared helper to `ami/implementations/sql/postgresql_util.py`:**
```python
def parse_affected_count(result: str | None) -> int:
    """Parse asyncpg status like 'DELETE 3' or 'UPDATE 1'."""
    if not result:
        return 0
    parts = result.split()
    if len(parts) >= 2:
        try:
            return int(parts[-1])
        except ValueError:
            return 0
    return 0
```

Replace string parsing in each file with `parse_affected_count(result)`.

**Verification:** `parse_affected_count("DELETE 21")` returns `21` (old `endswith("1")` bug fixed).

---

### Issues #46-48: Silent Error Swallowing

**#46 (vec/pgvector_dao.py:~339):**
```python
# Current: except Exception: return None
# Replace:
except Exception as e:
    msg = f"Embedding generation failed for {field_name}"
    raise QueryError(msg) from e
```
Embedding failure during a query operation is a `QueryError`.

**#47 (rest/rest_dao.py:~286):**
```python
# Current: except StorageError: pass
# Replace:
except StorageError:
    raise
```
Re-raise. Do not swallow and fall through to a full scan.

**#48 (vault/openbao_dao.py:~225):**
```python
# Current: except OpenBaoError: return []
# Replace:
except OpenBaoError as e:
    msg = f"Vault listing failed for path {self._mount_path}"
    raise StorageConnectionError(msg) from e
```
Vault communication failure is a `StorageConnectionError`.

**Imports:** `from ami.core.exceptions import QueryError, StorageConnectionError` where needed.

---

## Phase 5: MEDIUM -- Error Handling & Resources

### Issue #11: Graph Relations Unescaped DQL

**File:** `ami/core/graph_relations.py:334-360`

**Add helper and apply to all `build()` interpolations:**
```python
def _escape_dql_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')

# Apply:
func_parts.append(f'eq({f["field"]}, "{_escape_dql_value(f["value"])}")')
```

---

### Issue #13: YAML Cache No Invalidation

**File:** `ami/models/storage_config_factory.py`

**Add:** `def invalidate_yaml_cache() -> None: _yaml_cache["data"] = None`

---

### Issue #15: Unescaped Passwords in Connection Strings

**File:** `ami/models/storage_config.py:101-102`

**Replace:** Use `urllib.parse.quote_plus(user)` and `quote_plus(password)` in f-string.

---

### Issue #16: Prometheus Update/Delete on Append-Only DB

**File:** `ami/implementations/timeseries/prometheus_dao.py`

**Replace update and delete:** `raise NotImplementedError("Prometheus is append-only")`

---

### Issue #18: SecretPointerRecord Hardcodes Postgres

**File:** `ami/models/secret_pointer.py:39`

**Replace:** `_model_meta = ModelMetadata(path="secret_pointer_records")` -- config resolved at runtime.

---

### Issue #49: Production Assert Statements (36 sites)

**Pattern -- replace each assert with the appropriate specific exception:**

**Pool/connection checks (most sites):**
```python
# Before:
assert self.pool is not None

# After:
if self.pool is None:
    msg = "Database pool not initialized -- call connect() first"
    raise StorageConnectionError(msg)
```

**Config/setup checks:**
```python
# Before:
assert self.collection_name is not None

# After:
if self.collection_name is None:
    msg = "collection_name not set on DAO"
    raise ConfigurationError(msg)
```

**Sites and their exception classes:**
- `postgresql_dao.py` (8): `StorageConnectionError` (pool checks)
- `postgresql_delete.py` (1): `StorageConnectionError` (pool check)
- `pgvector_read.py` (13): `StorageConnectionError` (pool checks)
- `pgvector_delete.py` (2): `StorageConnectionError` (pool checks)
- `pgvector_dao.py` (3): `StorageConnectionError` (pool checks)
- `pgvector_vector.py` (2): `StorageConnectionError` (pool checks)
- `pgvector_create.py` (2): `StorageConnectionError` (pool checks)
- `pgvector_update.py` (2): `StorageConnectionError` (pool checks)
- `prometheus_dao.py` (2): `StorageConnectionError` (client checks)
- `rest_dao.py` (1): `ConfigurationError` (session check)

**Import:** `from ami.core.exceptions import StorageConnectionError, ConfigurationError` where needed.

---

### Issue #50: Broad except Exception (54+ sites)

**Replace each with backend-specific exception. The exact exception classes must be verified against the installed library versions at implementation time:**

| Backend | Catch | Needs verification |
|---------|-------|--------------------|
| PostgreSQL (asyncpg) | `asyncpg.PostgresError`, `asyncpg.InterfaceError` | Verify these exist in asyncpg |
| Redis (redis-py) | `redis.RedisError` | Verify this exists in redis-py |
| Dgraph (pydgraph) | Verify available exceptions | `pydgraph.AbortedError` and `grpc.RpcError` need verification |
| REST (aiohttp) | `aiohttp.ClientError`, `asyncio.TimeoutError` | Verify these exist in aiohttp |
| Vault (hvac) | `hvac.exceptions.VaultError` | Verify this exists in hvac |

**Implementation step:** For each backend, run `python -c "import X; print([a for a in dir(X) if 'Error' in a])"` to enumerate available exception classes before writing the patches.

---

### Issue #51: Index Creation Failures Swallowed

**Files:** `sql/postgresql_util.py`, `vec/pgvector_util.py`

**Replace:** `except Exception: logger.warning(...)` -> `except asyncpg.PostgresError as e: raise StorageError(msg) from e`

---

### Issue #52: Double Transaction Discard

**File:** `ami/implementations/graph/dgraph_update.py`

**Remove `txn.discard()` from except blocks. Keep only in finally.**

---

### Issue #53: ContextVar Never Cleaned Up

**File:** `ami/secrets/adapter.py:23-25`

**Add context manager:**
```python
import contextlib

@contextlib.contextmanager
def pointer_context():
    try:
        yield
    finally:
        _POINTER_CONTEXT.set(None)
```

---

### Issue #54: No Graph Transaction Timeouts

**Files:** All `graph/dgraph_*.py` with `txn.query()`

**Wrap:** `response = await asyncio.wait_for(asyncio.to_thread(txn.query, query, variables=variables), timeout=30.0)`

---

### Issue #55: No HTTP Timeout Default

**File:** `ami/utils/http_client.py:83`

**Add:** `kwargs.setdefault("timeout", aiohttp.ClientTimeout(total=30))` before `session.request()`.

---

### Issue #56: Silent Field Skipping

**Files:** `sql/postgresql_update.py`, `sql/postgresql_create.py`, `vec/pgvector_update.py`

**Add after filtering:**
```python
skipped = [k for k in data if not is_valid_identifier(k) and k != "id"]
if skipped:
    logger.warning("Skipped invalid field names: %s", skipped)
```

---

### Issue #57: Hardcoded Datetime Field List

**File:** `ami/implementations/graph/dgraph_update.py:219-224`

**Current:** `if key in ["created_at", "updated_at", "verified_at", "last_login"]:`

**Replace with:** `if isinstance(value, datetime):`

---

### Issue #58: Redis Error Masking

**File:** `ami/implementations/mem/redis_update.py:97-101`

**Current:** `except Exception: existing_data = None`

**Replace with:**
```python
except KeyError:
    existing_data = None
except redis.RedisError:
    raise
```

---

### Issue #60: Incomplete Vault Path Validation

**File:** `ami/implementations/vault/openbao_dao.py:66-78`

**Add:**
```python
import re

_SAFE_PATH_RE = re.compile(r"^[a-zA-Z0-9_/.-]+$")

def _validate_vault_path(path: str) -> str:
    if not _SAFE_PATH_RE.match(path):
        msg = f"Invalid vault path: {path!r}"
        raise StorageValidationError(msg)
    if ".." in path:
        msg = f"Path traversal not allowed: {path!r}"
        raise StorageValidationError(msg)
    return path.strip("/")
```

---

## Phase 6: LOW -- Quality & Testing

### Issue #19: No Public API Surface

**DEFERRED.** All `__init__.py` must be empty per AMI-CI. Non-standard workarounds (like `ami/public.py`) add confusion without solving the underlying constraint. Revisit when CI rules are updated to allow controlled `__init__.py` exports.

---

### Issue #21: secured_mixin Minimal Tests

**Action:** Create `tests/unit/test_secured_mixin.py` with:
- `test_owner_always_granted`
- `test_deny_overrides_allow`
- `test_expired_rule_ignored`
- `test_log_access_records_permission_and_result`
- `test_max_access_entries_trimmed`

---

### Issue #22: REST DAO Guesses Envelope Patterns

**File:** `ami/implementations/rest/rest_dao.py`

**Make configurable:**
```python
self._data_key = (self.config.options or {}).get("response_data_key", "results")
self._count_key = (self.config.options or {}).get("response_count_key", "total")
```

---

### Issue #62: Unreachable type(None) Check

**File:** `ami/core/graph_relations.py:107`

**Action:** Delete the `if target_type is type(None):` branch.

---

### Issue #63: Always-True if vector: Check

**File:** `ami/implementations/embedding_service.py:216`

**Action:** Delete the `if vector:` guard. Just `return vector`.

---

### Issue #64: Hardcoded Skip Fields

**File:** `ami/implementations/graph/dgraph_util.py:379`

**Replace:** Accept `skip_fields` parameter with default `{"id", "uid", "storage_configs", "path"}`.

---

### Issue #65: Dead Config Values

**File:** `config/storage-config.yaml`

**Remove:** `is_ground_truth: true` (Dgraph, never read), `role_id`, `secret_id`, `jwt`, `kubernetes_role` (OpenBao, never read).

---

### Issue #66: Coverage Threshold

**File:** `res/config/coverage_thresholds.yaml`

**Change:** `integration: 50` -> `integration: 80`

---

### Issue #67: Zero Mock Usage

**Action:** Add mock-based boundary tests for each backend DAO using `unittest.mock.AsyncMock`.

---

### Issue #68: Empty Test Directories

**Action:** Populate `tests/integration/` and `tests/unit/adapters/` with test stubs or remove empty dirs.

---

### Issue #69: to_dgraph_rule() Never Tested

**Action:** Create `tests/unit/test_security.py` with `test_jwt_rule`, `test_graph_traversal_rule`, `test_unknown_type_raises`.

---

### Issue #70: Incomplete Type Mappings

**Files:** `sql/postgresql_create.py:98-124`, `graph/dgraph_util.py:18-26`

**Add:** `UUID` -> `UUID`/`string`, `Decimal` -> `NUMERIC`/`float`, `bytes` -> `BYTEA`/`string`, `date` -> `DATE`/`datetime`.

---

## Execution Protocol

1. Read exact source file(s) at current HEAD
2. Apply the exact patch shown above
3. Run `ruff check` and `mypy` on modified files
4. Run relevant tests
5. Mark issue complete

Phases execute in order. No Phase N+1 issue starts until Phase N is verified.

No `__init__.py` files modified. No banned patterns introduced. No scope creep.
