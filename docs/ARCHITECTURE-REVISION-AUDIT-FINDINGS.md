# AMI-DATAOPS Architecture Revision -- Audit Findings

**Date:** 2026-02-14
**Status:** Proposed
**Scope:** Full codebase audit -- security, correctness, placebo code, error handling
**Relation:** Supplements `ARCHITECTURE-REVISION.md` (issues 1-22). Issues here are numbered 23+.

**Severity Legend:**
- CRITICAL = security vulnerability or data corruption
- HIGH = broken logic, placebo functionality, silent data loss
- MEDIUM = bad practice, error swallowing, resource leaks
- LOW = code quality, dead code, missing validation

---

## Issue Index

| #  | Severity | Category | Title | Files Affected |
|----|----------|----------|-------|----------------|
| 23 | CRITICAL | Security | Hardcoded cryptographic master key and salt | `secrets/client.py` |
| 24 | CRITICAL | Security | Field name used as PBKDF2 salt (deterministic) | `security/encryption.py` |
| 25 | CRITICAL | Security | SHA-256 for password hashing with zero iterations | `security/encryption.py` |
| 26 | CRITICAL | Security | Silent random key generation on master key failure | `security/encryption.py` |
| 27 | CRITICAL | Security | DQL injection across entire graph layer | `graph/dgraph_read.py`, `dgraph_traversal.py`, `dgraph_util.py` |
| 28 | CRITICAL | Security | PromQL injection via unescaped label values | `timeseries/prometheus_models.py` |
| 29 | CRITICAL | Security | Vault HTTPS/HTTP logic inverted | `vault/openbao_dao.py` |
| 30 | CRITICAL | Placebo | `record_event` decorator records nothing | `services/decorators.py` |
| 31 | CRITICAL | Placebo | `cached_result` TTL ignored for non-memory backends | `services/decorators.py` |
| 32 | CRITICAL | Placebo | `k_hop_query` ignores `_k` and `_edge_types` params | `graph/dgraph_graph.py` |
| 33 | CRITICAL | Placebo | `traverse()` ignores `_filters` parameter | `graph/dgraph_traversal.py` |
| 34 | CRITICAL | Placebo | `raw_write_query` ignores `_params` parameter | `graph/dgraph_update.py` |
| 35 | CRITICAL | Placebo | `_log_access` discards permission and result params | `models/secured_mixin.py` |
| 36 | CRITICAL | Security | Sensitive fields stored plaintext on ImportError | `models/base_model.py` |
| 37 | HIGH | Logic | SQL parameter order loss in raw queries | `sql/postgresql_dao.py` |
| 38 | HIGH | Logic | Empty SET clause generates invalid SQL | `sql/postgresql_update.py` |
| 39 | HIGH | Logic | UID registry creates false negatives on read | `core/unified_crud.py` |
| 40 | HIGH | Logic | `_row_to_model` silently returns wrong type | `sql/postgresql_dao.py`, `vec/pgvector_read.py` |
| 41 | HIGH | Logic | `query.pop()` mutates caller's dict | `timeseries/prometheus_dao.py` |
| 42 | HIGH | Logic | REST DAO returns string `"None"` as record ID | `rest/rest_dao.py` |
| 43 | HIGH | Logic | `to_dgraph_rule()` returns empty string for unknown types | `models/security.py` |
| 44 | HIGH | Logic | Fragile string parsing of asyncpg result status | `sql/postgresql_delete.py`, `sql/postgresql_update.py`, `vec/pgvector_delete.py` |
| 45 | HIGH | Security | Decryption failure returns fake data silently | `security/encryption.py` |
| 46 | HIGH | Error | Embedding failure silently returns None | `vec/pgvector_dao.py` |
| 47 | HIGH | Error | REST count silently falls back to full table scan | `rest/rest_dao.py` |
| 48 | HIGH | Error | Vault listing returns empty on error | `vault/openbao_dao.py` |
| 49 | MEDIUM | Error | Production `assert` statements (disabled with `-O`) | 12+ files across all backends |
| 50 | MEDIUM | Error | Overly broad `except Exception` catches (20+ sites) | All backend DAOs |
| 51 | MEDIUM | Error | Index creation failures swallowed everywhere | `sql/postgresql_util.py`, `vec/pgvector_util.py` |
| 52 | MEDIUM | Resource | Double transaction discard in graph layer | `graph/dgraph_update.py` |
| 53 | MEDIUM | Resource | ContextVar never cleaned up (secret pointer leak) | `secrets/adapter.py` |
| 54 | MEDIUM | Resource | No transaction timeouts in graph layer | `graph/dgraph_graph.py`, `dgraph_read.py`, etc. |
| 55 | MEDIUM | Resource | No HTTP request timeout default | `utils/http_client.py` |
| 56 | MEDIUM | Logic | Silent field skipping on invalid identifiers | `sql/postgresql_update.py`, `sql/postgresql_create.py`, `vec/pgvector_update.py` |
| 57 | MEDIUM | Logic | Hardcoded datetime field list in graph updates | `graph/dgraph_update.py` |
| 58 | MEDIUM | Logic | Redis cache miss indistinguishable from real errors | `mem/redis_update.py` |
| 59 | MEDIUM | Logic | Bulk create is sequential, no transaction wrapping | `vec/pgvector_create.py`, all DAOs |
| 60 | MEDIUM | Security | Incomplete vault path traversal validation | `vault/openbao_dao.py` |
| 61 | MEDIUM | Security | Credentials embedded in connection strings | `models/storage_config.py` |
| 62 | LOW | Dead Code | Unreachable `type(None)` check | `core/graph_relations.py` |
| 63 | LOW | Dead Code | Always-true `if vector:` check | `implementations/embedding_service.py` |
| 64 | LOW | Dead Code | Hardcoded skip fields not configurable | `graph/dgraph_util.py` |
| 65 | LOW | Dead Code | Dead config (`is_ground_truth`, unused auth methods) | `config/storage-config.yaml` |
| 66 | LOW | Testing | Integration coverage threshold at 50% | `res/config/coverage_thresholds.yaml` |
| 67 | LOW | Testing | Zero mock usage in test suite | `tests/` |
| 68 | LOW | Testing | Empty test directories | `tests/integration/`, `tests/unit/adapters/` |
| 69 | LOW | Testing | `to_dgraph_rule()` never tested | `models/security.py`, `tests/` |
| 70 | LOW | Quality | Incomplete type mappings (missing UUID, Decimal, bytes) | `sql/postgresql_create.py`, `graph/dgraph_util.py` |

---

## CRITICAL Issues

### 23. Hardcoded Cryptographic Master Key and Salt

**Files:** `ami/secrets/client.py:24-31`

**Problem:**
```python
DEFAULT_MASTER_KEY = os.getenv(
    "DATAOPS_VAULT_MASTER_KEY",
    "dev-master-key",           # <-- hardcoded fallback
).encode()

_PUBLIC_INTEGRITY_SALT = os.getenv(
    "DATAOPS_VAULT_INTEGRITY_SALT",
    "ami-integrity-salt",       # <-- hardcoded fallback
).encode()
```

If env vars are unset, every deployment uses the same predictable master key and salt. All HMAC integrity checks become worthless. An attacker who reads the source code can forge integrity hashes for any secret.

**Fix:**
Fail fast if env vars are missing. Never provide fallback cryptographic material:

```python
def _require_env(name: str) -> bytes:
    value = os.getenv(name)
    if not value:
        msg = f"Required environment variable {name} is not set"
        raise EnvironmentError(msg)
    return value.encode()

DEFAULT_MASTER_KEY = _require_env("DATAOPS_VAULT_MASTER_KEY")
_PUBLIC_INTEGRITY_SALT = _require_env("DATAOPS_VAULT_INTEGRITY_SALT")
```

For development, use `.env` files or test fixtures -- not hardcoded defaults in production code.

---

### 24. Field Name Used as PBKDF2 Salt (Deterministic)

**File:** `ami/security/encryption.py:49`

**Problem:**
```python
salt=field_name.encode()
```

PBKDF2-HMAC uses the field name as salt. The same field always produces the same derived key. An attacker knowing the field name (e.g., `"ssn"`, `"credit_card"`) can precompute the key.

**Fix:**
Generate a random salt per encrypted value and store it alongside the ciphertext:

```python
import os

salt = os.urandom(16)
key = hashlib.pbkdf2_hmac("sha256", master_key, salt, iterations=600_000)
# Store as: base64(salt + ciphertext + tag)
```

---

### 25. SHA-256 for Password Hashing With Zero Iterations

**File:** `ami/security/encryption.py:136`

**Problem:**
```python
hash_obj = hashlib.sha256(value.encode())
return hash_obj.hexdigest()
```

Raw SHA-256 with zero iterations. A GPU can compute billions of SHA-256 hashes per second. This is trivially brute-forceable.

**Fix:**
Use a proper password hashing algorithm:

```python
import bcrypt

def hash_password(value: str) -> str:
    return bcrypt.hashpw(value.encode(), bcrypt.gensalt()).decode()

def verify_password(value: str, hashed: str) -> bool:
    return bcrypt.checkpw(value.encode(), hashed.encode())
```

---

### 26. Silent Random Key Generation on Master Key Failure

**File:** `ami/security/encryption.py:34`

**Problem:**
```python
cls._master_key = Fernet.generate_key()
logger.warning("Using generated master key - not for production!")
```

If master key initialization fails, a random key is generated with only a warning log. Data encrypted with this ephemeral key becomes **permanently unrecoverable** after process restart. No exception raised.

**Fix:**
Raise an exception. Never silently generate cryptographic keys:

```python
if not master_key:
    msg = "Master encryption key must be provided. Set DATAOPS_MASTER_KEY."
    raise EnvironmentError(msg)
cls._master_key = master_key.encode()
```

---

### 27. DQL Injection Across Entire Graph Layer

**Files:**
- `ami/implementations/graph/dgraph_read.py:27-47, 193-200`
- `ami/implementations/graph/dgraph_traversal.py:207-214, 338-363`
- `ami/implementations/graph/dgraph_util.py:275-307`

**Problem:**
User-supplied values are interpolated directly into DQL query strings with zero sanitization:

```python
# dgraph_read.py:34 -- item_id directly in query
query = f'    node(func: eq({coll}.app_uid, "{item_id}"))'

# dgraph_util.py:275 -- filter values directly in query
filters.append(f'eq({collection_name}.{key}, "{value}")')

# dgraph_util.py:307 -- regex from user input
filters.append(f'regexp({collection_name}.{key}, "/{op_value}/")')

# dgraph_traversal.py:343 -- edge path not validated
query_parts.append(f"{indent}{edge} {{")
```

An attacker can inject arbitrary DQL via crafted `item_id`, filter values, regex patterns, or edge paths.

**Fix:**
Use Dgraph's `$variable` parameterization for all user-supplied values:

```python
# Before (injectable)
query = f'{{ node(func: eq({coll}.uid, "{item_id}")) {{ uid }} }}'
response = txn.query(query)

# After (parameterized)
query = f'query find($id: string) {{ node(func: eq({coll}.uid, $id)) {{ uid }} }}'
variables = {"$id": item_id}
response = txn.query(query, variables=variables)
```

For edge paths and field names, validate against a strict allowlist:

```python
_EDGE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

def validate_edge_name(edge: str) -> str:
    if not _EDGE_NAME_RE.match(edge):
        msg = f"Invalid edge name: {edge!r}"
        raise StorageValidationError(msg)
    return edge
```

---

### 28. PromQL Injection via Unescaped Label Values

**File:** `ami/implementations/timeseries/prometheus_models.py:174`

**Problem:**
```python
label_selectors.append(f'{key}="{value}"')
```

If `value` contains `"` or `}`, the PromQL query breaks or is injectable:
```python
dict_query_to_promql("metric", {"label": 'x"} or vector(1) #'})
# Produces: metric{label="x"} or vector(1) #"}  -- INJECTION
```

**Fix:**
Escape PromQL special characters in label values:

```python
def _escape_promql_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

label_selectors.append(f'{key}="{_escape_promql_value(value)}"')
```

---

### 29. Vault HTTPS/HTTP Logic Inverted

**File:** `ami/implementations/vault/openbao_dao.py:105`

**Problem:**
```python
is_default = self.config.port == _VAULT_DEFAULT_PORT
protocol = "https" if is_default else "http"
```

Uses HTTPS only when port is 8200 (default). Any custom port falls back to plaintext HTTP. This is backwards -- should default to HTTPS and only allow HTTP when explicitly configured.

**Fix:**
```python
use_tls = self.config.options.get("tls", True) if self.config.options else True
protocol = "https" if use_tls else "http"
```

---

### 30. `record_event` Decorator Records Nothing

**File:** `ami/services/decorators.py:125-150`

**Problem:**
```python
def record_event(event_type: type[StorageModel] | str, **options):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
            except Exception:
                logger.exception("Event %s: %s failed", event_type, func.__name__)
                raise
            else:
                logger.debug("Event %s: %s succeeded", event_type, func.__name__)
                return result
```

Despite the name "record_event", it only calls `logger.debug()`. The `options` parameter is accepted and completely ignored. An `EventRecord` model exists in the codebase but is never instantiated or persisted. This is pure placebo -- it gives the appearance of event recording without doing it.

**Fix (Option A -- implement it):**
```python
async def wrapper(*args, **kwargs):
    start = datetime.now(UTC)
    try:
        result = await func(*args, **kwargs)
    except Exception as e:
        await _persist_event(event_type, func.__name__, "FAILED", start, error=str(e))
        raise
    else:
        await _persist_event(event_type, func.__name__, "SUCCESS", start)
        return result
```

**Fix (Option B -- remove it):**
Delete the decorator entirely. Replace usages with explicit logging at call sites. Don't pretend to record events when you're not.

---

### 31. `cached_result` TTL Ignored for Non-Memory Backends

**File:** `ami/services/decorators.py:167-192`

**Problem:**
```python
def cached_result(ttl: int = 300, cache_key=None, backend: str = "memory"):
```

When `backend="redis"`, the TTL parameter is accepted but **never enforced**. The decorator returns the uncached result directly. Only `backend="memory"` actually implements caching with TTL.

**Fix:**
Either implement Redis caching properly or remove the `backend` parameter and document that only in-memory caching is supported:

```python
# Option A: implement Redis backend
if backend == "redis":
    cached = await redis_client.get(key)
    if cached:
        return json.loads(cached)
    result = await func(*args, **kwargs)
    await redis_client.setex(key, ttl, json.dumps(result))
    return result

# Option B: remove the parameter
def cached_result(ttl: int = 300, cache_key=None):
    """In-memory cache with TTL. No external backend support."""
```

---

### 32. `k_hop_query` Ignores `_k` and `_edge_types` Parameters

**File:** `ami/implementations/graph/dgraph_graph.py:80-95`

**Problem:**
```python
async def k_hop_query(
    dao: Any,
    start_id: str,
    _k: int,                                # <-- IGNORED
    _edge_types: list[str] | None = None,   # <-- IGNORED
) -> dict[str, Any]:
    """_k: Number of hops (currently unused, defaults to 1-hop)
       _edge_types: Optional edge types to follow (currently unused)"""
```

The function always performs a hardcoded 1-hop traversal regardless of `_k`. The `_edge_types` parameter is completely ignored. The underscore prefix attempts to hide this.

**Fix:**
Either implement the parameters or remove them and rename the function:

```python
# Option A: implement
async def k_hop_query(dao, start_id, k: int, edge_types=None):
    # Build recursive query with k levels of nesting
    ...

# Option B: honest interface
async def one_hop_neighbors(dao, start_id):
    """Get immediate neighbors of a node (1-hop only)."""
    ...
```

---

### 33. `traverse()` Ignores `_filters` Parameter

**File:** `ami/implementations/graph/dgraph_traversal.py:405-410`

**Problem:**
```python
async def traverse(self, start_uid, edge_path, _filters=None):
```

`_filters` is accepted but never passed to `_build_traverse_query()` or used anywhere in the function body.

**Fix:** Implement filtering or remove the parameter.

---

### 34. `raw_write_query` Ignores `_params` Parameter

**File:** `ami/implementations/graph/dgraph_update.py:384-386`

**Problem:**
```python
async def raw_write_query(dao, query, _params=None):
```

`_params` is never passed to the Dgraph mutation. Parameterized writes are impossible despite the interface suggesting otherwise.

**Fix:** Pass params to the mutation or remove the parameter.

---

### 35. `_log_access` Discards Permission and Result Parameters

**File:** `ami/models/secured_mixin.py:124-135`

**Problem:**
```python
def _log_access(self, context, _permission, _result):
    # Only records WHO accessed, not WHAT permission or WHETHER granted/denied
    if context.user_id not in self.accessed_by:
        self.accessed_by.append(context.user_id)
    self.accessed_at.append(datetime.now(UTC))
```

Called with meaningful data:
```python
self._log_access(context, permission, "GRANTED")   # line 63
self._log_access(context, permission, "DENIED")    # line 74
self._log_access(context, permission, "NO_MATCH")  # line 92
```

But `_permission` and `_result` are thrown away. The audit trail records who accessed but not what permission was checked or whether access was granted or denied. Useless for security forensics.

**Fix:**
```python
def _log_access(self, context, permission, result):
    entry = {
        "user_id": context.user_id,
        "permission": permission.value,
        "result": result,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    self._access_log.append(entry)
    if len(self._access_log) > _MAX_ACCESS_ENTRIES:
        self._access_log = self._access_log[-_MAX_ACCESS_ENTRIES:]
```

---

### 36. Sensitive Fields Stored Plaintext on ImportError

**File:** `ami/models/base_model.py:186-189`

**Problem:**
```python
def to_storage_dict(self, context=None):
    if getattr(self.__class__, "_sensitive_fields", None):
        try:
            from ami.secrets.adapter import prepare_instance_for_storage
            return prepare_instance_for_storage(self, data, context)
        except ImportError:
            pass  # <-- SILENTLY STORES SENSITIVE FIELDS IN PLAINTEXT
    return data
```

If the secrets adapter module is unavailable (missing dependency, broken import), fields marked as sensitive are stored in plaintext with zero warning.

**Fix:**
Fail loudly when sensitive fields exist but the adapter is unavailable:

```python
if getattr(self.__class__, "_sensitive_fields", None):
    try:
        from ami.secrets.adapter import prepare_instance_for_storage
    except ImportError as e:
        msg = (
            f"Model {self.__class__.__name__} has sensitive fields but "
            "ami.secrets.adapter is not available. Cannot store safely."
        )
        raise ImportError(msg) from e
    return prepare_instance_for_storage(self, data, context)
```

---

## HIGH Issues

### 37. SQL Parameter Order Loss in Raw Queries

**File:** `ami/implementations/sql/postgresql_dao.py:209-211`

**Problem:**
```python
param_values = list(params.values())
rows = await conn.fetch(query, *param_values)
```

Converts dict to positional list via `.values()`. Dict insertion order doesn't necessarily match `$1, $2, $3` placeholder order in the SQL query.

**Fix:**
Accept params as a list (matching positional `$N` placeholders) or require ordered mapping:

```python
async def raw_read_query(self, query: str, params: list[Any] | None = None):
    if params:
        rows = await conn.fetch(query, *params)
```

---

### 38. Empty SET Clause Generates Invalid SQL

**File:** `ami/implementations/sql/postgresql_update.py:37-50`

**Problem:**
If all field names in `data` fail `is_valid_identifier()`, `set_clauses` is empty and the SQL becomes:
```sql
UPDATE table_name SET  WHERE id = $1
```
This is syntactically invalid. The error is caught and wrapped in a generic `StorageError` that hides the root cause.

**Fix:**
Check for empty clauses before building SQL:

```python
if not set_clauses:
    msg = f"No valid fields to update in {data.keys()}"
    raise StorageValidationError(msg)
```

---

### 39. UID Registry Creates False Negatives on Read

**File:** `ami/core/unified_crud.py:229-234`

**Problem:**
```python
async def read_by_uid(self, uid):
    if uid in self._uid_registry:
        return await self.read(...)
    return None  # UIDs from other instances or restarts are invisible
```

UIDs that exist in storage but weren't created through this `UnifiedCRUD` instance return `None`.

**Fix:**
Fall through to storage lookup when registry miss occurs:

```python
async def read_by_uid(self, uid):
    if uid in self._uid_registry:
        model_class, config_index = self._uid_registry[uid]
        return await self.read(model_class, uid, config_index)
    # Registry miss -- try all registered model classes
    for model_class in self._registered_models:
        result = await self.read(model_class, uid)
        if result:
            return result
    return None
```

---

### 40. `_row_to_model` Silently Returns Wrong Type

**Files:**
- `ami/implementations/sql/postgresql_dao.py:412-421`
- `ami/implementations/vec/pgvector_read.py:321-325`

**Problem:**
```python
try:
    return self.model_cls.from_storage_dict(row)
except Exception:
    logger.debug("Could not hydrate row...")
    return row  # Returns dict instead of model -- breaks type contract
```

Callers expect a model instance but silently receive a raw dict. Logged at DEBUG level only.

**Fix:**
Log at WARNING level and raise. Callers that want raw dicts should use `raw_read_query()`:

```python
try:
    return self.model_cls.from_storage_dict(row)
except Exception as e:
    msg = f"Failed to hydrate {self.model_cls.__name__} from row"
    raise StorageError(msg) from e
```

---

### 41. `query.pop()` Mutates Caller's Dict

**File:** `ami/implementations/timeseries/prometheus_dao.py:221`

**Problem:**
```python
metric_name = query.pop("metric_name", self._metric_name)
```

Mutates the caller's dictionary. Calling `find()` twice with the same dict produces different results.

**Fix:**
```python
query = dict(query)  # Work on a copy
metric_name = query.pop("metric_name", self._metric_name)
```

---

### 42. REST DAO Returns String `"None"` as Record ID

**File:** `ami/implementations/rest/rest_dao.py:176-179`

**Problem:**
```python
return str(extracted)  # str(None) == "None"
```

If extraction yields `None`, the string `"None"` is returned as the record ID.

**Fix:**
```python
if extracted is None:
    msg = "REST API returned no ID for created record"
    raise StorageError(msg)
```

---

### 43. `to_dgraph_rule()` Returns Empty String for Unknown Types

**File:** `ami/models/security.py:146-152`

**Problem:**
```python
def to_dgraph_rule(self) -> str:
    if self.rule_type == "jwt":
        return str(self.rule_config.get("query", ""))
    if self.rule_type == "graph_traversal":
        return str(self.rule_config.get("traversal", ""))
    return ""  # No auth rule = open door
```

Unknown rule types silently produce empty auth rules, meaning no authentication is applied.

**Fix:**
```python
def to_dgraph_rule(self) -> str:
    if self.rule_type == "jwt":
        rule = self.rule_config.get("query", "")
    elif self.rule_type == "graph_traversal":
        rule = self.rule_config.get("traversal", "")
    else:
        msg = f"Unsupported auth rule type: {self.rule_type!r}"
        raise ValueError(msg)
    if not rule:
        msg = f"Empty auth rule for type {self.rule_type!r}"
        raise ValueError(msg)
    return str(rule)
```

---

### 44. Fragile String Parsing of asyncpg Result Status

**Files:**
- `ami/implementations/sql/postgresql_delete.py:39`
- `ami/implementations/sql/postgresql_update.py:60`
- `ami/implementations/vec/pgvector_delete.py:25`

**Problem:**
```python
deleted = result.split()[-1] == "1" if result else False
```

Parses asyncpg's status string (e.g., `"DELETE 1"`) by splitting and checking the last token. Fragile, undocumented, and `"DELETE 21".endswith("1")` would give false positive in pgvector variant.

**Fix:**
Parse robustly:

```python
def _parse_affected_count(result: str | None) -> int:
    """Parse asyncpg command status like 'DELETE 3' or 'UPDATE 1'."""
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

---

### 45. Decryption Failure Returns Fake Data Silently

**File:** `ami/security/encryption.py:94-96`

**Problem:**
```python
except Exception:
    logger.exception("Decryption failed")
    return "[DECRYPTION_FAILED]"
```

Returns a sentinel string instead of raising. Callers receive fake data and have no way to distinguish it from a real value that happens to contain `[DECRYPTION_FAILED]`.

**Fix:**
Raise a `DecryptionError`:

```python
class DecryptionError(StorageError):
    """Raised when field decryption fails."""

# In decrypt():
except Exception as e:
    msg = f"Decryption failed for field"
    raise DecryptionError(msg) from e
```

---

### 46-48. Silent Error Swallowing in DAO Operations

**46. Embedding failure silently returns None** -- `vec/pgvector_dao.py:337-341`
```python
except Exception:
    logger.warning("Embedding generation failed...")
    return None  # Records stored without embeddings, vector search broken
```

**47. REST count falls back to full table scan** -- `rest/rest_dao.py:284-288`
```python
except StorageError:
    pass  # Fetches ALL records and counts them
```

**48. Vault listing returns empty on error** -- `vault/openbao_dao.py:225-226`
```python
except OpenBaoError:
    return []  # Vault down? Apparently no secrets exist.
```

**Fix for all:** Log at ERROR level and propagate the exception. Let callers decide how to handle failures instead of silently degrading.

---

## MEDIUM Issues

### 49. Production `assert` Statements

**Files:** 12+ files across all backends

Pattern:
```python
if not self.pool:
    await self.connect()
assert self.pool is not None  # Disabled with python -O
```

**Fix:**
```python
if not self.pool:
    await self.connect()
if self.pool is None:
    msg = "Database pool not initialized after connect()"
    raise StorageError(msg)
```

---

### 50. Overly Broad `except Exception` Catches

**Files:** 20+ locations across all backend DAOs

Every connection test, row hydration, index creation, and several CRUD operations catch bare `except Exception:`. This catches `KeyboardInterrupt` equivalents and masks the distinction between transient network errors, permanent auth failures, and data corruption.

**Fix:**
Catch specific exception types per backend:
- asyncpg: `asyncpg.PostgresError`, `asyncpg.InterfaceError`
- aiohttp: `aiohttp.ClientError`, `asyncio.TimeoutError`
- Redis: `redis.RedisError`
- Dgraph: `pydgraph.AbortedError`, `grpc.RpcError`

---

### 51. Index Creation Failures Swallowed

**Files:**
- `ami/implementations/sql/postgresql_util.py:136-162`
- `ami/implementations/vec/pgvector_util.py:175-188, 219-222`

```python
except Exception:
    logger.warning("Failed to create GIN index...")
```

Index creation failures are logged at WARNING and swallowed. Query performance degrades silently.

**Fix:**
Propagate the exception or collect all failures and report them:

```python
index_errors: list[str] = []
try:
    await conn.execute(...)
except Exception as e:
    index_errors.append(f"{col}: {e}")

if index_errors:
    logger.error("Failed to create indexes: %s", "; ".join(index_errors))
    # Optionally raise or return error list to caller
```

---

### 52. Double Transaction Discard in Graph Layer

**File:** `ami/implementations/graph/dgraph_update.py:248-289`

`txn.discard()` is called in both except blocks AND the finally block. While pydgraph handles this gracefully, it's unnecessary and confusing.

**Fix:** Only discard in finally:

```python
txn = dao.client.txn()
try:
    # ... mutation ...
    txn.commit()
except Exception as e:
    msg = f"Failed to update: {e}"
    raise StorageError(msg) from e
finally:
    txn.discard()  # Only here
```

---

### 53. ContextVar Never Cleaned Up

**File:** `ami/secrets/adapter.py:23-25, 133-143`

`_POINTER_CONTEXT` is set during hydration but only cleared if `consume_pointer_cache()` is explicitly called. If it's never called, sensitive pointer data leaks across async task boundaries.

**Fix:**
Use a context manager pattern:

```python
@contextlib.contextmanager
def pointer_context():
    try:
        yield
    finally:
        _POINTER_CONTEXT.set(None)
```

---

### 54. No Transaction Timeouts in Graph Layer

**Files:** `ami/implementations/graph/dgraph_graph.py`, `dgraph_read.py`, `dgraph_traversal.py`, `dgraph_update.py`

```python
txn = dao.client.txn(read_only=True)
response = txn.query(query)  # No timeout
```

A slow or malicious query hangs the event loop indefinitely.

**Fix:**
Set gRPC deadline on the transaction or use `asyncio.wait_for()`:

```python
response = await asyncio.wait_for(
    asyncio.to_thread(txn.query, query),
    timeout=30.0,
)
```

---

### 55. No HTTP Request Timeout Default

**File:** `ami/utils/http_client.py:83-87`

```python
response = await session.request(method, url, **kwargs)
```

No default timeout. Requests can hang forever if caller doesn't set one.

**Fix:**
```python
DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30)

async def request_with_retry(session, method, url, *, retry_cfg=..., **kwargs):
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    ...
```

---

### 56. Silent Field Skipping on Invalid Identifiers

**Files:**
- `ami/implementations/sql/postgresql_update.py:37-41`
- `ami/implementations/sql/postgresql_create.py:332-336`
- `ami/implementations/vec/pgvector_update.py:44-46`

Fields that fail `is_valid_identifier()` are silently dropped. Users believe their update succeeded when fields were quietly ignored.

**Fix:**
Log a warning for each skipped field or collect and raise:

```python
skipped = [k for k in data if not is_valid_identifier(k) and k != "id"]
if skipped:
    logger.warning("Skipped invalid field names: %s", skipped)
```

---

### 57. Hardcoded Datetime Field List in Graph Updates

**File:** `ami/implementations/graph/dgraph_update.py:219-224`

```python
if key in ["created_at", "updated_at", "verified_at", "last_login"]:
```

Only these 4 fields get datetime conversion. Any other datetime field is silently stored as a string.

**Fix:**
Use type annotation introspection or check `isinstance(value, datetime)`:

```python
if isinstance(value, datetime):
    nquad_value = f'"{value.isoformat()}"^^<xs:dateTime>'
```

---

### 58. Redis Cache Miss Indistinguishable From Real Errors

**File:** `ami/implementations/mem/redis_update.py:97-101`

```python
except Exception:
    existing_data = None  # "Cache miss" -- could also be Redis crash
```

All exceptions are treated as cache misses.

**Fix:**
Catch only the expected exception for missing keys:

```python
from redis.exceptions import RedisError

try:
    existing_data = await read(dao, item_id)
except KeyError:
    existing_data = None  # Actual cache miss
except RedisError:
    raise  # Real error -- propagate
```

---

### 59-61. Additional Medium Issues

**59. Bulk create is sequential, no transaction wrapping** -- All DAOs loop `await self.create()` individually. Partial failures leave data in an inconsistent state.

**60. Incomplete vault path traversal validation** -- `openbao_dao.py:66-78` checks for `..` and leading `/` but allows newlines and other special characters.

**61. Credentials embedded in connection strings** -- `models/storage_config.py` builds URLs with raw passwords. Passwords containing `@`, `:`, or `/` produce malformed URLs. Use `urllib.parse.quote_plus()`.

---

## LOW Issues

### 62. Unreachable `type(None)` Check

**File:** `ami/core/graph_relations.py:107`

```python
if target_type is type(None):  # Always False
```

`type(None)` is `<class 'NoneType'>`. This branch never executes. Dead code.

---

### 63. Always-True `if vector:` Check

**File:** `ami/implementations/embedding_service.py:216`

```python
vector = [0.0] * self.embedding_dim
if vector:  # Always True when embedding_dim > 0
```

---

### 64. Hardcoded Skip Fields Not Configurable

**File:** `ami/implementations/graph/dgraph_util.py:126-127, 379`

```python
skip_fields = {"id", "uid", "storage_configs", "path"}
```

Not configurable or overridable per model.

---

### 65. Dead Configuration

**File:** `config/storage-config.yaml`

- `is_ground_truth: true` on Dgraph -- never read by code
- `role_id`, `secret_id`, `jwt`, `kubernetes_role` on OpenBao -- never read by code

---

### 66-69. Testing Issues

**66.** Integration coverage threshold is 50% (`res/config/coverage_thresholds.yaml:8`). Should be at least 80%.

**67.** Zero usage of `mock`, `Mock`, `patch`, or `MagicMock` anywhere in `tests/`. No mocked boundary testing.

**68.** `tests/integration/` and `tests/unit/adapters/` are empty (only `__init__.py`).

**69.** `to_dgraph_rule()` in `models/security.py` has zero test coverage.

---

### 70. Incomplete Type Mappings

**Files:**
- `ami/implementations/sql/postgresql_create.py:98-124`
- `ami/implementations/graph/dgraph_util.py:18-26`

Missing mappings for `UUID`, `Decimal`, `bytes`, `date` (non-datetime). Unknown types silently default to `TEXT`/`string`.

---

## Revision Priority Order

### Phase 1 -- Security (CRITICAL, immediate)

| Issue | Action |
|-------|--------|
| 23 | Remove hardcoded key/salt fallbacks, require env vars |
| 24 | Use random salt per encrypted value |
| 25 | Replace SHA-256 with bcrypt for password hashing |
| 26 | Fail on missing master key instead of generating random |
| 27 | Parameterize all DQL queries |
| 28 | Escape PromQL label values |
| 29 | Default to HTTPS for vault, make TLS configurable |
| 36 | Fail loudly when secrets adapter unavailable |
| 45 | Raise exception on decryption failure |

### Phase 2 -- Remove Placebo Code (CRITICAL)

| Issue | Action |
|-------|--------|
| 30 | Implement event recording or remove decorator |
| 31 | Implement Redis caching or remove backend param |
| 32 | Implement k-hop traversal or rename to `one_hop_neighbors` |
| 33 | Implement filtering or remove `_filters` param |
| 34 | Implement params or remove `_params` param |
| 35 | Record permission and result in audit log |

### Phase 3 -- Fix Broken Logic (HIGH)

| Issue | Action |
|-------|--------|
| 37 | Change raw query params from dict to list |
| 38 | Validate non-empty SET clause before SQL execution |
| 39 | Fall through to storage on registry miss |
| 40 | Raise on hydration failure instead of returning dict |
| 41 | Copy query dict before mutating |
| 42 | Raise on missing ID instead of returning "None" |
| 43 | Raise on unknown auth rule type |
| 44 | Robust asyncpg status parsing |
| 46-48 | Propagate errors instead of swallowing |

### Phase 4 -- Error Handling and Resources (MEDIUM)

| Issue | Action |
|-------|--------|
| 49 | Replace `assert` with explicit checks |
| 50 | Narrow exception catches to specific types |
| 51 | Propagate index creation errors |
| 52-55 | Fix resource management (transactions, timeouts, cleanup) |
| 56-61 | Remaining medium issues |

### Phase 5 -- Quality and Testing (LOW)

| Issue | Action |
|-------|--------|
| 62-65 | Remove dead code and config |
| 66-69 | Fix test infrastructure |
| 70 | Extend type mappings |
