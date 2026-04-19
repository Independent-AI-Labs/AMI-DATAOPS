# `ami-report` + `ami-intake`: Technical Specification

**Date:** 2026-04-19
**Status:** ACTIVE
**Type:** Specification
**Requirements:** [REQ-REPORT.md](../requirements/REQ-REPORT.md)

This specification describes behaviour, not code. For the implementation, see `projects/AMI-DATAOPS/ami/dataops/report/` (sender), `projects/AMI-DATAOPS/ami/dataops/intake/` (receiver), and `projects/AMI-DATAOPS/res/ansible/intake.yml`.

---

## 1. Pipelines

### 1.1 Sender pipeline (`ami-report send`)

```
┌─────────────────┐
│ 1. Resolve       │  Read dataops_report_sender_config + peers from
│    inventory     │  inventory. Validate sender_id, peer endpoint,
│                  │  required env vars present.
├─────────────────┤
│ 2. Scan roots    │  Walk AMI_ROOT/logs plus extra_roots. Emit every
│                  │  candidate file with size, mtime, ext-check,
│                  │  null-byte probe result.
├─────────────────┤
│ 3. TUI select    │  SelectionDialog group-per-dir, file-per-child.
│                  │  Disabled rows for candidates that fail pre-flight.
│                  │  Empty result: exit 2.
├─────────────────┤
│ 4. Pick peer     │  dialogs.select over dataops_report_peers.
│                  │  Auto-advance if only one peer configured.
├─────────────────┤
│ 5. Build         │  Compute per-file SHA256. Assemble JSON manifest.
│    manifest      │  Canonicalise (sorted keys, UTF-8, LF terminator).
├─────────────────┤
│ 6. Sign          │  X-AMI-Signature = HMAC-SHA256(secret, manifest).
├─────────────────┤
│ 7. Pack          │  tar -cz of the selected files at their relative
│                  │  paths under source_root.
├─────────────────┤
│ 8. Confirm       │  dialogs.confirm shows: N files, M MiB, destination,
│                  │  bundle_id. [y/N]. Cancel: exit 0.
├─────────────────┤
│ 9. POST          │  multipart/form-data to https://<endpoint>/v1/bundles.
│                  │  Retries 3x on transient 5xx. Honours Retry-After.
├─────────────────┤
│10. Report        │  Render receipt summary (accepted or reject reason).
│    receipt       │  Exit 0 on accept, 3/4/5/6/10 per REQ §16.
└─────────────────┘
```

### 1.2 Receiver pipeline (`ami-intake serve`, per request)

```
┌─────────────────┐
│ 1. Authn         │  Authorization: Bearer <token> matches
│                  │  AMI_INTAKE_TOKENS__<SENDER_ID>. On miss: 401.
├─────────────────┤
│ 2. Verify HMAC   │  X-AMI-Signature matches HMAC-SHA256 over
│                  │  canonical manifest bytes, per-sender secret.
│                  │  Constant-time compare. On miss: 401.
├─────────────────┤
│ 3. Header match  │  X-AMI-Sender-Id and X-AMI-Bundle-Id equal the
│                  │  manifest's fields. On miss: 401.
├─────────────────┤
│ 4. Quota gate    │  Bundle size <= max_bundle_mb, file count <=
│                  │  max_files_per_bundle. On excess: 413.
├─────────────────┤
│ 5. Idempotency   │  Look up (sender_id, bundle_id) in quarantine.
│                  │  Present -> return original receipt with 200.
├─────────────────┤
│ 6. Unpack        │  Stream-extract tar to staging tmpdir, running
│                  │  byte counter + file counter, reject symlinks,
│                  │  reject paths with '..' or absolute prefixes.
├─────────────────┤
│ 7. Validate      │  For each file: ext allowlist, null-byte probe,
│                  │  per-file size cap, SHA256 match vs manifest.
│                  │  Any failure: abort, delete staging, 400.
├─────────────────┤
│ 8. Quarantine    │  Atomic move to <intake_root>/<sender_id>/
│                  │  <YYYY>/<MM>/<DD>/<bundle_id>/. Write
│                  │  manifest.json + receipt.json. chmod 0640/0440.
├─────────────────┤
│ 9. Audit         │  Append NDJSON record to audit.log, fsync.
│                  │  Chain prev_hash from previous record.
├─────────────────┤
│10. Respond       │  202 with receipt JSON. The audit record is the
│                  │  authoritative acceptance even if the response
│                  │  is lost in transit.
└─────────────────┘
```

---

## 2. Inventory layout

Declared under `host_vars/<host>/report.yml` (sender) or `host_vars/<host>/intake.yml` (receiver), or merged into `group_vars/all/`.

### 2.1 Sender

```yaml
dataops_report_sender_config:
  sender_id: alpha                # stable short name; matches receiver allowlist
  extra_roots:
    - /var/log/ami
  default_ci_defaults: ami/config/report-ci.yml

dataops_report_peers:
  - name: bravo
    endpoint: https://intake.bravo.example.com
    shared_secret_env_var: AMI_REPORT_SECRET_BRAVO
  - name: charlie
    endpoint: https://10.0.0.42:8443
    shared_secret_env_var: AMI_REPORT_SECRET_CHARLIE
```

Required on each peer: `name`, `endpoint`, `shared_secret_env_var`. The matching bearer token is read from `AMI_REPORT_TOKENS__<PEER_NAME>` (uppercase).

### 2.2 Receiver

```yaml
dataops_intake_config:
  intake_port: 9180
  intake_root: /home/ami/AMI-AGENTS/logs/intake    # default
  persist: true
  max_file_mb: 100
  max_bundle_mb: 500
  max_files_per_bundle: 1000
  global_concurrency: 4
  allowed_senders:
    - alpha
    - bravo
```

Secrets and bearer tokens are **not** in inventory. They live in the host `.env` under `AMI_INTAKE_TOKENS__<SENDER_ID>` and `AMI_INTAKE_SECRETS__<SENDER_ID>`.

---

## 3. Manifest & signing

### 3.1 Canonical shape

```json
{
  "bundle_id": "01J8RGBDY6FH7S9X0P4M4V6NQ2",
  "files": [
    {
      "mtime": "2026-04-19T08:11:04Z",
      "relative_path": "banner/banner-20260419T081104Z.log",
      "sha256": "b2c3e4f5...",
      "size_bytes": 14532
    }
  ],
  "schema_version": 1,
  "sender_id": "alpha",
  "sent_at": "2026-04-19T08:12:00Z",
  "source_root": "/home/ami/AMI-AGENTS/logs"
}
```

### 3.2 Canonicalisation rules

- JSON keys sorted lexicographically at every object level.
- UTF-8 encoding.
- No trailing whitespace on any line.
- Exactly one LF terminator at the end of file.
- Floats forbidden: all integer fields are integers; timestamps are RFC3339 strings.
- Array order is significant: `files[]` is in the order the sender chose to emit (typically lexicographic, but any deterministic order is allowed as long as it is preserved across signing and verification).

### 3.3 Signature

```
X-AMI-Signature: sha256=<hex>
where hex = HEX(HMAC-SHA256(shared_secret_utf8, canonical_manifest_bytes))
```

The receiver recomputes the HMAC over the bytes it received (not a re-serialised JSON) and compares in constant time. Any normalisation performed by the sender is also performed by the receiver's `json.loads` + `json.dumps(..., sort_keys=True, separators=(",", ":"))` round trip only as a sanity check; the authoritative compare is against the received bytes.

### 3.4 Schema version migration

A v1 receiver shall return HTTP 400 with `reason_code: "schema_unsupported"` for any `schema_version != 1`. v2 is expected to add optional fields; v2 receivers shall accept v1 and v2; v1 senders will never emit v2 bundles.

---

## 4. Wire format

`POST https://<endpoint>/v1/bundles`

Headers:

```
Authorization: Bearer <opaque-token>
X-AMI-Sender-Id: alpha
X-AMI-Bundle-Id: 01J8RGBDY6FH7S9X0P4M4V6NQ2
X-AMI-Signature: sha256=<hex>
Content-Type: multipart/form-data; boundary=<boundary>
Content-Length: <N>
```

Body:

```
--<boundary>
Content-Disposition: form-data; name="manifest"
Content-Type: application/json

<canonical manifest bytes>
--<boundary>
Content-Disposition: form-data; name="bundle"; filename="bundle.tar.gz"
Content-Type: application/gzip

<gzip stream>
--<boundary>--
```

Exactly two parts. Any extra part rejects the request with 400.

Compression is gzip. xz / zstd / bzip2 are out of scope for v1.

---

## 5. Receipt

```json
{
  "audit_log_offset": 73215,
  "bundle_id": "01J8RGBDY6FH7S9X0P4M4V6NQ2",
  "per_file_sha256_verified": [
    {
      "relative_path": "banner/banner-20260419T081104Z.log",
      "sha256": "b2c3e4f5..."
    }
  ],
  "received_at": "2026-04-19T08:12:01Z",
  "status": "accept"
}
```

HTTP status mapping:

| HTTP | Meaning | Sender action |
|------|---------|---------------|
| 200  | Idempotent replay of a prior accept | Treat as success. |
| 202  | Accepted, quarantined, audit-logged | Treat as success. |
| 400  | Validation reject with `reason_code` | Do not retry. Exit 5. |
| 401  | Auth reject (bearer or HMAC) | Do not retry. Exit 4. |
| 413  | Quota reject (file or bundle too large) | Do not retry. Exit 5. |
| 429  | Global concurrency exceeded | Wait per `Retry-After`, then retry within budget. |
| 5xx  | Transient | Retry per §5 of REQ. |

Reject bodies always contain `{"status": "reject", "reason_code": "<enum>", "detail": "<message>"}`.

---

## 6. Validation rules in detail

All rules live in `ami/dataops/intake/validation.py` as pure callables with zero I/O side effects beyond reading the staging tmpdir. Each raises a typed `ValidationRejected(reason_code, detail)` on violation.

| # | Rule | Reason code |
|---|------|-------------|
| 1 | Extension on allowlist | `ext_not_allowed` |
| 2 | Path contains no `..`, is not absolute, no symlink targets | `path_unsafe` |
| 3 | First 8 KiB contains no NUL byte | `not_text` |
| 4 | Per-file size <= `max_file_mb` | `file_too_large` |
| 5 | Aggregate size <= `max_bundle_mb` | `bundle_too_large` |
| 6 | File count <= `max_files_per_bundle` | `too_many_files` |
| 7 | SHA256 matches the manifest entry | `hash_mismatch` |

Rules are evaluated in the order above. The first failing rule wins and aborts the bundle. No partial acceptance.

The rules are exported as pure functions so AMI-AGENTS core (or any other project) can import and call them from a different receiving pipeline without pulling FastAPI or uvicorn.

---

## 7. Audit log format

`<intake_root>/audit.log` — NDJSON, append-only, one record per request, flushed and fsynced before the HTTP response is returned.

### 7.1 Accept record

```json
{
  "byte_count": 48293,
  "bundle_id": "01J8RGBDY6FH7S9X0P4M4V6NQ2",
  "event": "accept",
  "file_count": 3,
  "prev_hash": "e7c4...ab12",
  "receipt_sha256": "9f3e...0cd7",
  "reject_reason": null,
  "remote_addr": "10.0.0.17",
  "sender_id": "alpha",
  "ts": "2026-04-19T08:12:01Z"
}
```

### 7.2 Reject record

```json
{
  "byte_count": 48293,
  "bundle_id": "01J8RGBDY6FH7S9X0P4M4V6NQ2",
  "event": "reject",
  "file_count": 0,
  "prev_hash": "e7c4...ab12",
  "receipt_sha256": "9f3e...0cd7",
  "reject_reason": "ext_not_allowed",
  "remote_addr": "10.0.0.17",
  "sender_id": "alpha",
  "ts": "2026-04-19T08:12:05Z"
}
```

Auth rejects (bearer or HMAC) log `sender_id` as the value claimed in the header (may be unverified) with `event: "reject"` and `reject_reason: "auth"`.

### 7.3 Chain hash

`prev_hash` = SHA256 of the previous record's exact bytes (the UTF-8-encoded line including its trailing LF, but excluding the LF that separates records). The first record of a fresh `audit.log` carries `prev_hash` equal to the `seal_hash` of the previous rotated file, or 64 zero hex chars if none exists.

### 7.4 Rotation (`ami-intake rotate-audit`)

1. Compute `seal_hash` = SHA256 over the entire current `audit.log` content.
2. Append one terminal `seal` record: `{"event":"seal","prev_hash":"<last>","seal_hash":"<hex>","ts":"..."}`.
3. fsync and close.
4. `chmod 0440`.
5. `mv audit.log audit/<YYYY-MM-DDThhmmssZ>.log`.
6. Open a new `audit.log`; write no records yet. The next accepted bundle record's `prev_hash` is `seal_hash`.

Auditors verify a chain across rotations by walking `audit/*.log` in chronological order and confirming each first record's `prev_hash` equals the previous file's `seal_hash`.

---

## 8. TUI flow

Three screens, all reusing existing primitives from `ami/cli_components/`.

### 8.1 File selection (`SelectionDialog`, multi-select)

```
┌─ Select log files to report ──────────────────────────────────────┐
│   [□] AMI_ROOT/logs                                                │
│      [ ] banner/banner-20260419T081104Z.log    14.2 KiB            │
│      [ ] banner/banner-20260418T091501Z.log    12.8 KiB            │
│      [◧] serve/                                                     │
│         [x] serve/ami-serve-20260419T081104Z.log  9.4 KiB          │
│         [ ] serve/ami-serve-20260418T091501Z.log  8.8 KiB          │
│   [□] /var/log/ami                                                  │
│      [ ] app.log                                42.1 KiB            │
│ ▼ 12 more below                                                     │
└ ↑/↓: navigate, Space: toggle, a: all, n: none, Enter: ok, Esc: cancel ┘
```

Directories render as group headers so space-toggling a header selects every non-disabled child under it — the existing SelectionDialog group-toggle behaviour. Pre-flight failures render as dimmed rows with a suffix like `(not .log)` or `(binary)` and cannot be toggled.

### 8.2 Peer selection (`dialogs.select`, single-select)

```
┌─ Choose destination ──────────────────────────────────────────────┐
│ > bravo      https://intake.bravo.example.com     (token: set)     │
│   charlie    https://10.0.0.42:8443                (token: set)     │
│   delta      https://intake.delta.example.com      (token: MISSING) │
└ ↑/↓: navigate, Enter: ok, Esc: cancel ────────────────────────────┘
```

Peers whose bearer token env var is unset render dimmed and cannot be selected.

### 8.3 Confirmation (`dialogs.confirm`)

```
┌─ Confirm report ──────────────────────────────────────────────────┐
│ Destination: bravo (https://intake.bravo.example.com)              │
│ Bundle ID:   01J8RGBDY6FH7S9X0P4M4V6NQ2                            │
│ Files:       7                                                     │
│ Total size:  94.3 KiB                                              │
│                                                                    │
│ Send now? [y/N]                                                    │
└────────────────────────────────────────────────────────────────────┘
```

After confirmation, progress is rendered inline via `TUI.draw_box` with one update per 5 % of bytes sent or once per second.

---

## 9. Error taxonomy

| Condition | HTTP | Sender exit | Retry? | Operator action |
|-----------|------|-------------|--------|-----------------|
| Missing env var (secret or token) | n/a | 2 | no | Set env var, re-run. |
| Empty selection | n/a | 2 | no | Select at least one file. |
| Plain HTTP to public address | n/a | 2 | no | Use HTTPS endpoint. |
| Connection refused | n/a | 3 | yes, 3× | Confirm daemon running, check firewall. |
| TLS handshake failure | n/a | 3 | no | Confirm certificate valid for hostname. |
| 401 bearer reject | 401 | 4 | no | Confirm receiver has our token/secret. |
| 401 HMAC reject | 401 | 4 | no | Confirm shared_secret matches receiver's. |
| 400 ext_not_allowed | 400 | 5 | no | Remove the offending file from selection. |
| 400 not_text (NUL byte) | 400 | 5 | no | File is not a text log. |
| 400 path_unsafe | 400 | 5 | no | Internal bug: report to maintainers. |
| 413 file_too_large | 413 | 5 | no | Split or truncate, re-select. |
| 413 bundle_too_large | 413 | 5 | no | Send in multiple smaller bundles. |
| 429 with Retry-After | 429 | 0 on eventual accept, else 3 | yes, within 300 s budget | Receiver under load; no action. |
| 5xx unclassified | 5xx | 3 | yes, 3× | Check receiver logs. |
| Local hash mismatch (file changed during send) | n/a | 6 | no | Re-run; avoid rotating logs during send. |
| Disk full on receiver | 500 | 3 | yes, 3× | Receiver operator frees space. |

---

## 10. Concurrency

The intake daemon gates upload handlers on two levels:

1. **Global semaphore** of size `dataops_intake_config.global_concurrency` (default 4). Any request that would exceed it gets 429 with `Retry-After: <seconds>` computed from a rolling estimate.
2. **Per-sender advisory file lock** on `<intake_root>/<sender_id>/.lock`, acquired for the duration of extraction + validation + quarantine + audit. This serialises bundles from the same sender so their audit records land in the correct order relative to each other.

Metrics endpoint (`/metrics`) and status endpoints are not gated by the semaphore.

---

## 11. Threat model

### 11.1 In scope

| Threat | Mitigation |
|--------|-----------|
| Malicious sender uploads an executable | Extension allowlist rejects non-allowlisted extensions; null-byte probe rejects binary payloads; atomic reject on first failure. |
| Malicious sender uses path traversal (`../../etc/passwd`) | Tar entries with `..` or absolute prefixes rejected; symlinks in the tar stream rejected outright. |
| Zip bomb | Streaming extraction with running byte counter; per-file and aggregate caps enforced mid-stream. |
| Symlink escape post-extract | Staging tmpdir scanned for symlinks after extract; any found rejects the bundle. |
| Stolen bearer token | HMAC signature requires the shared secret; attacker needs both credentials. Rotate token by env var swap. |
| Stolen shared secret | Bearer token required at the HTTP layer; attacker needs both. |
| Replay of a captured bundle | Idempotent on `bundle_id`: duplicate returns the original receipt with 200. Semantically harmless; still audit-logged on first acceptance. |
| Tampering with audit log | Chain-hash + rotation seal: any edit or missing line breaks the chain, detectable by `ami-intake verify-chain` (a future CLI, not shipped in v1; raw `prev_hash` chain is auditable by shell scripts today). |
| Byzantine sender with partial bundle | Atomic accept-or-reject blocks it. No path permits partial quarantine. |

### 11.2 Out of scope

- **Compromised receiver host.** If the receiver host is rooted, all bets are off. The audit chain is tamper-evident against naive edits but not against a root-level attacker who recomputes the entire chain; an external-witness signature is a v2 concern.
- **Denial of service by flood of rejected bundles.** 429 and the concurrency semaphore throttle; an attacker who can mint valid bearer tokens has bigger problems than this endpoint.
- **Side-channel timing attacks on HMAC comparison.** Constant-time compare in `hmac.compare_digest` is assumed sufficient.
- **Malicious Cloudflare Tunnel.** This is a trust-CF situation, same as every other tunnel-fronted service.

---

## 12. Extension wiring

`projects/AMI-DATAOPS/extension.manifest.yaml` gains two entries:

```yaml
  - name: ami-report
    binary: projects/AMI-DATAOPS/ami/dataops/report/main.py
    description: Multi-select + ship log files to a peer AMI instance
    category: ops
    features: [send, preview, peers]
    bannerPriority: 230
    check:
      command: ["{python}", "{binary}", "--help"]
      healthExpect: "ami-report"
      timeout: 5

  - name: ami-intake
    binary: projects/AMI-DATAOPS/ami/dataops/intake/main.py
    description: Receive, validate, and quarantine remote log bundles
    category: infra
    features: [serve, status, ls, show, verify, rotate-audit]
    bannerPriority: 240
    check:
      command: ["{python}", "{binary}", "--help"]
      healthExpect: "ami-intake"
      timeout: 5
```

---

## 13. Ansible structure

| File | Purpose |
|------|---------|
| `res/ansible/intake.yml` | Main playbook. Tags: `deploy`, `stop`, `restart`, `status`. |
| `res/ansible/templates/ami-intake.service.j2` | Renders the user systemd unit. |
| `res/ansible/templates/ami-intake-config.yml.j2` | Renders the daemon YAML config from `dataops_intake_config`. |

The playbook targets `hosts: localhost` by default. Multi-host deployments invoke it with `-l <host>` per machine; each host maintains its own `intake_root` and audit chain.

---

## 14. Python package layout

### 14.1 `ami/dataops/report/`

- `cli.py` — argparse dispatcher.
- `main.py` — entry point.
- `scanner.py` — walks candidate roots, runs pre-flight.
- `tui.py` — builds the SelectionDialog item tree and runs the three screens.
- `manifest.py` — canonical JSON, HMAC signing.
- `transport.py` — multipart POST via `httpx`, retry logic.
- `config.py` — inventory resolution.

### 14.2 `ami/dataops/intake/`

- `cli.py` — argparse dispatcher (`serve`, `status`, `ls`, `show`, `verify`, `rotate-audit`).
- `main.py` — entry point.
- `app.py` — FastAPI application factory; routes `/v1/bundles` and `/metrics`.
- `validation.py` — pure validation rules (§6), no framework coupling.
- `audit.py` — append + chain-hash + rotation (§7), no framework coupling.
- `quarantine.py` — staging -> quarantine atomic move.
- `config.py` — config loading from the Ansible-rendered YAML.

`validation.py` and `audit.py` are importable from AMI-AGENTS core or any other project without pulling FastAPI / uvicorn. This is the reuse hook in REQ §14.

---

## 15. Edge cases

| Case | Behaviour |
|------|-----------|
| Sender sends bundle while file rotates on disk | Local hash mismatch at pre-flight; exit 6 with per-file detail. |
| Receiver's audit.log is world-readable | Startup fails with a permissions error; no requests served until fixed. |
| bundle_id is reused by a different sender | Quarantine path includes sender_id, so no collision. Accepted normally. |
| Clock skew between sender and receiver | Not a validation criterion in v1. `sent_at` and `received_at` may differ freely. |
| Tar entry with a name longer than 100 bytes | POSIX ustar supports up to 100 bytes in the name field; longer names rejected as `path_unsafe`. |
| Manifest declares a file, tarball omits it | `hash_mismatch` after extraction (file missing -> no bytes -> hash diverges). |
| Tarball contains a file not in the manifest | `hash_mismatch` on the unexpected file (no manifest entry to compare against, treated as bundle corruption). |
| `--check` mode on the intake Ansible playbook | Renders the config and unit templates to stdout, reloads systemd as a dry run, never starts the daemon. |
| Rotating `audit.log` while a request is in flight | The in-flight handler holds a file descriptor; its record lands in the old file even after rotation renames it. The sealed record accounts for any records appended post-compute, so seal is taken at rotate time and any late writes would extend beyond the seal (treated as post-seal appendix). Rotation must be operator-scheduled during low-traffic windows. |
