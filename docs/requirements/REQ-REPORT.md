# Requirements: P2P Log Reporting (`ami-report` sender + `ami-intake` daemon)

**Date:** 2026-04-19
**Status:** ACTIVE
**Type:** Requirements
**Spec:** [SPEC-REPORT](../specifications/SPEC-REPORT.md)

---

## Background

There is no supported way today for one AMI-AGENTS instance to ship audit-grade log files to another instance. Operators copy via `scp`, paste into chat, or describe symptoms in prose. This blocks cross-instance incident review, the 2026 chain-of-custody baseline (tamper-evident transfer with cryptographic integrity), and any future fleet-scale log consolidation.

This feature delivers two DATAOPS extensions, designed to pair but shipped and owned independently:

- **`ami-report`** — an interactive TUI that lets the operator multi-select `.log` / `logs/**` / acceptable text files, pick a destination AMI-AGENTS instance by FQDN or IP, sign a manifest with HMAC-SHA256, and POST the bundle over HTTPS to the destination's intake daemon. Mirrors the `ami-restore` UX: restore is a TUI picker over backups, report is a TUI picker over logs.
- **`ami-intake`** — a FastAPI daemon deployed via Ansible as a user-scoped systemd unit. Receives signed bundles, verifies HMAC, validates content (extension allowlist + null-byte probe + size caps — no execution, ever), quarantines accepted files under a per-sender audit tree, and appends an immutable receipt to a tamper-evident audit log. Exposed on a Cloudflare Tunnel FQDN via `ami-serve` for cross-internet peers, or reachable by IP:port on a private network.

Together they form the P2P reporting channel. Neither creates the other; each is registered as its own extension and can be installed independently.

---

## Core Requirements

### 1. Scope & Roles

- **R-REPORT-001**: `ami-report` and `ami-intake` shall be distinct DATAOPS extensions with separate manifest entries, CLIs, Python packages, and test trees. Installing one shall not require installing the other.
- **R-REPORT-002**: `ami-report` is a one-shot interactive sender (no persistent process); `ami-intake` is a long-running daemon. The two roles shall never be colocated in the same process.
- **R-REPORT-003**: `ami-intake` shall be an HTTP app sitting behind an operator-provided transport (typically `ami-serve` publishing it on a Cloudflare Tunnel FQDN, or direct binding on an RFC1918 IP). `ami-intake` shall not embed tunnel or TLS-termination logic of its own.
- **R-REPORT-004**: Cross-peer trust shall be per-sender: each `ami-intake` instance maintains an explicit allowlist of sender IDs with their per-sender bearer token and shared secret. No implicit trust between peers.
- **R-REPORT-005**: Received files shall never be executed, evaluated, imported, or passed to any interpreter under any circumstance. This is a non-negotiable invariant of the receiver.

### 2. Sender — File Selection

- **R-REPORT-010**: `ami-report send` (interactive) shall present a multi-select tree of candidate files using the existing `ami.cli_components.selection_dialog.SelectionDialog` primitive via `dialogs.multiselect()`. No new TUI library shall be introduced.
- **R-REPORT-011**: The tree shall render directories as group headers and individual files as toggleable children, so SelectionDialog's group-toggle action selects every file under a subdirectory in one keystroke.
- **R-REPORT-012**: Default candidate roots shall include the repository `logs/` directory under `AMI_ROOT`. Operator-declared extra roots shall be read from `dataops_report_sender_config.extra_roots` (list of absolute paths).
- **R-REPORT-013**: Nothing shall be preselected. The operator must opt into every file explicitly.
- **R-REPORT-014**: Files failing the sender's pre-flight content check (extension not in the allowlist in §9, or NUL byte in first 8 KiB, or size above `max_file_mb`) shall be rendered as disabled entries with a reason suffix; they are visible but not toggleable.
- **R-REPORT-015**: `ami-report send --ci --defaults FILE` shall replace the TUI with a non-interactive run driven by `FILE`, which lists selected paths plus the destination peer name. This mirrors the `ami-update --ci --defaults` convention.
- **R-REPORT-016**: Selection shall resolve symbolic links before pre-flight; a symlink pointing outside any declared candidate root shall be rejected with a clear error.
- **R-REPORT-017**: Empty selection shall abort the send before the destination step, with exit code 2.
- **R-REPORT-018**: In interactive mode, after the manifest + tarball are built and before the POST, the wizard shall render an archive-preview screen showing the compressed tar size, uncompressed aggregate size, and the list of files being shipped (up to 20 shown, with a `(+K more)` indicator for longer archives). The operator may abort (exit 0) or continue to the final confirmation.

### 3. Sender — Destination

- **R-REPORT-030**: Destinations shall be declared in inventory as `dataops_report_peers` entries with fields `name`, `endpoint`, and `shared_secret_env_var`. `endpoint` is either `https://<fqdn>` or `https://<ip>:<port>`.
- **R-REPORT-031**: After file selection, the TUI shall present a single-select list of configured peers via `dialogs.select()`. If only one peer is configured, selection shall auto-advance.
- **R-REPORT-032**: Plain HTTP shall be rejected by default. `--insecure` shall allow plain HTTP **only** when the destination resolves to a loopback or RFC1918 address; any other combination shall exit with an actionable error.
- **R-REPORT-033**: The shared secret shall be read from the environment variable named by `shared_secret_env_var`. A missing variable shall abort the send with exit code 2 before any network traffic.
- **R-REPORT-034**: A peer's bearer token shall be read from `AMI_REPORT_TOKENS__<PEER_NAME>` (uppercase, underscores). Missing token behaves the same as missing shared secret.

### 4. Sender — Manifest & Signing

- **R-REPORT-040**: Each send shall produce one JSON manifest with fields: `schema_version` (integer, v1 == 1), `sender_id` (string, from `dataops_report_sender_config.sender_id`), `sent_at` (RFC3339 UTC), `bundle_id` (UUIDv7 string, generated via the workspace-standard `uuid_utils.uuid7()`), `source_root` (absolute path the selection was made under), and `files` (array of `{relative_path, sha256, size_bytes, mtime}`).
- **R-REPORT-041**: Per-file `sha256` in the manifest shall be computed over the exact bytes of the source file on the sender host before any archiving.
- **R-REPORT-042**: `relative_path` entries shall be forward-slash-separated, never absolute, never containing `..`, and unique within the manifest.
- **R-REPORT-043**: The manifest shall be serialised to bytes using [RFC 8785 JSON Canonicalization Scheme (JCS)](https://www.rfc-editor.org/rfc/rfc8785), UTF-8 encoded, with exactly one trailing LF. Because the manifest schema forbids floats and non-BMP characters in v1, the RFC 8785 number-formatting edge cases do not apply; a conforming implementation shall still call a JCS serialiser rather than relying on `json.dumps` with ad-hoc options, so upgrading to v2 cannot silently break verification.
- **R-REPORT-044**: The signature header value shall be `HMAC-SHA256(shared_secret, canonical_manifest_bytes)`, hex-encoded, sent as `X-AMI-Signature: sha256=<hex>`.
- **R-REPORT-045**: The wire bundle shall be `multipart/form-data` with exactly two parts: `manifest` (the canonical JSON bytes, `Content-Type: application/json`) and `bundle` (a gzip-compressed tar of the selected files, `Content-Type: application/gzip`). No other parts shall be accepted.
- **R-REPORT-046**: Tarball compression shall be gzip. xz, zstd, and bzip2 are out of scope for v1 to keep the sender dependency surface minimal.
- **R-REPORT-047**: The sender shall include `X-AMI-Sender-Id: <sender_id>` and `X-AMI-Bundle-Id: <bundle_id>` headers; both values shall match the manifest's corresponding fields. A mismatch shall be detected and rejected by the receiver.

### 5. Sender — Transport

- **R-REPORT-060**: HTTP transport shall use `Authorization: Bearer <token>` for every request.
- **R-REPORT-061**: Connection timeout shall be 10 seconds; total request timeout shall be 300 seconds. Both limits are hard and not operator-tunable in v1.
- **R-REPORT-062**: Transient failures (connection reset, 502, 503, 504) shall trigger exponential backoff retry with three attempts (1s, 4s, 16s base, ±25% jitter). Non-transient failures (4xx other than 429) shall not be retried.
- **R-REPORT-063**: 429 Too Many Requests shall be honoured: the sender shall wait at least the value of the `Retry-After` header before retrying, within the total 300s budget.
- **R-REPORT-064**: Progress reporting shall emit one line per 5% of bundle bytes sent, or once per second, whichever is less frequent. The TUI shall consume this via the existing `TUI.draw_box` primitive for the progress screen.

### 6. Sender — CLI

- **R-REPORT-070**: `ami-report` shall expose these subcommands: `send` (default), `preview`, `peers`.
- **R-REPORT-071**: `ami-report preview` shall print the manifest that would be sent (files + sizes + hashes + destination) and exit without network activity. It shall never sign the manifest.
- **R-REPORT-072**: `ami-report peers` shall list configured peers with their endpoints and whether the required env vars are present (masked). Useful for CI smoke checks.
- **R-REPORT-073**: `ami-report send --dry-run` shall perform the full pre-flight and signing but skip the POST; the canonical manifest bytes and the signature shall be printed to stdout for audit.
- **R-REPORT-074**: `ami-report` shall be registered as a DATAOPS extension via `projects/AMI-DATAOPS/extension.manifest.yaml` with a `{python} {binary} --help` health check.
- **R-REPORT-075**: `ami-report` shall always resolve `AMI_ROOT` and operate from there, regardless of the caller's working directory.

### 7. Intake — Service Model

- **R-REPORT-100**: `ami-intake serve` shall run a FastAPI application under `uvicorn` (ASGI), bound by default to `127.0.0.1:<intake_port>` where `intake_port` is declared in `dataops_intake_config`. `uvicorn` shall be launched with `--limit-max-requests` and `--limit-concurrency` set from `dataops_intake_config`, and the request-body limit enforced at the ASGI layer shall match `max_bundle_mb` so oversized bodies are rejected before the FastAPI handler is invoked.
- **R-REPORT-101**: A user-scoped systemd unit shall be installed at `~/.config/systemd/user/ami-intake.service` with `Type=simple`, `Restart=always`, `RestartSec=5`, `WantedBy=default.target`.
- **R-REPORT-102**: The unit `ExecStart` shall invoke the bootstrapped Python with `ami-intake serve` (the argparse subcommand), passing the config path as `--config <path>`.
- **R-REPORT-103**: When `dataops_intake_config.persist: true`, the deploy shall run `loginctl enable-linger $(id -un)` once per host so the daemon survives logout.
- **R-REPORT-104**: System-scoped units (`/etc/systemd/system/`) are out of scope; user-scope is the only supported mode.
- **R-REPORT-105**: Public exposure shall be delegated to `ami-serve`: an operator binds a Cloudflare Tunnel ingress rule to `http://localhost:<intake_port>`. `ami-intake` shall not open a public socket itself.

### 8. Intake — Auth

- **R-REPORT-120**: Every request shall require a valid `Authorization: Bearer <token>` header. Tokens are looked up from the environment variable `AMI_INTAKE_TOKENS__<SENDER_ID>` (uppercase, underscores).
- **R-REPORT-121**: A missing, empty, or mismatched bearer token shall return HTTP 401 with a body that does not disclose whether the sender_id is known to the receiver (avoid sender enumeration).
- **R-REPORT-122**: After bearer verification, the receiver shall recompute the HMAC signature of the manifest using the per-sender shared secret (env `AMI_INTAKE_SECRETS__<SENDER_ID>`) and compare it in constant time to `X-AMI-Signature`. Mismatch shall return HTTP 401.
- **R-REPORT-123**: Bearer + HMAC are required together; verifying only one is insufficient. This provides defence in depth: a stolen bearer alone cannot forge a bundle, and a stolen secret alone cannot impersonate at the HTTP layer.
- **R-REPORT-124**: `X-AMI-Sender-Id` in the HTTP header shall match `sender_id` in the signed manifest. Any mismatch shall return HTTP 401.

### 9. Intake — Content Validation

- **R-REPORT-140**: The bundle tarball shall be extracted into a staging directory (tmpfs if available) using Python's [PEP 706](https://peps.python.org/pep-0706/) `tarfile` `filter="data"` extraction filter. Nothing shall be moved to the quarantine tree until every validation rule below has passed for every file.
- **R-REPORT-141**: Extension allowlist: `.log`, `.txt`. The feature is log reporting; the allowlist matches. Any file whose final extension is not on the list shall reject the entire bundle.
- **R-REPORT-142**: Path safety: no entry may contain `..`, be absolute, or resolve (after extraction) to a path outside the staging root. The `filter="data"` tarfile extraction filter shall be relied on for this check; any tar member that would violate it rejects the entire bundle. Symlinks, hardlinks, device nodes, FIFOs, and setuid/setgid bits in the tar stream are rejected outright.
- **R-REPORT-143**: Text-only probe: the first 8192 bytes of each file shall be scanned; a NUL (`\x00`) byte shall reject the entire bundle.
- **R-REPORT-144**: Per-file size cap: each file's size on disk after extraction shall be ≤ `max_file_mb` (default 1 MiB — sized for text logs, not data dumps). Enforced during streaming extraction so a zip-bomb is detected before the whole file is written.
- **R-REPORT-145**: Aggregate bundle cap: the sum of all extracted file sizes shall be ≤ `max_bundle_mb` (default 500 MiB). Same streaming-enforcement rule.
- **R-REPORT-146**: File count cap: a bundle shall contain ≤ `max_files_per_bundle` (default 1000) entries. Exceeding this rejects the bundle before extraction proceeds.
- **R-REPORT-147**: Hash verification: after extraction, the receiver shall recompute SHA256 over each file and compare to the per-file hash in the manifest. Mismatch rejects the entire bundle.
- **R-REPORT-148**: Atomic acceptance: any failure in §9 rejects the bundle in its entirety. Partial acceptance is not supported because the signed manifest is an atomic claim.
- **R-REPORT-149**: Validators shall be implemented as pure functions with no filesystem side effects beyond reading the input path, so they are trivially unit-testable and reusable by other projects (§14).
- **R-REPORT-150**: The receive handler shall consume the request body via the ASGI stream interface (`Request.stream()` or an equivalent chunked reader), not via the default FastAPI `UploadFile` spool. This is load-bearing: the default `SpooledTemporaryFile` threshold would cause every bundle to hit disk unconditionally, and the size caps could be violated before they are checked. The streaming reader shall enforce `max_bundle_mb` with a running byte counter that aborts the read as soon as the limit is exceeded.

### 10. Intake — Quarantine Layout

- **R-REPORT-170**: Accepted bundles shall be moved from the staging directory to `<intake_root>/<sender_id>/<YYYY>/<MM>/<DD>/<bundle_id>/`. The move shall be atomic within one filesystem (rename); cross-filesystem stages shall use copy-then-fsync-then-unlink.
- **R-REPORT-171**: `intake_root` defaults to `${AMI_ROOT}/logs/intake` and may be overridden via `dataops_intake_config.intake_root`.
- **R-REPORT-172**: Each quarantine directory shall contain: the unpacked files under their declared relative paths, `manifest.json` (the original signed manifest bytes), and `receipt.json` (the receiver-computed hashes and timestamps).
- **R-REPORT-173**: File permissions after move shall be `0640` for data files and `0440` for `manifest.json` and `receipt.json`. Directory mode shall be `0750`.
- **R-REPORT-174**: A duplicate `bundle_id` for the same `sender_id` shall be accepted idempotently: the receiver shall return the original receipt with HTTP 200 instead of 202, without rewriting any files.

### 11. Intake — Audit Log

- **R-REPORT-180**: Every receive attempt, accepted or rejected, shall append exactly one NDJSON record to `<intake_root>/audit.log`.
- **R-REPORT-181**: Each audit record shall include: `ts` (RFC3339 UTC), `event` (`accept` or `reject`), `sender_id`, `bundle_id`, `remote_addr`, `byte_count`, `file_count`, `reject_reason` (null on accept, enum value on reject), `receipt_sha256` (SHA256 over the canonical manifest bytes plus the tarball bytes, joined by a null separator), and `prev_hash` (SHA256 of the previous record's bytes, for the tamper-evident chain).
- **R-REPORT-182**: The first record of a newly created `audit.log` shall carry `prev_hash` equal to the sealed hash of the previous (rotated) log, or 64 zero hex chars if this is the very first log.
- **R-REPORT-183**: `audit.log` shall be opened in append-only mode (`O_APPEND`). The running file shall be `chmod 0640`; rotated sealed files shall be `chmod 0440`.
- **R-REPORT-184**: `ami-intake rotate-audit` shall append a terminal `seal` record containing `seal_hash` (SHA256 over the full sealed file up to but not including the seal record), close the file, chmod it to `0440`, move it to `<intake_root>/audit/<YYYY-MM-DDThhmmssZ>.log`, and open a fresh `audit.log` whose first record's `prev_hash` is the sealed file's `seal_hash`.
- **R-REPORT-185**: Audit records shall be flushed and fsynced before the HTTP response is returned. A power-loss scenario may lose the response but shall never lose the audit record.

### 12. Intake — CLI

- **R-REPORT-200**: `ami-intake` shall expose these subcommands: `serve`, `status`, `ls`, `show`, `verify`, `rotate-audit`.
- **R-REPORT-201**: `ami-intake status` shall report systemd unit state, uptime, bind address, config path, current `audit.log` size, last audit record timestamp, and the last 5 outcomes.
- **R-REPORT-202**: `ami-intake ls [--sender ID] [--since DATE] [--until DATE] [--status accept|reject]` shall list bundles matching the filter in reverse chronological order, one per line.
- **R-REPORT-203**: `ami-intake show <bundle_id>` shall print the manifest and receipt for a specific bundle.
- **R-REPORT-204**: `ami-intake verify <bundle_id>` shall recompute SHA256 for every file in the quarantine and compare to the receipt. Any mismatch exits non-zero with a list of the mismatched files.
- **R-REPORT-205**: `ami-intake rotate-audit` shall execute the rotation procedure defined in R-REPORT-184.
- **R-REPORT-206**: `ami-intake` shall be registered as a DATAOPS extension via the manifest, with a `{python} {binary} --help` health check.

### 13. Intake — Observability

- **R-REPORT-220**: The daemon shall expose Prometheus metrics at `/metrics`:
  - `ami_intake_bundles_received_total{sender_id,outcome}` (counter)
  - `ami_intake_bytes_received_total{sender_id}` (counter)
  - `ami_intake_rejected_total{reason}` (counter, labelled by reject-reason enum)
  - `ami_intake_audit_log_bytes` (gauge, size of current `audit.log`)
  - `ami_intake_last_accept_timestamp{sender_id}` (gauge, unix seconds)
- **R-REPORT-221**: Application logs shall be structured NDJSON on stderr, one record per request. Systemd captures these into `journalctl --user -u ami-intake.service`.
- **R-REPORT-222**: `/metrics` shall be reachable on the same bind address as the upload endpoint. Operators wanting to isolate metrics shall use an `ami-serve` ingress rule to split `/metrics` onto a separate hostname.

### 14. Generic Upload Extension Hook

- **R-REPORT-240**: Validation shall live in `ami/dataops/intake/validation.py` as pure, FastAPI-free callables. The module shall be importable from AMI-AGENTS core or any other project without pulling FastAPI, uvicorn, or DATAOPS-specific routing.
- **R-REPORT-241**: Audit-log append and chain-hash computation shall live in `ami/dataops/intake/audit.py` as plain callables with no framework dependency.
- **R-REPORT-242**: A reusable callable `accept_bundle(manifest_bytes, tarball_bytes, *, config) -> AcceptanceResult` shall encapsulate the full validation + quarantine + audit pipeline. Any project can call it from its own HTTP handler or message-queue consumer.
- **R-REPORT-243**: No side-effecting path inside the validation module shall require a running event loop. This keeps the hook usable from synchronous callers.

### 15. Ansible Implementation

- **R-REPORT-260**: A playbook `projects/AMI-DATAOPS/res/ansible/intake.yml` shall handle the lifecycle via tags `deploy`, `stop`, `restart`, `status`. Mirrors the `serve.yml` tag layout.
- **R-REPORT-261**: The playbook shall be idempotent: re-running `deploy` with no config changes shall produce no systemd or file-write activity.
- **R-REPORT-262**: Templates shall live at `projects/AMI-DATAOPS/res/ansible/templates/ami-intake.service.j2` and `projects/AMI-DATAOPS/res/ansible/templates/ami-intake-config.yml.j2`.
- **R-REPORT-263**: Check mode (`--check`) shall be supported for dry-run verification.

### 16. Exit Codes

- **R-REPORT-280**: `ami-report` exit **0** on successful send (HTTP 200 or 202 from receiver), on `preview`, on `peers`, or on interactive cancellation.
- **R-REPORT-281**: `ami-report` exit **2** on invalid arguments, missing config, empty selection, or missing env var.
- **R-REPORT-282**: `ami-report` exit **3** on network failure after retries.
- **R-REPORT-283**: `ami-report` exit **4** on HTTP 401 from receiver (auth reject).
- **R-REPORT-284**: `ami-report` exit **5** on HTTP 400, 413, or 429-after-max-wait from receiver (validation or quota reject).
- **R-REPORT-285**: `ami-report` exit **6** on local pre-flight validation failure (file disappeared, hash mismatch against disk, etc).
- **R-REPORT-286**: `ami-report` exit **10** on unexpected internal error.
- **R-REPORT-287**: `ami-intake serve` exit non-zero on config parse failure, bind failure, or audit-log permission failure; the systemd unit shall surface the exit code and restart per `Restart=always`.
- **R-REPORT-288**: `ami-intake verify` exit non-zero when any file fails hash comparison; exit 0 when all files match.

---

## Constraints

- Python 3.11+ (DATAOPS is pinned).
- New runtime dependencies to be added to `projects/AMI-DATAOPS/pyproject.toml`: `fastapi`, `uvicorn[standard]`, `prometheus-client`, `python-rfc8785` (JCS serialiser). `httpx` is already present. `uuid_utils` (for UUIDv7 `bundle_id`) is either already a workspace dependency or shall be promoted to one before implementation.
- HMAC-SHA256 and SHA256 shall come from `hashlib` / `hmac`; constant-time comparison shall use `hmac.compare_digest`. No new crypto libraries.
- No execution of received content under any circumstance; no code path shall pass received bytes to `exec`, `eval`, `subprocess.run`, `importlib`, or any interpreter.
- Tar extraction uses `tarfile` in streaming mode (`mode="r|gz"`) with `filter="data"` (PEP 706); no pax extensions honoured, no `@LongLink`, no support for absolute paths or symlinks.
- User-scoped systemd only for the daemon (no root required).
- Reuses the existing `ami.cli_components.selection_dialog.SelectionDialog` primitive and `ami.cli_components.dialogs` facade; no new TUI library.
- UUID generation follows the workspace rule: UUIDv7 only (via `uuid_utils.uuid7()`). No `uuid.uuid4` / `uuid1` / etc.

## Non-Requirements

- **Realtime log streaming.** Bundles are discrete snapshots. Streaming ingestion is a different feature.
- **File deletion on the sender after send.** The sender retains ownership of its logs.
- **At-rest encryption on the intake side.** Filesystem responsibility; operator may place `intake_root` on an encrypted volume.
- **Multi-hop relaying.** A→B→C is out of scope; each send is a direct peer-to-peer exchange.
- **Delta or incremental uploads.** v1 is whole-file; deduplication across bundles is not supported.
- **Retention or garbage collection of old bundles.** Operator handles expiry via cron or systemd timer.
- **Tunnel creation.** Operators expose the intake daemon via `ami-serve`; this feature does not touch tunnel lifecycle.
- **Policy engine.** Who-can-send-what is encoded in the per-sender allowlist; richer policy (per-file-pattern, time windows) is out of scope for v1.
- **Rate limiting per sender beyond the global 4-concurrency semaphore.** More granular quotas are a v2 concern.
