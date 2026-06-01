# Changelog

All notable changes to `github.com/costa92/llm-agent-memory-postgres` will be
documented in this file.

<!-- Keep a Changelog format: https://keepachangelog.com/en/1.1.0/ -->
<!-- Semver: https://semver.org/ -->

## [Unreleased]

### Added

- M8a-prep relay hardening + migration framework v2:
  - **Migration framework v2.** `migrationGroup` type with per-group
    `Transactional bool` control, group-based runner that records each
    version atomically, and `AcceptableSkewVersions = 5` slack so a
    rolling deploy where a newer pod arrives at an older pod's database
    no longer forces a strict-equal version check (`ErrSchemaVersionAhead`
    fires only beyond the slack).
  - **Schema group v2.** Three new nullable outbox columns (`claimed_by`,
    `claimed_at`, `lease_expires_at`) + partial index
    `<outbox>_lease_idx (status, lease_expires_at) WHERE status='processing'`.
    All `ADD COLUMN IF NOT EXISTS` for partial-failure idempotency.
  - **Relay rewrite.** `ClaimBatch` claims pending or expired-lease rows
    `FOR UPDATE SKIP LOCKED` and bumps `attempt_count`. `Ack` carries an
    ownership predicate (`claimed_by = $workerID AND lease_expires_at >
    NOW()`) so a worker whose lease was stolen cannot mutate the row.
    Zero-rows-affected returns the new `ErrLeaseLost` sentinel. `RunOnce`
    collects ack errors via `errors.Join` instead of bailing early, so a
    single hot row can't stall progress on the rest of the batch.
    `Release(ctx)` flips owned-only leases back to `pending` for graceful
    shutdown (does NOT decrement `attempt_count` — the claim still counts
    toward the retry budget).
  - **RunStats.LeaseLost** field surfaces per-tick lease-loss counts to
    callers; metrics can wire this to alert on stuck publishes.
  - **Worker identity.** `NewRandomWorkerID()` returns
    `<hostname>-<32-hex-char>` (128 bits of `crypto/rand`); on
    `os.Hostname()` failure substitutes the literal `unknown`. Workers
    regenerate this on every process start so a crashed pod's lease can
    be reclaimed by lease-time rather than by identity.
  - **Write-side event-type allowlist.** `validateEventType` + new
    `ErrInvalidEventType` sentinel reject typos at `AppendEvent`,
    `EnqueueOutbox`, and `mutateRecord`. Two new event types added to
    the allowlist for M8a: `memory_promoted`, `memory_dedupe_collapsed`.
  - **`outboxStatusFailed`** constant; rows whose `attempt_count` reaches
    `MaxAttempts` transition here permanently until an operator
    `RequeueFailed`s them.
  - **Operator API.** `Store.ListFailed(ctx, limit)` returns the
    newest-first window of failed rows (id, aggregate_id, event_type,
    attempt_count, last_error, created_at). `Store.RequeueFailed(ctx,
    outboxID)` flips a `failed` row back to `pending` and resets
    `attempt_count` to 0; no-op (RowsAffected=0) on non-`failed` rows.
  - **`LeaseAwarePublisher`** test fake with injectable `PublishHook` for
    delay/error scenarios (notably the "publish exceeded lease TTL"
    case).
- `memory_decision_trace` table and three supporting indexes (tenant+time,
  request, stage+reason) appended to the existing migration sequence for the
  M7 validation-telemetry + decision-trace work. `reason` column is free-form
  in v1.x and will be frozen to an enum in v2 (M8).

### Changed

- `HeadSchemaVersion` bumped to **2**; `SchemaVersion` retained as an
  alias for backwards compatibility with M5-M7 callers.
- Postgres 11+ is now the minimum supported version (M8a-prep relies on
  nullable `ADD COLUMN` being metadata-only).

## [0.1.0] - 2026-05-26

### Added

- Initial Postgres durable-storage backend split out from the SDK module.
- `postgres.Store` with:
  - schema migration
  - idempotent `WriteRecord`
  - OCC mutation paths: `PatchRecord`, `DeleteRecord`, `PinRecord`, `DisableRecord`
  - tenant-bound `GetRecord`
- polling outbox relay with pluggable publisher interface
- `cmd/memory-migrate`

### Dependencies

- `github.com/costa92/llm-agent-memory` for SDK-owned durable abstractions
- `github.com/jackc/pgx/v5` for Postgres connectivity

### Notes

- Live Postgres tests are env-gated behind `LLM_AGENT_MEMORY_PG_URL`.
- Gateway HTTP and service composition are intentionally not part of this
  module.
