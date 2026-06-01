# llm-agent-memory-postgres

Postgres durable-storage backend for `github.com/costa92/llm-agent-memory`.

## Scope

This module owns the first concrete durable backend for the SDK-owned
pluggable memory data abstractions.

Owns:

- Postgres schema and migration runner
- durable record persistence
- OCC mutation paths
- idempotency persistence
- polling transactional outbox relay
- `cmd/memory-migrate`

Does not own:

- SDK-core memory abstractions
- HTTP gateway logic
- auth / request binding

## Package Layout

- `postgres/`
  - `Store` constructor and migration runner
  - record write / mutate / read paths
  - idempotency replay and conflict handling
  - polling outbox relay
- `cmd/memory-migrate/`
  - thin migration command

## SDK Relationship

This module depends on `github.com/costa92/llm-agent-memory/memory` and
implements the SDK-owned storage-layer abstractions directly rather than
redefining its own durable-memory contracts.

The durable models used here are the SDK-owned backend-neutral types
themselves, not local compatibility aliases.

## Current Capabilities

Implemented today:

- `(*Store).Migrate(ctx)`
- `(*Store).WriteRecord(ctx, in)`
- `(*Store).PatchRecord(ctx, in)`
- `(*Store).DeleteRecord(ctx, in)`
- `(*Store).PinRecord(ctx, in)`
- `(*Store).DisableRecord(ctx, in)`
- `(*Store).GetRecord(ctx, tenantID, memoryID)`
- `NewRelay(...).RunOnce(ctx)`

## Minimal Usage

```go
pool, err := pgxpool.New(ctx, os.Getenv("LLM_AGENT_MEMORY_PG_URL"))
if err != nil {
	panic(err)
}

store, err := postgres.New(pool, postgres.Config{})
if err != nil {
	panic(err)
}

if err := store.Migrate(ctx); err != nil {
	panic(err)
}
```

Migration command:

```bash
LLM_AGENT_MEMORY_PG_URL=postgres://... GOWORK=off go run ./cmd/memory-migrate
```

## Testing

Default tests do not require a live Postgres instance:

```bash
GOWORK=off go test ./... -count=1
```

Live Postgres tests are env-gated:

```bash
LLM_AGENT_MEMORY_PG_URL=postgres://... GOWORK=off go test ./postgres -count=1
```

When `LLM_AGENT_MEMORY_PG_URL` is unset, the live integration tests skip by
design.

## Deferred

Still deferred to later gateway/worker milestones:

- HTTP API surface
- auth and tenant binding from request context
- real MQ backends
- vector index integration
- cache invalidation workers
- decision-trace persistence and validation metrics
