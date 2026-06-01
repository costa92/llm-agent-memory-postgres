package postgres

import (
	"context"
	"fmt"
)

// HeadSchemaVersion is the latest schema version this code knows how to migrate to.
// Bumped to 3 in M8a for working-kind support and the dedupe index table.
const HeadSchemaVersion = 3

// SchemaVersion is retained as an alias for backwards compatibility with
// existing M5-M7 callers/tests. It now points at HeadSchemaVersion.
const SchemaVersion = HeadSchemaVersion

// AcceptableSkewVersions is the slack between a deployed code's HeadSchemaVersion
// and the DB's current version that is tolerated (logged, but not fatal). This
// lets rolling deploys land where a newer pod arrives at an older pod's database
// without forcing a strict-equal version check.
const AcceptableSkewVersions = 5

// migrationGroup is a single, atomically-applied bundle of DDL statements
// recorded under a specific schema version row.
type migrationGroup struct {
	Version       int
	Transactional bool
	Statements    []string
}

func (s *Store) Migrate(ctx context.Context) error {
	current, err := s.currentSchemaVersion(ctx)
	if err != nil {
		return err
	}
	if current > HeadSchemaVersion+AcceptableSkewVersions {
		return fmt.Errorf("%w: db=%d code=%d", ErrSchemaVersionAhead, current, HeadSchemaVersion)
	}
	// current > HeadSchemaVersion (but within AcceptableSkewVersions slack) is
	// tolerable — a newer-version peer applied a future group ahead of us. We
	// proceed without applying anything past `current`, and any groups <= current
	// are skipped by the loop below. Logging is intentionally left to the caller
	// (the gateway wires slog; the SDK has no logger contract).
	for _, group := range s.migrationGroups() {
		if group.Version <= current {
			continue
		}
		if group.Transactional {
			if err := s.runGroupInTx(ctx, group); err != nil {
				return err
			}
		} else {
			if err := s.runGroupDirect(ctx, group); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) migrationGroups() []migrationGroup {
	return []migrationGroup{
		{Version: 1, Transactional: true, Statements: s.v1Statements()},
		{Version: 2, Transactional: true, Statements: s.v2RelayLeaseStatements()},
		{Version: 3, Transactional: true, Statements: s.v3WorkingAndDedupeStatements()},
	}
}

// runGroupInTx executes a transactional migration group atomically: all
// statements + the version-row insert happen inside one pgx transaction,
// so any error rolls back the entire group.
func (s *Store) runGroupInTx(ctx context.Context, group migrationGroup) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("memory/postgres: migrate begin tx (v%d): %w", group.Version, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, stmt := range group.Statements {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("memory/postgres: migrate v%d: %w", group.Version, err)
		}
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(
			`INSERT INTO %s (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`,
			s.schemaVersionTable(),
		),
		group.Version,
	); err != nil {
		return fmt.Errorf("memory/postgres: migrate v%d record version: %w", group.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("memory/postgres: migrate v%d commit: %w", group.Version, err)
	}
	return nil
}

// runGroupDirect executes a non-transactional migration group by running each
// statement directly against the pool, then recording the version row as a
// final separate statement. Used for DDL that Postgres refuses to run inside
// a transaction (e.g., CREATE INDEX CONCURRENTLY).
func (s *Store) runGroupDirect(ctx context.Context, group migrationGroup) error {
	for _, stmt := range group.Statements {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("memory/postgres: migrate v%d (direct): %w", group.Version, err)
		}
	}
	if _, err := s.pool.Exec(ctx,
		fmt.Sprintf(
			`INSERT INTO %s (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`,
			s.schemaVersionTable(),
		),
		group.Version,
	); err != nil {
		return fmt.Errorf("memory/postgres: migrate v%d (direct) record version: %w", group.Version, err)
	}
	return nil
}

func (s *Store) currentSchemaVersion(ctx context.Context) (int, error) {
	var exists bool
	if err := s.pool.QueryRow(
		ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = $1
		)`,
		s.schemaVersionTable(),
	).Scan(&exists); err != nil {
		return 0, fmt.Errorf("memory/postgres: schema version table exists: %w", err)
	}
	if !exists {
		return 0, nil
	}

	var version int
	if err := s.pool.QueryRow(
		ctx,
		fmt.Sprintf(`SELECT COALESCE(MAX(version), 0) FROM %s`, s.schemaVersionTable()),
	).Scan(&version); err != nil {
		return 0, fmt.Errorf("memory/postgres: current schema version: %w", err)
	}
	return version, nil
}

// v1Statements returns the flat list of DDL statements that originally
// constituted SchemaVersion=1 (M5-M7). The schema_version table is created
// here so subsequent group recording can target it.
func (s *Store) v1Statements() []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			version BIGINT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`, s.schemaVersionTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			memory_id TEXT PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			user_id TEXT NOT NULL,
			project_id TEXT,
			session_id TEXT,
			kind TEXT NOT NULL,
			source TEXT NOT NULL,
			category TEXT NOT NULL,
			content TEXT NOT NULL,
			normalized_content_hash TEXT NOT NULL,
			tags JSONB NOT NULL DEFAULT '[]'::jsonb,
			importance DOUBLE PRECISION NOT NULL,
			pinned BOOLEAN NOT NULL DEFAULT FALSE,
			disabled BOOLEAN NOT NULL DEFAULT FALSE,
			deleted BOOLEAN NOT NULL DEFAULT FALSE,
			version BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			deleted_at TIMESTAMPTZ,
			last_access_at TIMESTAMPTZ,
			hit_count BIGINT NOT NULL DEFAULT 0,
			consolidated_from_event_id TEXT,
			UNIQUE (tenant_id, memory_id),
			CHECK (importance >= 0 AND importance <= 1),
			CHECK (kind IN ('episodic', 'semantic')),
			CHECK (source IN ('user_saved', 'agent_inferred', 'system'))
		)`, s.memoryRecordTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			event_id TEXT PRIMARY KEY,
			memory_id TEXT NOT NULL,
			tenant_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			version BIGINT NOT NULL,
			idempotency_key TEXT,
			payload JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (memory_id, version, event_type)
		)`, s.memoryEventTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			tenant_id TEXT NOT NULL,
			idempotency_key TEXT NOT NULL,
			request_hash TEXT NOT NULL,
			memory_id TEXT,
			response_snapshot JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (tenant_id, idempotency_key)
		)`, s.idempotencyTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			outbox_id TEXT PRIMARY KEY,
			aggregate_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			tenant_id TEXT NOT NULL,
			event_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			payload JSONB NOT NULL,
			status TEXT NOT NULL,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			sent_at TIMESTAMPTZ,
			last_error TEXT
		)`, s.outboxTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_scope_idx ON %s (tenant_id, user_id, project_id, deleted, disabled)`,
			s.memoryRecordTable(), s.memoryRecordTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_category_idx ON %s (tenant_id, user_id, category, deleted, disabled)`,
			s.memoryRecordTable(), s.memoryRecordTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_hash_idx ON %s (tenant_id, user_id, normalized_content_hash)`,
			s.memoryRecordTable(), s.memoryRecordTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_updated_at_idx ON %s (tenant_id, updated_at DESC)`,
			s.memoryRecordTable(), s.memoryRecordTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_memory_version_idx ON %s (memory_id, version)`,
			s.memoryEventTable(), s.memoryEventTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_tenant_created_idx ON %s (tenant_id, created_at DESC)`,
			s.memoryEventTable(), s.memoryEventTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_event_type_created_idx ON %s (event_type, created_at DESC)`,
			s.memoryEventTable(), s.memoryEventTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_status_created_idx ON %s (status, created_at)`,
			s.outboxTable(), s.outboxTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_aggregate_created_idx ON %s (aggregate_id, created_at)`,
			s.outboxTable(), s.outboxTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_event_id_idx ON %s (event_id)`,
			s.outboxTable(), s.outboxTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			trace_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			tenant_id  TEXT NOT NULL,
			request_id TEXT,
			stage      TEXT NOT NULL,
			reason     TEXT NOT NULL,
			memory_id  TEXT,
			version    BIGINT,
			emitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			emitter    TEXT NOT NULL,
			payload    JSONB
		)`, s.memoryDecisionTraceTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_tenant_time_idx ON %s (tenant_id, emitted_at DESC)`,
			s.memoryDecisionTraceTable(), s.memoryDecisionTraceTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_request_idx ON %s (request_id) WHERE request_id IS NOT NULL`,
			s.memoryDecisionTraceTable(), s.memoryDecisionTraceTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_stage_reason_idx ON %s (stage, reason)`,
			s.memoryDecisionTraceTable(), s.memoryDecisionTraceTable()),
		fmt.Sprintf(`COMMENT ON COLUMN %s.reason IS 'free-form in v1.x (M7); enum frozen in v2 (M8)'`,
			s.memoryDecisionTraceTable()),
	}
}

// v2RelayLeaseStatements is the schema group v2 — relay lease columns on the
// outbox table plus a partial index for the claim-expired-lease query.
// All ADD COLUMN clauses use IF NOT EXISTS (Postgres 9.6+) so a partial-failure
// re-run is idempotent without needing a DO $$ ... EXCEPTION wrapper.
func (s *Store) v2RelayLeaseStatements() []string {
	outbox := s.outboxTable()
	return []string{
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS claimed_by TEXT`, outbox),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ`, outbox),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ`, outbox),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_lease_idx
			ON %s (status, lease_expires_at)
			WHERE status = 'processing'`, outbox, outbox),
	}
}

// v3WorkingAndDedupeStatements is schema group v3 — adds 'working' to the kind
// check constraint on memory_record and creates the dedupe-winner registry table.
func (s *Store) v3WorkingAndDedupeStatements() []string {
	record := s.memoryRecordTable()
	dedupe := s.dedupeIndexTable()
	return []string{
		// Widen the kind CHECK to admit 'working'. The original constraint is
		// unnamed, so Postgres auto-generated (and 63-byte-truncated) its name;
		// a constructed "<table>_kind_check" literal will not match it. Discover
		// the real name from the catalog and drop whatever kind CHECK exists,
		// then re-add the widened constraint.
		fmt.Sprintf(`DO $$
DECLARE con text;
BEGIN
  FOR con IN
    SELECT conname FROM pg_constraint
    WHERE conrelid = '%s'::regclass AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%%kind%%'
  LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %%I', con);
  END LOOP;
  ALTER TABLE %s ADD CHECK (kind IN ('working', 'episodic', 'semantic'));
END $$;`, record, record, record),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			tenant_id TEXT NOT NULL,
			dedupe_key TEXT NOT NULL,
			winner_memory_id TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (tenant_id, dedupe_key)
		)`, dedupe),
	}
}
