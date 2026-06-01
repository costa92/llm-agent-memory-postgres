package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const liveEnvVar = "LLM_AGENT_MEMORY_PG_URL"

func openTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(liveEnvVar)
	if dsn == "" {
		t.Skipf("set %s to run live postgres tests", liveEnvVar)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMigrate_FirstRunCreatesTables(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_first", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	assertTableExists(t, ctx, pool, s.memoryRecordTable())
	assertTableExists(t, ctx, pool, s.memoryEventTable())
	assertTableExists(t, ctx, pool, s.idempotencyTable())
	assertTableExists(t, ctx, pool, s.outboxTable())
}

func TestMigrate_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_idem", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate first run: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate second run: %v", err)
	}
}

func TestMigrate_RejectsFutureSchemaVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_future", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (version) VALUES ($1)`, s.schemaVersionTable()),
		HeadSchemaVersion+AcceptableSkewVersions+1,
	); err != nil {
		t.Fatalf("insert future version: %v", err)
	}
	if err := s.Migrate(ctx); err == nil {
		t.Fatal("expected future schema version error")
	}
}

func TestMigrate_CreatesDecisionTraceTable(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m7_%d_trace", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	assertTableExists(t, ctx, pool, s.memoryDecisionTraceTable())
}

func TestMigrate_DecisionTraceIndexes(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m7_%d_idx", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	table := s.memoryDecisionTraceTable()
	for _, idx := range []string{
		table + "_tenant_time_idx",
		table + "_request_idx",
		table + "_stage_reason_idx",
	} {
		assertIndexExists(t, ctx, pool, table, idx)
	}
}

func TestMigrate_DecisionTraceIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m7_%d_idem", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate first run: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate second run: %v", err)
	}

	assertTableExists(t, ctx, pool, s.memoryDecisionTraceTable())
}

// TestMigrationGroup_TypeExists is a compile-time check that the migrationGroup type
// has the expected fields. If any field is missing or has the wrong type, this fails to compile.
func TestMigrationGroup_TypeExists(t *testing.T) {
	g := migrationGroup{
		Version:       1,
		Transactional: true,
		Statements:    []string{"SELECT 1"},
	}
	if g.Version != 1 || !g.Transactional || len(g.Statements) != 1 {
		t.Fatalf("migrationGroup fields not wired: %+v", g)
	}
}

func TestMigrationGroups_HeadVersionIsThree(t *testing.T) {
	if HeadSchemaVersion != 3 {
		t.Fatalf("HeadSchemaVersion = %d, want 3", HeadSchemaVersion)
	}
	s := &Store{}
	groups := s.migrationGroups()
	if len(groups) < 3 {
		t.Fatalf("migrationGroups() returned %d groups, want >= 3", len(groups))
	}
	if groups[0].Version != 1 {
		t.Fatalf("groups[0].Version = %d, want 1", groups[0].Version)
	}
	if groups[1].Version != 2 {
		t.Fatalf("groups[1].Version = %d, want 2", groups[1].Version)
	}
	if groups[2].Version != 3 {
		t.Fatalf("groups[2].Version = %d, want 3", groups[2].Version)
	}
	for i, g := range groups {
		if !g.Transactional {
			t.Fatalf("groups[%d].Transactional = false, expected true for v%d", i, g.Version)
		}
	}
}

func TestMigrate_RunsGroupsInOrder(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_order", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// After Migrate(), every group version must be recorded.
	rows, err := pool.Query(ctx,
		fmt.Sprintf(`SELECT version FROM %s ORDER BY version ASC`, s.schemaVersionTable()),
	)
	if err != nil {
		t.Fatalf("query schema_version: %v", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		versions = append(versions, v)
	}
	if len(versions) < 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("recorded versions = %v, want first two to be 1,2", versions)
	}
}

func TestMigrate_SkipsAlreadyAppliedGroups(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_skip", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	// Second invocation must be a no-op (no error, no duplicate version rows).
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE version = 1`, s.schemaVersionTable()),
	).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("schema_version row for v=1 count = %d, want 1", count)
	}
}

func TestMigrate_TransactionalGroupRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_rollback", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Inject a deliberately-bad statement into a fresh group to verify rollback.
	bad := migrationGroup{
		Version:       99,
		Transactional: true,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE %s_sentinel (id INT)`, prefix),
			`SELECT * FROM definitely_nonexistent_table_xyz`,
		},
	}
	if err := s.runGroupInTx(ctx, bad); err == nil {
		t.Fatal("expected error from bad migration group")
	}
	// Sentinel table must NOT exist — tx rolled back.
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1
		)`,
		prefix+"_sentinel",
	).Scan(&exists); err != nil {
		t.Fatalf("check sentinel: %v", err)
	}
	if exists {
		t.Fatalf("sentinel table exists; rollback failed")
	}
}

func TestAcceptableSkewVersions_Constant(t *testing.T) {
	if AcceptableSkewVersions != 5 {
		t.Fatalf("AcceptableSkewVersions = %d, want 5", AcceptableSkewVersions)
	}
}

func TestMigrate_NonTransactionalGroupRunsWithoutOuterTx(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_nontx", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Bootstrap v1 so the schema_version table exists.
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	group := migrationGroup{
		Version:       42,
		Transactional: false,
		Statements: []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_nontx (id INT)`, prefix),
		},
	}
	if err := s.runGroupDirect(ctx, group); err != nil {
		t.Fatalf("runGroupDirect: %v", err)
	}
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = $1
		)`,
		prefix+"_nontx",
	).Scan(&exists); err != nil {
		t.Fatalf("check table: %v", err)
	}
	if !exists {
		t.Fatalf("table not created by non-tx group")
	}
	var versionCount int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE version = 42`, s.schemaVersionTable()),
	).Scan(&versionCount); err != nil {
		t.Fatalf("count version: %v", err)
	}
	if versionCount != 1 {
		t.Fatalf("version 42 count = %d, want 1", versionCount)
	}
}

func TestMigrate_TolerablyAheadDB_WarnsButProceeds(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_skew_ok", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	// Insert a future version within tolerance.
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (version) VALUES ($1)`, s.schemaVersionTable()),
		HeadSchemaVersion+2,
	); err != nil {
		t.Fatalf("insert future: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("tolerable-skew Migrate: %v", err)
	}
}

func TestMigrate_TooFarAheadDB_ReturnsErrSchemaVersionAhead(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_skew_bad", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (version) VALUES ($1)`, s.schemaVersionTable()),
		HeadSchemaVersion+AcceptableSkewVersions+1,
	); err != nil {
		t.Fatalf("insert future: %v", err)
	}
	err = s.Migrate(ctx)
	if err == nil {
		t.Fatal("expected error for too-far-ahead schema version")
	}
	if !errors.Is(err, ErrSchemaVersionAhead) {
		t.Fatalf("err = %v, want ErrSchemaVersionAhead", err)
	}
}

func TestCurrentSchemaVersion_RunsOnFreshDB(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_fresh_csv", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	v, err := s.currentSchemaVersion(ctx)
	if err != nil {
		t.Fatalf("currentSchemaVersion on fresh db: %v", err)
	}
	if v != 0 {
		t.Fatalf("currentSchemaVersion = %d, want 0", v)
	}
}

func TestMigrate_FreshDB_AppliesAllGroups(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_fresh_all", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate on fresh db: %v", err)
	}

	// Each group's Version must be recorded.
	for _, group := range s.migrationGroups() {
		var count int
		if err := pool.QueryRow(ctx,
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE version = $1`, s.schemaVersionTable()),
			group.Version,
		).Scan(&count); err != nil {
			t.Fatalf("query version row for v%d: %v", group.Version, err)
		}
		if count != 1 {
			t.Fatalf("version=%d count = %d, want 1", group.Version, count)
		}
	}
}

func TestMigrate_RelayLeaseColumns_Added(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_lease_cols", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	wantCols := map[string]string{
		"claimed_by":       "text",
		"claimed_at":       "timestamp with time zone",
		"lease_expires_at": "timestamp with time zone",
	}
	for col, wantType := range wantCols {
		var dataType string
		var isNullable string
		err := pool.QueryRow(ctx,
			`SELECT data_type, is_nullable
			 FROM information_schema.columns
			 WHERE table_schema = current_schema()
			   AND table_name = $1
			   AND column_name = $2`,
			s.outboxTable(), col,
		).Scan(&dataType, &isNullable)
		if err != nil {
			t.Fatalf("column %q lookup: %v", col, err)
		}
		if dataType != wantType {
			t.Fatalf("column %q data_type = %q, want %q", col, dataType, wantType)
		}
		if isNullable != "YES" {
			t.Fatalf("column %q is_nullable = %q, want YES", col, isNullable)
		}
	}
}

func TestMigrate_RelayLeaseIndex_PartialOnProcessing(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_lease_idx", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	idxName := s.outboxTable() + "_lease_idx"
	var indexDef string
	if err := pool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes
		 WHERE schemaname = current_schema()
		   AND tablename = $1
		   AND indexname = $2`,
		s.outboxTable(), idxName,
	).Scan(&indexDef); err != nil {
		t.Fatalf("look up index %s: %v", idxName, err)
	}
	// The partial predicate must reference status='processing'. Postgres
	// canonicalizes this to "WHERE (status = 'processing'::text)".
	if !strings.Contains(indexDef, "processing") {
		t.Fatalf("indexdef = %q, want partial WHERE on status='processing'", indexDef)
	}
}

func assertIndexExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table, index string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND tablename = $1
			  AND indexname = $2
		)`,
		table, index,
	).Scan(&exists); err != nil {
		t.Fatalf("index exists query for %s on %s: %v", index, table, err)
	}
	if !exists {
		t.Fatalf("expected index %s on table %s to exist", index, table)
	}
}

func assertTableExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			  AND table_name = $1
		)`,
		table,
	).Scan(&exists); err != nil {
		t.Fatalf("table exists query for %s: %v", table, err)
	}
	if !exists {
		t.Fatalf("expected table %s to exist", table)
	}
}
