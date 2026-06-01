package postgres

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestEventTypeConstants_PromotedAndDedupeCollapsed(t *testing.T) {
	// Compile-time sanity check: both new constants must exist with the
	// canonical spec strings.
	if eventTypeMemoryPromoted != "memory_promoted" {
		t.Fatalf("eventTypeMemoryPromoted = %q, want memory_promoted", eventTypeMemoryPromoted)
	}
	if eventTypeMemoryDedupeCollapsed != "memory_dedupe_collapsed" {
		t.Fatalf("eventTypeMemoryDedupeCollapsed = %q, want memory_dedupe_collapsed", eventTypeMemoryDedupeCollapsed)
	}
}

func TestValidateEventType_AcceptsKnownTypes(t *testing.T) {
	for _, et := range []string{
		eventTypeMemoryCreated,
		eventTypeMemoryUpdated,
		eventTypeMemoryDeleted,
		eventTypeMemoryPinned,
		eventTypeMemoryUnpinned,
		eventTypeMemoryDisabled,
		eventTypeMemoryEnabled,
		eventTypeMemoryPromoted,
		eventTypeMemoryDedupeCollapsed,
	} {
		if err := validateEventType(et); err != nil {
			t.Errorf("validateEventType(%q) = %v, want nil", et, err)
		}
	}
}

func TestValidateEventType_RejectsTypo(t *testing.T) {
	err := validateEventType("memry_created") // deliberate typo
	if err == nil {
		t.Fatal("expected error for typo")
	}
	if !errors.Is(err, ErrInvalidEventType) {
		t.Fatalf("err = %v, want ErrInvalidEventType", err)
	}
}

func TestValidateEventType_RejectsEmpty(t *testing.T) {
	err := validateEventType("")
	if err == nil {
		t.Fatal("expected error for empty event type")
	}
	if !errors.Is(err, ErrInvalidEventType) {
		t.Fatalf("err = %v, want ErrInvalidEventType", err)
	}
}

func TestAppendEvent_RejectsInvalidEventType(t *testing.T) {
	// No DB required: validation happens before any pool call.
	s := &Store{}
	err := s.AppendEvent(nil, corememory.StoredEvent{ //nolint:staticcheck
		MemoryID:  "mem_1",
		TenantID:  "tenant_1",
		EventType: "memry_created", // deliberate typo
		Version:   1,
	})
	if err == nil {
		t.Fatal("expected error for invalid event type")
	}
	if !errors.Is(err, ErrInvalidEventType) {
		t.Fatalf("err = %v, want ErrInvalidEventType", err)
	}
}

func TestEnqueueOutbox_RejectsInvalidEventType(t *testing.T) {
	s := &Store{}
	err := s.EnqueueOutbox(nil, corememory.OutboxMessage{ //nolint:staticcheck
		MemoryID:  "mem_1",
		TenantID:  "tenant_1",
		EventID:   "evt_1",
		EventType: "memry_created",
		Version:   1,
	})
	if err == nil {
		t.Fatal("expected error for invalid event type")
	}
	if !errors.Is(err, ErrInvalidEventType) {
		t.Fatalf("err = %v, want ErrInvalidEventType", err)
	}
}

func TestMutateRecord_RejectsInvalidEventType(t *testing.T) {
	s := &Store{}
	_, err := s.mutateRecord(nil, mutationInput{ //nolint:staticcheck
		tenantID:        "tenant_1",
		memoryID:        "mem_1",
		expectedVersion: 1,
		eventType:       "memry_updated", // deliberate typo
		apply:           func(*corememory.MemoryRecord, time.Time) {},
	})
	if err == nil {
		t.Fatal("expected error for invalid event type")
	}
	if !errors.Is(err, ErrInvalidEventType) {
		t.Fatalf("err = %v, want ErrInvalidEventType", err)
	}
}

func TestPostgresSurface_Compiles(t *testing.T) {
	var (
		_ error = ErrVersionConflict
		_ error = ErrIdempotencyConflict
		_ error = ErrNotFound
		_ error = ErrSchemaVersionAhead
		_ error = ErrInvalidEventType
	)

	_, _ = New((*pgxpool.Pool)(nil), Config{})

	var (
		_ corememory.RecordStore      = (*Store)(nil)
		_ corememory.EventStore       = (*Store)(nil)
		_ corememory.IdempotencyStore = (*Store)(nil)
		_ corememory.Outbox           = (*Store)(nil)
	)
}

func TestNewRejectsBadInputs(t *testing.T) {
	if _, err := New(nil, Config{}); err == nil {
		t.Fatal("expected error for nil pool")
	}
	if _, err := New(&pgxpool.Pool{}, Config{TablePrefix: "bad-prefix"}); err == nil {
		t.Fatal("expected error for invalid table prefix")
	}
}

func TestPostgresPackage_DoesNotDefineSDKTypeAliases(t *testing.T) {
	t.Helper()

	path := filepath.Join(".", "models.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	file, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok || typeSpec.Assign == 0 {
				continue
			}
			t.Fatalf("compatibility alias forbidden in %s: %s", path, typeSpec.Name.Name)
		}
	}
}

func TestPostgresJSON_MemoryRecordRoundTrip(t *testing.T) {
	now := time.Unix(1712345678, 0).UTC()
	deletedAt := now.Add(5 * time.Minute)
	lastAccessAt := now.Add(10 * time.Minute)

	want := corememory.MemoryRecord{
		MemoryID:                "mem_123",
		TenantID:                "tenant_123",
		UserID:                  "user_123",
		ProjectID:               "proj_123",
		SessionID:               "sess_123",
		Kind:                    "episodic",
		Source:                  "user_saved",
		Category:                "fact",
		Content:                 "hello",
		NormalizedContentHash:   "hash_123",
		Tags:                    []string{"alpha", "beta"},
		Importance:              0.8,
		Pinned:                  true,
		Disabled:                false,
		Deleted:                 true,
		Version:                 7,
		CreatedAt:               now,
		UpdatedAt:               now,
		DeletedAt:               &deletedAt,
		LastAccessAt:            &lastAccessAt,
		HitCount:                9,
		ConsolidatedFromEventID: "evt_ancestor",
	}

	got, err := roundTripJSON(t, want)
	if err != nil {
		t.Fatalf("roundTripJSON() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestPostgresJSON_EventOutboxAndIdempotencyRoundTrip(t *testing.T) {
	now := time.Unix(1712349999, 0).UTC()

	record := corememory.MemoryRecord{
		MemoryID:              "mem_456",
		TenantID:              "tenant_456",
		UserID:                "user_456",
		Kind:                  "semantic",
		Source:                "agent_inferred",
		Category:              "summary",
		Content:               "content",
		NormalizedContentHash: "hash_456",
		Tags:                  []string{"tag"},
		Importance:            0.5,
		Version:               11,
		CreatedAt:             now,
		UpdatedAt:             now,
	}

	eventWant := corememory.StoredEvent{
		Version:        11,
		TenantID:       "tenant_456",
		MemoryID:       "mem_456",
		EventType:      "memory.record_written",
		IdempotencyKey: "idem_456",
		Record:         record,
	}
	eventGot, err := roundTripJSON(t, eventWant)
	if err != nil {
		t.Fatalf("roundTripJSON(event) error = %v", err)
	}
	if !reflect.DeepEqual(eventGot, eventWant) {
		t.Fatalf("event round trip mismatch\n got: %#v\nwant: %#v", eventGot, eventWant)
	}

	outboxWant := corememory.OutboxMessage{
		Version:   11,
		TenantID:  "tenant_456",
		MemoryID:  "mem_456",
		EventType: "memory.record_written",
		EventID:   "evt_456",
		Record:    record,
	}
	outboxGot, err := roundTripJSON(t, outboxWant)
	if err != nil {
		t.Fatalf("roundTripJSON(outbox) error = %v", err)
	}
	if !reflect.DeepEqual(outboxGot, outboxWant) {
		t.Fatalf("outbox round trip mismatch\n got: %#v\nwant: %#v", outboxGot, outboxWant)
	}

	idempotencyWant := corememory.IdempotencyEntry{
		TenantID:       "tenant_456",
		IdempotencyKey: "idem_456",
		RequestHash:    "req_hash_456",
		MemoryID:       "mem_456",
		Response: corememory.WriteRecordResult{
			MemoryID: "mem_456",
			Version:  11,
			Created:  true,
			Record:   record,
		},
		CreatedAt: now,
		ExpiresAt: ptrTime(now.Add(time.Hour)),
	}
	idempotencyGot, err := roundTripJSON(t, idempotencyWant)
	if err != nil {
		t.Fatalf("roundTripJSON(idempotency) error = %v", err)
	}
	if !reflect.DeepEqual(idempotencyGot, idempotencyWant) {
		t.Fatalf("idempotency round trip mismatch\n got: %#v\nwant: %#v", idempotencyGot, idempotencyWant)
	}
}

func roundTripJSON[T any](t *testing.T, want T) (T, error) {
	t.Helper()

	raw, err := encodeJSON(want)
	if err != nil {
		var zero T
		return zero, err
	}

	var got T
	if err := decodeJSON(raw, &got); err != nil {
		var zero T
		return zero, err
	}
	return got, nil
}

func ptrTime(v time.Time) *time.Time {
	return &v
}

// markOutboxFailed flips the outbox row that corresponds to the given
// memoryID into a 'failed' state with attempt_count=maxAttempts and an
// explicit last_error. Used by RequeueFailed / ListFailed tests to stage a
// terminal row without driving the full relay state machine.
func markOutboxFailed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, outboxTable, memoryID, lastErr string, attempt int) string {
	t.Helper()
	var outboxID string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT outbox_id FROM %s WHERE aggregate_id = $1`, outboxTable),
		memoryID,
	).Scan(&outboxID); err != nil {
		t.Fatalf("lookup outbox_id: %v", err)
	}
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
			SET status = $1, attempt_count = $2, last_error = $3,
			    claimed_by = NULL, claimed_at = NULL, lease_expires_at = NULL
			WHERE outbox_id = $4`, outboxTable),
		outboxStatusFailed, attempt, lastErr, outboxID,
	); err != nil {
		t.Fatalf("mark outbox failed: %v", err)
	}
	return outboxID
}

func TestRequeueFailed_ResetsToPendingAndZeroAttempts(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_requeue_ok", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	res, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_requeue_ok",
		RequestHash:    "hash_requeue_ok",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "requeue ok",
			NormalizedContentHash: "hash-requeue-ok",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	outboxID := markOutboxFailed(t, ctx, pool, s.outboxTable(), res.MemoryID, "terminal failure", 5)

	rqRes, err := s.RequeueFailed(ctx, outboxID)
	if err != nil {
		t.Fatalf("RequeueFailed: %v", err)
	}
	if rqRes.RowsAffected != 1 {
		t.Fatalf("RowsAffected = %d, want 1", rqRes.RowsAffected)
	}

	var status string
	var attempt int
	var lastError *string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, attempt_count, last_error FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &attempt, &lastError); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != outboxStatusPending {
		t.Fatalf("status = %s, want pending", status)
	}
	if attempt != 0 {
		t.Fatalf("attempt_count = %d, want 0", attempt)
	}
	if lastError != nil {
		t.Fatalf("last_error = %v, want nil", *lastError)
	}
}

func TestRequeueFailed_NoOpOnNonFailedRow(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_requeue_noop", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	res, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_requeue_noop",
		RequestHash:    "hash_requeue_noop",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "requeue noop",
			NormalizedContentHash: "hash-requeue-noop",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	// Row is still pending — RequeueFailed must be a no-op.
	var outboxID string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT outbox_id FROM %s WHERE aggregate_id = $1`, s.outboxTable()),
		res.MemoryID,
	).Scan(&outboxID); err != nil {
		t.Fatalf("lookup outbox_id: %v", err)
	}
	rqRes, err := s.RequeueFailed(ctx, outboxID)
	if err != nil {
		t.Fatalf("RequeueFailed: %v", err)
	}
	if rqRes.RowsAffected != 0 {
		t.Fatalf("RowsAffected = %d, want 0 (row is pending, not failed)", rqRes.RowsAffected)
	}
}

func TestListFailed_OrdersByCreatedAtDesc(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_list_order", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Write 3 records, then mark all failed. Backdate created_at so the order
	// is deterministic regardless of insert speed.
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		res, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
			TenantID:       "tenant_a",
			IdempotencyKey: fmt.Sprintf("idem_list_%d", i),
			RequestHash:    fmt.Sprintf("hash_list_%d", i),
			Record: corememory.MemoryRecord{
				UserID:                "user_a",
				Kind:                  "episodic",
				Source:                "user_saved",
				Category:              "project",
				Content:               fmt.Sprintf("list %d", i),
				NormalizedContentHash: fmt.Sprintf("hash-list-%d", i),
				Tags:                  []string{"relay"},
				Importance:            0.9,
			},
		})
		if err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
		ids = append(ids, res.MemoryID)
	}
	// Mark each failed and backdate created_at: row 0 oldest, row 2 newest.
	for i, mid := range ids {
		outboxID := markOutboxFailed(t, ctx, pool, s.outboxTable(), mid, fmt.Sprintf("err %d", i), 5)
		if _, err := pool.Exec(ctx,
			fmt.Sprintf(`UPDATE %s SET created_at = NOW() - ($1 * INTERVAL '1 hour') WHERE outbox_id = $2`, s.outboxTable()),
			3-i, outboxID, // i=0 → 3h ago (oldest), i=2 → 1h ago (newest)
		); err != nil {
			t.Fatalf("backdate created_at: %v", err)
		}
	}

	rows, err := s.ListFailed(ctx, 10)
	if err != nil {
		t.Fatalf("ListFailed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	// Newest first.
	if rows[0].AggregateID != ids[2] || rows[1].AggregateID != ids[1] || rows[2].AggregateID != ids[0] {
		t.Fatalf("ordering: got %v %v %v, want %v %v %v",
			rows[0].AggregateID, rows[1].AggregateID, rows[2].AggregateID,
			ids[2], ids[1], ids[0])
	}
	if rows[0].LastError != "err 2" {
		t.Fatalf("LastError[0] = %q, want %q", rows[0].LastError, "err 2")
	}
	if rows[0].AttemptCount != 5 {
		t.Fatalf("AttemptCount[0] = %d, want 5", rows[0].AttemptCount)
	}
}

func TestListFailed_RespectsLimit(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_list_limit", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	for i := 0; i < 5; i++ {
		res, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
			TenantID:       "tenant_a",
			IdempotencyKey: fmt.Sprintf("idem_limit_%d", i),
			RequestHash:    fmt.Sprintf("hash_limit_%d", i),
			Record: corememory.MemoryRecord{
				UserID:                "user_a",
				Kind:                  "episodic",
				Source:                "user_saved",
				Category:              "project",
				Content:               fmt.Sprintf("limit %d", i),
				NormalizedContentHash: fmt.Sprintf("hash-limit-%d", i),
				Tags:                  []string{"relay"},
				Importance:            0.9,
			},
		})
		if err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
		markOutboxFailed(t, ctx, pool, s.outboxTable(), res.MemoryID, "x", 5)
	}

	rows, err := s.ListFailed(ctx, 2)
	if err != nil {
		t.Fatalf("ListFailed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (limit)", len(rows))
	}
}
