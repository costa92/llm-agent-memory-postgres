package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/jackc/pgx/v5"
)

func TestWriteRecord_CreatesRecordEventOutboxAndIdempotency(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_write", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	in := corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_a",
		RequestHash:    "hash_a",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "remember this",
			NormalizedContentHash: "hash-content-a",
			Tags:                  []string{"tag1"},
			Importance:            0.8,
		},
	}

	got, err := s.WriteRecord(ctx, in)
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if !got.Created || got.MemoryID == "" || got.Version != 1 {
		t.Fatalf("WriteRecord result = %+v", got)
	}

	assertCount(t, ctx, pool, s.memoryRecordTable(), 1)
	assertCount(t, ctx, pool, s.memoryEventTable(), 1)
	assertCount(t, ctx, pool, s.outboxTable(), 1)
	assertCount(t, ctx, pool, s.idempotencyTable(), 1)
}

func TestWriteRecord_ReplaysSameIdempotencyKeyAndHash(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_replay", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	in := corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_replay",
		RequestHash:    "hash_replay",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "remember replay",
			NormalizedContentHash: "hash-content-replay",
			Tags:                  []string{"tag1"},
			Importance:            0.8,
		},
	}

	first, err := s.WriteRecord(ctx, in)
	if err != nil {
		t.Fatalf("first WriteRecord: %v", err)
	}
	second, err := s.WriteRecord(ctx, in)
	if err != nil {
		t.Fatalf("second WriteRecord: %v", err)
	}
	if second.MemoryID != first.MemoryID || second.Version != first.Version {
		t.Fatalf("replay mismatch: first=%+v second=%+v", first, second)
	}

	assertCount(t, ctx, pool, s.memoryRecordTable(), 1)
	assertCount(t, ctx, pool, s.memoryEventTable(), 1)
	assertCount(t, ctx, pool, s.outboxTable(), 1)
}

func TestWriteRecord_ConflictsOnSameIdempotencyKeyDifferentHash(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_conflict", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	base := corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_conflict",
		RequestHash:    "hash_one",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "remember conflict",
			NormalizedContentHash: "hash-content-conflict",
			Tags:                  []string{"tag1"},
			Importance:            0.8,
		},
	}
	if _, err := s.WriteRecord(ctx, base); err != nil {
		t.Fatalf("first WriteRecord: %v", err)
	}

	_, err = s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       base.TenantID,
		IdempotencyKey: base.IdempotencyKey,
		RequestHash:    "hash_two",
		Record:         base.Record,
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func assertCount(t *testing.T, ctx context.Context, pool dbQueryer, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}

type dbQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}
