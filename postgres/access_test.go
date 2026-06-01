package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

func TestMarkAccess_UpdatesLastAccessAtAndHitCount(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_access", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	created, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_access",
		RequestHash:    "hash_access",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "remember access",
			NormalizedContentHash: "hash-access",
			Importance:            0.7,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	accessedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := s.MarkAccess(ctx, corememory.MarkAccessInput{
		TenantID:   created.Record.TenantID,
		MemoryIDs:  []string{created.MemoryID, created.MemoryID},
		AccessedAt: accessedAt,
	}); err != nil {
		t.Fatalf("MarkAccess: %v", err)
	}

	record, err := s.loadRecordForUpdate(ctx, s.pool, created.Record.TenantID, created.MemoryID)
	if err != nil {
		t.Fatalf("loadRecordForUpdate: %v", err)
	}
	if record.LastAccessAt == nil {
		t.Fatal("LastAccessAt is nil, want timestamp")
	}
	if !record.LastAccessAt.Equal(accessedAt) {
		t.Fatalf("LastAccessAt = %v, want %v", record.LastAccessAt, accessedAt)
	}
	if record.HitCount != 1 {
		t.Fatalf("HitCount = %d, want 1", record.HitCount)
	}
}
