package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

func TestListSessionWorking_ReturnsOnlyWorkingRecordsForScope(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8b_%d_session", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	tenantID := "tenant_a"
	userID := "user_a"
	projectID := "project_a"
	sessionID := "session_a"
	created, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       tenantID,
		IdempotencyKey: "idem_session_working",
		RequestHash:    "hash_session_working",
		Record: corememory.MemoryRecord{
			UserID:                userID,
			ProjectID:             projectID,
			SessionID:             sessionID,
			Kind:                  corememory.RecordKindWorking,
			Source:                "user_saved",
			Category:              "project",
			Content:               "working note",
			NormalizedContentHash: "hash-working",
			Importance:            0.9,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord working: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       tenantID,
		IdempotencyKey: "idem_other_session",
		RequestHash:    "hash_other_session",
		Record: corememory.MemoryRecord{
			UserID:                userID,
			ProjectID:             projectID,
			SessionID:             "session_b",
			Kind:                  corememory.RecordKindWorking,
			Source:                "user_saved",
			Category:              "project",
			Content:               "other working note",
			NormalizedContentHash: "hash-working-b",
			Importance:            0.8,
		},
	}); err != nil {
		t.Fatalf("WriteRecord other session: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       tenantID,
		IdempotencyKey: "idem_episodic",
		RequestHash:    "hash_episodic",
		Record: corememory.MemoryRecord{
			UserID:                userID,
			ProjectID:             projectID,
			SessionID:             sessionID,
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "episodic note",
			NormalizedContentHash: "hash-episodic",
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord episodic: %v", err)
	}

	records, err := s.ListSessionWorking(ctx, tenantID, userID, projectID, sessionID)
	if err != nil {
		t.Fatalf("ListSessionWorking: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if got := records[0].Record.MemoryID; got != created.MemoryID {
		t.Fatalf("memory_id = %q, want %q", got, created.MemoryID)
	}
	if got := records[0].LatestEventID; got == "" {
		t.Fatal("LatestEventID is empty")
	}
}
