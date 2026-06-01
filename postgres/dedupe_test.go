package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

func TestResolveDedupe_FirstWriterBecomesWinner(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_dedupe_first", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	candidate, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_dedupe_first",
		RequestHash:    "hash_dedupe_first",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "same content",
			NormalizedContentHash: "same-content-hash",
			Importance:            0.8,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	got, err := s.ResolveDedupe(ctx, corememory.ResolveDedupeInput{
		TenantID:  candidate.Record.TenantID,
		DedupeKey: "tenant:user:project:same-content",
		Candidate: candidate.Record,
	})
	if err != nil {
		t.Fatalf("ResolveDedupe: %v", err)
	}
	if got.Action != corememory.DedupeNoCollision {
		t.Fatalf("Action = %v, want DedupeNoCollision", got.Action)
	}
	if got.WinnerID != candidate.MemoryID {
		t.Fatalf("WinnerID = %q, want %q", got.WinnerID, candidate.MemoryID)
	}
}

func TestResolveDedupe_SecondWriterDeletesLoserAndEmitsCollapse(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_dedupe_merge", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	winner, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_dedupe_winner",
		RequestHash:    "hash_dedupe_winner",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "same content",
			NormalizedContentHash: "same-content-hash",
			Importance:            0.8,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord winner: %v", err)
	}
	if _, err := s.ResolveDedupe(ctx, corememory.ResolveDedupeInput{
		TenantID:  winner.Record.TenantID,
		DedupeKey: "tenant:user:project:same-content",
		Candidate: winner.Record,
	}); err != nil {
		t.Fatalf("ResolveDedupe winner: %v", err)
	}

	loser, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_dedupe_loser",
		RequestHash:    "hash_dedupe_loser",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "same content",
			NormalizedContentHash: "same-content-hash-2",
			Importance:            0.6,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord loser: %v", err)
	}

	got, err := s.ResolveDedupe(ctx, corememory.ResolveDedupeInput{
		TenantID:  loser.Record.TenantID,
		DedupeKey: "tenant:user:project:same-content",
		Candidate: loser.Record,
	})
	if err != nil {
		t.Fatalf("ResolveDedupe loser: %v", err)
	}
	if got.Action != corememory.DedupeMergedExisting {
		t.Fatalf("Action = %v, want DedupeMergedExisting", got.Action)
	}
	if got.WinnerID != winner.MemoryID {
		t.Fatalf("WinnerID = %q, want %q", got.WinnerID, winner.MemoryID)
	}

	if _, err := s.GetRecord(ctx, loser.Record.TenantID, loser.MemoryID); err == nil {
		t.Fatal("expected loser to be hidden after dedupe collapse")
	}

	collapseEvent := latestEventPayloadByType(t, ctx, pool, s.memoryEventTable(), eventTypeMemoryDedupeCollapsed)
	if collapseEvent.MemoryID != winner.MemoryID {
		t.Fatalf("collapse event MemoryID = %q, want winner %q", collapseEvent.MemoryID, winner.MemoryID)
	}
	if collapseEvent.Metadata[corememory.DedupeCollapsedLoserIDMetadataKey] != loser.MemoryID {
		t.Fatalf("collapse loser metadata = %v, want %q", collapseEvent.Metadata[corememory.DedupeCollapsedLoserIDMetadataKey], loser.MemoryID)
	}

	deletedEvent := latestEventPayloadByType(t, ctx, pool, s.memoryEventTable(), eventTypeMemoryDeleted)
	if deletedEvent.MemoryID != loser.MemoryID {
		t.Fatalf("deleted event MemoryID = %q, want loser %q", deletedEvent.MemoryID, loser.MemoryID)
	}

	outbox := latestOutboxPayloadByType(t, ctx, pool, s.outboxTable(), eventTypeMemoryDedupeCollapsed)
	if outbox.MemoryID != winner.MemoryID {
		t.Fatalf("collapse outbox MemoryID = %q, want winner %q", outbox.MemoryID, winner.MemoryID)
	}
	if outbox.Metadata[corememory.DedupeCollapsedLoserIDMetadataKey] != loser.MemoryID {
		t.Fatalf("collapse outbox loser metadata = %v, want %q", outbox.Metadata[corememory.DedupeCollapsedLoserIDMetadataKey], loser.MemoryID)
	}
}

func TestResolveDedupe_PinnedWinnerReturnsCollapsedByPin(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_dedupe_pin", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	winner, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_dedupe_pin_winner",
		RequestHash:    "hash_dedupe_pin_winner",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "same content",
			NormalizedContentHash: "same-content-hash-pin",
			Importance:            0.8,
			Pinned:                true,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord winner: %v", err)
	}
	if _, err := s.ResolveDedupe(ctx, corememory.ResolveDedupeInput{
		TenantID:  winner.Record.TenantID,
		DedupeKey: "tenant:user:project:same-content-pin",
		Candidate: winner.Record,
	}); err != nil {
		t.Fatalf("ResolveDedupe winner: %v", err)
	}

	loser, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_dedupe_pin_loser",
		RequestHash:    "hash_dedupe_pin_loser",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindEpisodic,
			Source:                "user_saved",
			Category:              "project",
			Content:               "same content",
			NormalizedContentHash: "same-content-hash-pin-2",
			Importance:            0.6,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord loser: %v", err)
	}

	got, err := s.ResolveDedupe(ctx, corememory.ResolveDedupeInput{
		TenantID:  loser.Record.TenantID,
		DedupeKey: "tenant:user:project:same-content-pin",
		Candidate: loser.Record,
	})
	if err != nil {
		t.Fatalf("ResolveDedupe loser: %v", err)
	}
	if got.Action != corememory.DedupeCollapsedByPin {
		t.Fatalf("Action = %v, want DedupeCollapsedByPin", got.Action)
	}
}
