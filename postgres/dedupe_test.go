package postgres

import (
	"context"
	"fmt"
	"sync"
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

// TestResolveDedupe_ConcurrentFirstWritersOneWinsNoError is the C1 regression:
// two callers race ResolveDedupe with the SAME dedupe_key against an empty
// index. The old "SELECT ... FOR UPDATE then bare INSERT" head does not
// serialise this — FOR UPDATE locks nothing when the row is absent, so both
// callers INSERT and one hits the unique constraint, surfacing a raw error and
// (in production) failing the session close. The fix must let exactly one
// become the winner (DedupeNoCollision) and the other resolve as a loser
// (DedupeMergedExisting), with NO error from either.
func TestResolveDedupe_ConcurrentFirstWritersOneWinsNoError(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_dedupe_race", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	const dedupeKey = "tenant:user:project:race-content"
	mkCandidate := func(idemSuffix, hash string) corememory.MemoryRecord {
		res, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
			TenantID:       "tenant_a",
			IdempotencyKey: "idem_dedupe_race_" + idemSuffix,
			RequestHash:    "hash_dedupe_race_" + idemSuffix,
			Record: corememory.MemoryRecord{
				UserID:                "user_a",
				Kind:                  corememory.RecordKindEpisodic,
				Source:                "user_saved",
				Category:              "project",
				Content:               "race content",
				NormalizedContentHash: hash,
				Importance:            0.8,
			},
		})
		if err != nil {
			t.Fatalf("WriteRecord %s: %v", idemSuffix, err)
		}
		return res.Record
	}

	candA := mkCandidate("a", "race-hash-a")
	candB := mkCandidate("b", "race-hash-b")

	type outcome struct {
		res corememory.ResolveDedupeResult
		err error
	}
	results := make([]outcome, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, cand := range []corememory.MemoryRecord{candA, candB} {
		wg.Add(1)
		go func(idx int, candidate corememory.MemoryRecord) {
			defer wg.Done()
			<-start // fire both as simultaneously as possible
			res, err := s.ResolveDedupe(ctx, corememory.ResolveDedupeInput{
				TenantID:  candidate.TenantID,
				DedupeKey: dedupeKey,
				Candidate: candidate,
			})
			results[idx] = outcome{res: res, err: err}
		}(i, cand)
	}
	close(start)
	wg.Wait()

	for i, o := range results {
		if o.err != nil {
			t.Fatalf("ResolveDedupe[%d] returned error (C1 race not handled): %v", i, o.err)
		}
	}

	winners := 0
	losers := 0
	var winnerID string
	for _, o := range results {
		switch o.res.Action {
		case corememory.DedupeNoCollision:
			winners++
			winnerID = o.res.WinnerID
		case corememory.DedupeMergedExisting, corememory.DedupeCollapsedByPin:
			losers++
		default:
			t.Fatalf("unexpected action %v", o.res.Action)
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("want exactly one winner and one loser, got winners=%d losers=%d", winners, losers)
	}
	// Both callers must agree on the winner id.
	for _, o := range results {
		if o.res.WinnerID != winnerID {
			t.Fatalf("callers disagree on winner: %q vs %q", o.res.WinnerID, winnerID)
		}
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
