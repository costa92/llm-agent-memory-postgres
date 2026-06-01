package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

func TestPromote_TransitionsWorkingToEpisodicAndEmitsEvent(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_promote", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	created, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_create_working",
		RequestHash:    "hash_create_working",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindWorking,
			Source:                "agent_inferred",
			Category:              "session",
			Content:               "transient fact",
			NormalizedContentHash: "hash-working-promote",
			Importance:            0.6,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	got, err := s.Promote(ctx, corememory.PromoteRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		SourceEventID:   "evt_source_1",
		IdempotencyKey:  "idem_promote_1",
		Reason:          "threshold_met",
	})
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if !got.Created {
		t.Fatalf("Created = false, want true on first promote")
	}
	if got.Record.Kind != corememory.RecordKindEpisodic {
		t.Fatalf("Record.Kind = %q, want %q", got.Record.Kind, corememory.RecordKindEpisodic)
	}
	if got.Record.ConsolidatedFromEventID != "evt_source_1" {
		t.Fatalf("ConsolidatedFromEventID = %q, want evt_source_1", got.Record.ConsolidatedFromEventID)
	}
	if got.Version != created.Version+1 {
		t.Fatalf("Version = %d, want %d", got.Version, created.Version+1)
	}

	stored, err := s.GetRecord(ctx, created.Record.TenantID, created.MemoryID)
	if err != nil {
		t.Fatalf("GetRecord: %v", err)
	}
	if stored.Kind != corememory.RecordKindEpisodic {
		t.Fatalf("stored.Kind = %q, want %q", stored.Kind, corememory.RecordKindEpisodic)
	}

	event := latestEventPayload(t, ctx, pool, s.memoryEventTable())
	if event.EventType != eventTypeMemoryPromoted {
		t.Fatalf("event type = %q, want %q", event.EventType, eventTypeMemoryPromoted)
	}
	outbox := latestOutboxPayload(t, ctx, pool, s.outboxTable())
	if outbox.EventType != eventTypeMemoryPromoted {
		t.Fatalf("outbox event_type = %q, want %q", outbox.EventType, eventTypeMemoryPromoted)
	}

	var aggregateType string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT aggregate_type FROM %s WHERE event_type = $1 ORDER BY created_at DESC LIMIT 1`, s.outboxTable()),
		eventTypeMemoryPromoted,
	).Scan(&aggregateType); err != nil {
		t.Fatalf("scan aggregate_type: %v", err)
	}
	if aggregateType != "memory" {
		t.Fatalf("aggregate_type = %q, want memory", aggregateType)
	}
}

func TestPromote_ReplaysSameIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_promote_replay", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	created, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_create_working_replay",
		RequestHash:    "hash_create_working_replay",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindWorking,
			Source:                "agent_inferred",
			Category:              "session",
			Content:               "transient replay",
			NormalizedContentHash: "hash-working-replay",
			Importance:            0.6,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	input := corememory.PromoteRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		SourceEventID:   "evt_source_replay",
		IdempotencyKey:  "idem_promote_replay",
		Reason:          "threshold_met",
	}
	first, err := s.Promote(ctx, input)
	if err != nil {
		t.Fatalf("first Promote: %v", err)
	}
	second, err := s.Promote(ctx, input)
	if err != nil {
		t.Fatalf("second Promote: %v", err)
	}

	if second.Created {
		t.Fatalf("second.Created = true, want false on replay")
	}
	if second.MemoryID != first.MemoryID || second.Version != first.Version {
		t.Fatalf("replay mismatch: first=%+v second=%+v", first, second)
	}
	assertCount(t, ctx, pool, s.memoryEventTable(), 2)
	assertCount(t, ctx, pool, s.outboxTable(), 2)
}

func TestPromote_RejectsStaleVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_promote_conflict", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	created, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_create_working_conflict",
		RequestHash:    "hash_create_working_conflict",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  corememory.RecordKindWorking,
			Source:                "agent_inferred",
			Category:              "session",
			Content:               "transient conflict",
			NormalizedContentHash: "hash-working-conflict",
			Importance:            0.6,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	_, err = s.Promote(ctx, corememory.PromoteRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version + 1,
		SourceEventID:   "evt_source_conflict",
		IdempotencyKey:  "idem_promote_conflict",
		Reason:          "threshold_met",
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}
