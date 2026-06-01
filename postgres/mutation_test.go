package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPatchRecord_RequiresExpectedVersionAndBumpsVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "patch")
	created := seedRecordForMutation(t, ctx, s)

	got, err := s.PatchRecord(ctx, corememory.PatchRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Content:         "updated content",
		Category:        "updated-category",
		Tags:            []string{"x", "y"},
		Importance:      0.95,
	})
	if err != nil {
		t.Fatalf("PatchRecord: %v", err)
	}
	if got.Version != created.Version+1 {
		t.Fatalf("version = %d, want %d", got.Version, created.Version+1)
	}
	if got.Record.Content != "updated content" || got.Record.Category != "updated-category" {
		t.Fatalf("patched record = %+v", got.Record)
	}
	assertCount(t, ctx, pool, s.memoryEventTable(), 2)
	assertCount(t, ctx, pool, s.outboxTable(), 2)
}

func TestPatchRecord_ReturnsVersionConflictOnStaleVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "patch_conflict")
	created := seedRecordForMutation(t, ctx, s)

	_, err := s.PatchRecord(ctx, corememory.PatchRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version + 1,
		Content:         "stale write",
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}

func TestDeleteRecord_TombstonesAndBumpsVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "delete")
	created := seedRecordForMutation(t, ctx, s)

	got, err := s.DeleteRecord(ctx, corememory.DeleteRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
	})
	if err != nil {
		t.Fatalf("DeleteRecord: %v", err)
	}
	if !got.Record.Deleted || got.Record.DeletedAt == nil {
		t.Fatalf("deleted record = %+v", got.Record)
	}
	if got.Version != created.Version+1 {
		t.Fatalf("version = %d, want %d", got.Version, created.Version+1)
	}
}

func TestPinRecord_TogglesPinnedAndBumpsVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "pin")
	created := seedRecordForMutation(t, ctx, s)

	got, err := s.PinRecord(ctx, corememory.PinRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Pinned:          true,
	})
	if err != nil {
		t.Fatalf("PinRecord: %v", err)
	}
	if !got.Record.Pinned || got.Version != created.Version+1 {
		t.Fatalf("pin result = %+v", got)
	}
	outbox := latestOutboxPayload(t, ctx, pool, s.outboxTable())
	if outbox.EventType != eventTypeMemoryPinned {
		t.Fatalf("outbox event_type = %q, want %q", outbox.EventType, eventTypeMemoryPinned)
	}
}

func TestPinRecord_UnpinEmitsMemoryUnpinned(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "unpin")
	created := seedRecordForMutation(t, ctx, s)
	pinned, err := s.PinRecord(ctx, corememory.PinRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Pinned:          true,
	})
	if err != nil {
		t.Fatalf("PinRecord(pin): %v", err)
	}

	got, err := s.PinRecord(ctx, corememory.PinRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: pinned.Version,
		Pinned:          false,
	})
	if err != nil {
		t.Fatalf("PinRecord(unpin): %v", err)
	}
	if got.Record.Pinned || got.Version != pinned.Version+1 {
		t.Fatalf("unpin result = %+v", got)
	}
	outbox := latestOutboxPayload(t, ctx, pool, s.outboxTable())
	if outbox.EventType != eventTypeMemoryUnpinned {
		t.Fatalf("outbox event_type = %q, want %q", outbox.EventType, eventTypeMemoryUnpinned)
	}
}

func TestDisableRecord_TogglesDisabledAndBumpsVersion(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "disable")
	created := seedRecordForMutation(t, ctx, s)

	got, err := s.DisableRecord(ctx, corememory.DisableRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Disabled:        true,
	})
	if err != nil {
		t.Fatalf("DisableRecord: %v", err)
	}
	if !got.Record.Disabled || got.Version != created.Version+1 {
		t.Fatalf("disable result = %+v", got)
	}
	outbox := latestOutboxPayload(t, ctx, pool, s.outboxTable())
	if outbox.EventType != eventTypeMemoryDisabled {
		t.Fatalf("outbox event_type = %q, want %q", outbox.EventType, eventTypeMemoryDisabled)
	}
}

func TestDisableRecord_EnableEmitsMemoryEnabled(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "enable")
	created := seedRecordForMutation(t, ctx, s)
	disabled, err := s.DisableRecord(ctx, corememory.DisableRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Disabled:        true,
	})
	if err != nil {
		t.Fatalf("DisableRecord(disable): %v", err)
	}

	got, err := s.DisableRecord(ctx, corememory.DisableRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: disabled.Version,
		Disabled:        false,
	})
	if err != nil {
		t.Fatalf("DisableRecord(enable): %v", err)
	}
	if got.Record.Disabled || got.Version != disabled.Version+1 {
		t.Fatalf("enable result = %+v", got)
	}
	outbox := latestOutboxPayload(t, ctx, pool, s.outboxTable())
	if outbox.EventType != eventTypeMemoryEnabled {
		t.Fatalf("outbox event_type = %q, want %q", outbox.EventType, eventTypeMemoryEnabled)
	}
}

func TestPatchRecord_ConcurrentWritersYieldSingleWinner(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "patch_race")
	created := seedRecordForMutation(t, ctx, s)

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, content := range []string{"writer-a", "writer-b"} {
		wg.Add(1)
		go func(content string) {
			defer wg.Done()
			_, err := s.PatchRecord(ctx, corememory.PatchRecordInput{
				TenantID:        created.Record.TenantID,
				MemoryID:        created.MemoryID,
				ExpectedVersion: created.Version,
				Content:         content,
			})
			results <- err
		}(content)
	}
	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected err = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}

	assertCount(t, ctx, pool, s.memoryEventTable(), 2)
	assertCount(t, ctx, pool, s.outboxTable(), 2)
}

func newMutatingStore(t *testing.T, ctx context.Context, pool *pgxpool.Pool, suffix string) *Store {
	t.Helper()
	prefix := fmt.Sprintf("m5_%d_%s", time.Now().UnixNano(), suffix)
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

func seedRecordForMutation(t *testing.T, ctx context.Context, s *Store) corememory.WriteRecordResult {
	t.Helper()
	got, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_" + s.cfg.TablePrefix,
		RequestHash:    "hash_" + s.cfg.TablePrefix,
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "seed content",
			NormalizedContentHash: "seed-hash-" + s.cfg.TablePrefix,
			Tags:                  []string{"seed"},
			Importance:            0.7,
		},
	})
	if err != nil {
		t.Fatalf("WriteRecord seed: %v", err)
	}
	return got
}

func latestOutboxPayload(t *testing.T, ctx context.Context, pool dbQueryer, table string) corememory.OutboxMessage {
	t.Helper()

	var raw []byte
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT payload FROM %s ORDER BY created_at DESC LIMIT 1`, table)).Scan(&raw); err != nil {
		t.Fatalf("select latest outbox payload: %v", err)
	}

	var msg corememory.OutboxMessage
	if err := decodeJSON(raw, &msg); err != nil {
		t.Fatalf("decode latest outbox payload: %v", err)
	}
	return msg
}

func latestEventPayload(t *testing.T, ctx context.Context, pool dbQueryer, table string) corememory.StoredEvent {
	t.Helper()

	var raw []byte
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT payload FROM %s ORDER BY created_at DESC LIMIT 1`, table)).Scan(&raw); err != nil {
		t.Fatalf("select latest event payload: %v", err)
	}

	var evt corememory.StoredEvent
	if err := decodeJSON(raw, &evt); err != nil {
		t.Fatalf("decode latest event payload: %v", err)
	}
	return evt
}

func latestEventPayloadByType(t *testing.T, ctx context.Context, pool dbQueryer, table, eventType string) corememory.StoredEvent {
	t.Helper()

	var raw []byte
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT payload FROM %s WHERE event_type = $1 ORDER BY created_at DESC LIMIT 1`, table), eventType).Scan(&raw); err != nil {
		t.Fatalf("select latest event payload by type %q: %v", eventType, err)
	}

	var evt corememory.StoredEvent
	if err := decodeJSON(raw, &evt); err != nil {
		t.Fatalf("decode latest event payload by type %q: %v", eventType, err)
	}
	return evt
}

func latestOutboxPayloadByType(t *testing.T, ctx context.Context, pool dbQueryer, table, eventType string) corememory.OutboxMessage {
	t.Helper()

	var raw []byte
	if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT payload FROM %s WHERE event_type = $1 ORDER BY created_at DESC LIMIT 1`, table), eventType).Scan(&raw); err != nil {
		t.Fatalf("select latest outbox payload by type %q: %v", eventType, err)
	}

	var msg corememory.OutboxMessage
	if err := decodeJSON(raw, &msg); err != nil {
		t.Fatalf("decode latest outbox payload by type %q: %v", eventType, err)
	}
	return msg
}
