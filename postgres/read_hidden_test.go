package postgres

import (
	"context"
	"errors"
	"testing"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

func TestGetRecordIncludingHidden_ReturnsDeletedRecord(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "hidden_deleted")
	created := seedRecordForMutation(t, ctx, s)
	if _, err := s.DeleteRecord(ctx, corememory.DeleteRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
	}); err != nil {
		t.Fatalf("DeleteRecord: %v", err)
	}

	// GetRecord hides the deleted record.
	if _, err := s.GetRecord(ctx, created.Record.TenantID, created.MemoryID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRecord err = %v, want ErrNotFound", err)
	}

	// GetRecordIncludingHidden returns it with Deleted==true.
	got, err := s.GetRecordIncludingHidden(ctx, created.Record.TenantID, created.MemoryID)
	if err != nil {
		t.Fatalf("GetRecordIncludingHidden: %v", err)
	}
	if got.MemoryID != created.MemoryID {
		t.Fatalf("record = %+v, want memory_id=%s", got, created.MemoryID)
	}
	if !got.Deleted {
		t.Fatalf("record.Deleted = false, want true")
	}
}

func TestGetRecordIncludingHidden_ReturnsDisabledRecord(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "hidden_disabled")
	created := seedRecordForMutation(t, ctx, s)
	if _, err := s.DisableRecord(ctx, corememory.DisableRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Disabled:        true,
	}); err != nil {
		t.Fatalf("DisableRecord: %v", err)
	}

	// GetRecord hides the disabled record.
	if _, err := s.GetRecord(ctx, created.Record.TenantID, created.MemoryID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRecord err = %v, want ErrNotFound", err)
	}

	got, err := s.GetRecordIncludingHidden(ctx, created.Record.TenantID, created.MemoryID)
	if err != nil {
		t.Fatalf("GetRecordIncludingHidden: %v", err)
	}
	if !got.Disabled {
		t.Fatalf("record.Disabled = false, want true")
	}
}

func TestGetRecordIncludingHidden_AbsentReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "hidden_absent")

	_, err := s.GetRecordIncludingHidden(ctx, "tenant", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
