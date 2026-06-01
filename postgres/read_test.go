package postgres

import (
	"context"
	"errors"
	"testing"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

func TestGetRecord_ReturnsVisibleRecord(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "get_visible")
	created := seedRecordForMutation(t, ctx, s)

	got, err := s.GetRecord(ctx, created.Record.TenantID, created.MemoryID)
	if err != nil {
		t.Fatalf("GetRecord: %v", err)
	}
	if got.MemoryID != created.MemoryID {
		t.Fatalf("record = %+v, want memory_id=%s", got, created.MemoryID)
	}
}

func TestGetRecord_HidesDeletedRecord(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "get_deleted")
	created := seedRecordForMutation(t, ctx, s)
	if _, err := s.DeleteRecord(ctx, corememory.DeleteRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
	}); err != nil {
		t.Fatalf("DeleteRecord: %v", err)
	}

	_, err := s.GetRecord(ctx, created.Record.TenantID, created.MemoryID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestGetRecord_HidesDisabledRecord(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "get_disabled")
	created := seedRecordForMutation(t, ctx, s)
	if _, err := s.DisableRecord(ctx, corememory.DisableRecordInput{
		TenantID:        created.Record.TenantID,
		MemoryID:        created.MemoryID,
		ExpectedVersion: created.Version,
		Disabled:        true,
	}); err != nil {
		t.Fatalf("DisableRecord: %v", err)
	}

	_, err := s.GetRecord(ctx, created.Record.TenantID, created.MemoryID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestGetRecord_EnforcesTenantBoundary(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	s := newMutatingStore(t, ctx, pool, "get_tenant")
	created := seedRecordForMutation(t, ctx, s)

	_, err := s.GetRecord(ctx, "other_tenant", created.MemoryID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
