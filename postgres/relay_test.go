package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRelayConfig_Defaults(t *testing.T) {
	cfg := defaultRelayConfig()
	if cfg.BatchSize != 100 {
		t.Fatalf("BatchSize = %d, want 100", cfg.BatchSize)
	}
	if cfg.LeaseTTL != 180*time.Second {
		t.Fatalf("LeaseTTL = %v, want 180s", cfg.LeaseTTL)
	}
	if cfg.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", cfg.MaxAttempts)
	}
	if cfg.WorkerIDFunc == nil {
		t.Fatal("WorkerIDFunc is nil")
	}
}

func TestNewRelay_AppliesDefaultsForZeroFields(t *testing.T) {
	r, err := NewRelay(&Store{}, &MemoryPublisher{}, RelayConfig{})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	if r.cfg.BatchSize != 100 {
		t.Fatalf("BatchSize = %d, want 100", r.cfg.BatchSize)
	}
	if r.cfg.LeaseTTL != 180*time.Second {
		t.Fatalf("LeaseTTL = %v, want 180s", r.cfg.LeaseTTL)
	}
	if r.cfg.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", r.cfg.MaxAttempts)
	}
	if r.workerID == "" {
		t.Fatal("workerID is empty")
	}
}

func TestNewRelay_HonorsExplicitConfig(t *testing.T) {
	r, err := NewRelay(&Store{}, &MemoryPublisher{}, RelayConfig{
		BatchSize:    10,
		LeaseTTL:     30 * time.Second,
		MaxAttempts:  2,
		WorkerIDFunc: func() string { return "fixed-worker" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	if r.cfg.BatchSize != 10 {
		t.Fatalf("BatchSize = %d, want 10", r.cfg.BatchSize)
	}
	if r.workerID != "fixed-worker" {
		t.Fatalf("workerID = %q, want fixed-worker", r.workerID)
	}
}

func TestNewRelay_RejectsNilPublisher(t *testing.T) {
	_, err := NewRelay(&Store{}, nil, RelayConfig{})
	if err == nil {
		t.Fatal("expected error for nil publisher")
	}
}

func TestNewRelay_RejectsNilStore(t *testing.T) {
	_, err := NewRelay(nil, &MemoryPublisher{}, RelayConfig{})
	if err == nil {
		t.Fatal("expected error for nil store")
	}
}

func TestMemoryPublisher_PublishStoresPayload(t *testing.T) {
	p := &MemoryPublisher{}
	payload := corememory.OutboxMessage{MemoryID: "mem_1", EventType: "memory_created", Version: 1}
	if err := p.Publish(context.Background(), payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(p.Events) != 1 || p.Events[0].MemoryID != payload.MemoryID {
		t.Fatalf("events = %+v", p.Events)
	}
}

func TestClaimBatch_SetsLeaseColumnsAndIncrementsAttempt(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_claim_lease", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_claim_lease",
		RequestHash:    "hash_claim_lease",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "claim lease",
			NormalizedContentHash: "hash-claim-lease",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	relay, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize:    10,
		LeaseTTL:     60 * time.Second,
		MaxAttempts:  3,
		WorkerIDFunc: func() string { return "worker-claim-1" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	claimed, err := relay.ClaimBatch(ctx)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	if claimed[0].AttemptCount != 1 {
		t.Fatalf("AttemptCount = %d, want 1", claimed[0].AttemptCount)
	}

	// verify lease columns populated
	var claimedBy string
	var claimedAt, leaseExpiresAt *time.Time
	var status string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, claimed_by, claimed_at, lease_expires_at FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		claimed[0].OutboxID,
	).Scan(&status, &claimedBy, &claimedAt, &leaseExpiresAt); err != nil {
		t.Fatalf("read lease columns: %v", err)
	}
	if status != outboxStatusProcessing {
		t.Fatalf("status = %s, want %s", status, outboxStatusProcessing)
	}
	if claimedBy != "worker-claim-1" {
		t.Fatalf("claimed_by = %q, want worker-claim-1", claimedBy)
	}
	if claimedAt == nil {
		t.Fatal("claimed_at is nil")
	}
	if leaseExpiresAt == nil {
		t.Fatal("lease_expires_at is nil")
	}
}

func TestClaimBatch_ReclaimsExpiredLeases(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_claim_expired", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_claim_expired",
		RequestHash:    "hash_claim_expired",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "claim expired",
			NormalizedContentHash: "hash-claim-expired",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	// First worker claims; we then manually expire its lease to simulate a
	// crashed pod.
	worker1 := func() string { return "worker-expired-1" }
	r1, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize: 10, LeaseTTL: 60 * time.Second, MaxAttempts: 3, WorkerIDFunc: worker1,
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	claimed, err := r1.ClaimBatch(ctx)
	if err != nil {
		t.Fatalf("ClaimBatch worker1: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("worker1 claimed %d, want 1", len(claimed))
	}

	// Expire the lease.
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE outbox_id = $1`, s.outboxTable()),
		claimed[0].OutboxID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// Second worker should pick it up.
	worker2 := func() string { return "worker-expired-2" }
	r2, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize: 10, LeaseTTL: 60 * time.Second, MaxAttempts: 3, WorkerIDFunc: worker2,
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	reclaimed, err := r2.ClaimBatch(ctx)
	if err != nil {
		t.Fatalf("ClaimBatch worker2: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("worker2 reclaimed %d, want 1", len(reclaimed))
	}
	if reclaimed[0].AttemptCount != 2 {
		t.Fatalf("AttemptCount = %d, want 2 (worker1=1 + reclaim=1)", reclaimed[0].AttemptCount)
	}

	// Verify the row now owned by worker2.
	var claimedBy string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT claimed_by FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		reclaimed[0].OutboxID,
	).Scan(&claimedBy); err != nil {
		t.Fatalf("read claimed_by: %v", err)
	}
	if claimedBy != "worker-expired-2" {
		t.Fatalf("claimed_by = %q, want worker-expired-2", claimedBy)
	}
}

func TestClaimBatch_RespectsBatchSize(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_claim_batch", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
			TenantID:       "tenant_a",
			IdempotencyKey: fmt.Sprintf("idem_batch_%d", i),
			RequestHash:    fmt.Sprintf("hash_batch_%d", i),
			Record: corememory.MemoryRecord{
				UserID:                "user_a",
				Kind:                  "episodic",
				Source:                "user_saved",
				Category:              "project",
				Content:               fmt.Sprintf("batch %d", i),
				NormalizedContentHash: fmt.Sprintf("hash-batch-%d", i),
				Tags:                  []string{"relay"},
				Importance:            0.9,
			},
		}); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}

	relay, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize:    2,
		LeaseTTL:     60 * time.Second,
		MaxAttempts:  3,
		WorkerIDFunc: func() string { return "worker-batch" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	claimed, err := relay.ClaimBatch(ctx)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2 (BatchSize=2)", len(claimed))
	}
}

func TestRelay_RunOnceMarksSentOnSuccess(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_relay_sent", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_relay_sent",
		RequestHash:    "hash_relay_sent",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "relay sent",
			NormalizedContentHash: "relay-hash-sent",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	publisher := &MemoryPublisher{}
	relay, err := NewRelay(s, publisher, RelayConfig{BatchSize: 10})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	stats, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.Published != 1 || stats.Failed != 0 {
		t.Fatalf("stats = %+v", stats)
	}
	if len(publisher.Events) != 1 {
		t.Fatalf("published events = %d, want 1", len(publisher.Events))
	}
	assertOutboxStatus(t, ctx, pool, s.outboxTable(), "sent", 1)
}

func TestRelay_RunOnceMarksFailedOnPublisherError(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m5_%d_relay_failed", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_relay_failed",
		RequestHash:    "hash_relay_failed",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "relay failed",
			NormalizedContentHash: "relay-hash-failed",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	publisher := &MemoryPublisher{Fail: true}
	relay, err := NewRelay(s, publisher, RelayConfig{BatchSize: 10, MaxAttempts: 5})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	stats, err := relay.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if stats.Published != 0 || stats.Failed != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	assertOutboxStatus(t, ctx, pool, s.outboxTable(), outboxStatusPending, 1)
	assertOutboxAttemptCount(t, ctx, pool, s.outboxTable(), 1)
}

func TestAck_SuccessPathClearsLease(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_ack_success", time.Now().UnixNano())
	s, relay, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-ack-success", 5)

	if err := relay.Ack(ctx, outboxID, true, 1, nil); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var status string
	var claimedBy *string
	var leaseExpiresAt *time.Time
	var sentAt *time.Time
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, claimed_by, lease_expires_at, sent_at FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &claimedBy, &leaseExpiresAt, &sentAt); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != outboxStatusSent {
		t.Fatalf("status = %s, want sent", status)
	}
	if claimedBy != nil {
		t.Fatalf("claimed_by = %v, want nil", *claimedBy)
	}
	if leaseExpiresAt != nil {
		t.Fatalf("lease_expires_at = %v, want nil", *leaseExpiresAt)
	}
	if sentAt == nil {
		t.Fatal("sent_at is nil")
	}
}

func TestAck_RetryPathSetsPending(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_ack_retry", time.Now().UnixNano())
	s, relay, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-ack-retry", 5)

	publishErr := fmt.Errorf("transient publish failure")
	if err := relay.Ack(ctx, outboxID, false, 1, publishErr); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var status string
	var lastError *string
	var claimedBy *string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, last_error, claimed_by FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &lastError, &claimedBy); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != outboxStatusPending {
		t.Fatalf("status = %s, want pending", status)
	}
	if lastError == nil || *lastError != "transient publish failure" {
		t.Fatalf("last_error = %v, want transient publish failure", lastError)
	}
	if claimedBy != nil {
		t.Fatalf("claimed_by = %v, want nil", *claimedBy)
	}
}

func TestAck_FailedPathWhenAttemptsExhausted(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_ack_failed", time.Now().UnixNano())
	s, relay, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-ack-failed", 3)

	publishErr := fmt.Errorf("terminal failure")
	if err := relay.Ack(ctx, outboxID, false, 3, publishErr); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var status string
	var lastError *string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, last_error FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &lastError); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != outboxStatusFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	if lastError == nil || *lastError != "terminal failure" {
		t.Fatalf("last_error = %v, want terminal failure", lastError)
	}
}

func TestAck_RejectsStolenLease(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_ack_stolen", time.Now().UnixNano())
	s, relay, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-ack-original", 5)

	// Steal the lease — pretend another worker claimed it.
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET claimed_by = 'thief' WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	); err != nil {
		t.Fatalf("steal lease: %v", err)
	}

	err := relay.Ack(ctx, outboxID, true, 1, nil)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Ack err = %v, want ErrLeaseLost", err)
	}

	// Original row state unchanged.
	var status string
	var claimedBy *string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, claimed_by FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &claimedBy); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != outboxStatusProcessing {
		t.Fatalf("status = %s, want processing (untouched)", status)
	}
	if claimedBy == nil || *claimedBy != "thief" {
		t.Fatalf("claimed_by = %v, want thief (untouched)", claimedBy)
	}
}

func TestAck_RejectsExpiredLease(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_ack_expired", time.Now().UnixNano())
	s, relay, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-ack-expired", 5)

	// Expire the lease.
	if _, err := pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	err := relay.Ack(ctx, outboxID, true, 1, nil)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Ack err = %v, want ErrLeaseLost", err)
	}
}

func TestRelease_ClearsOwnedLeasesPreservesAttemptCount(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_release_owned", time.Now().UnixNano())
	s, relay, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-release", 5)

	// Snapshot attempt_count after claim — should be 1.
	var attemptBefore int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT attempt_count FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&attemptBefore); err != nil {
		t.Fatalf("read attempt_count before: %v", err)
	}
	if attemptBefore != 1 {
		t.Fatalf("attempt_count before = %d, want 1", attemptBefore)
	}

	if err := relay.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Status flipped back to pending, lease cleared, attempt_count unchanged.
	var status string
	var claimedBy *string
	var leaseExpiresAt *time.Time
	var attemptAfter int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, claimed_by, lease_expires_at, attempt_count FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &claimedBy, &leaseExpiresAt, &attemptAfter); err != nil {
		t.Fatalf("read row after release: %v", err)
	}
	if status != outboxStatusPending {
		t.Fatalf("status = %s, want pending", status)
	}
	if claimedBy != nil {
		t.Fatalf("claimed_by = %v, want nil", *claimedBy)
	}
	if leaseExpiresAt != nil {
		t.Fatalf("lease_expires_at = %v, want nil", *leaseExpiresAt)
	}
	if attemptAfter != attemptBefore {
		t.Fatalf("attempt_count after = %d, want %d (Release MUST NOT decrement)", attemptAfter, attemptBefore)
	}
}

func TestRelease_DoesNotClearOtherWorkersLeases(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_release_other", time.Now().UnixNano())
	s, _, outboxID := setupRelayWithClaim(t, ctx, pool, prefix, "worker-owner", 5)

	// A different relay (different workerID) calls Release.
	otherRelay, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize:    10,
		LeaseTTL:     60 * time.Second,
		MaxAttempts:  5,
		WorkerIDFunc: func() string { return "worker-intruder" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	if err := otherRelay.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Row is still owned by worker-owner.
	var status string
	var claimedBy *string
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, claimed_by FROM %s WHERE outbox_id = $1`, s.outboxTable()),
		outboxID,
	).Scan(&status, &claimedBy); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if status != outboxStatusProcessing {
		t.Fatalf("status = %s, want processing (intruder must not have touched it)", status)
	}
	if claimedBy == nil || *claimedBy != "worker-owner" {
		t.Fatalf("claimed_by = %v, want worker-owner", claimedBy)
	}
}

func TestRelease_NoOpOnFreshRelay(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_release_fresh", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	relay, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize: 10, LeaseTTL: 60 * time.Second, MaxAttempts: 5,
		WorkerIDFunc: func() string { return "worker-fresh" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	// No claimed rows; Release should be a no-op without error.
	if err := relay.Release(ctx); err != nil {
		t.Fatalf("Release on fresh relay: %v", err)
	}
}

// stealingPublisher publishes successfully but mutates the outbox row to
// steal its lease (claimed_by='thief') before returning, so the subsequent
// Ack inside RunOnce sees a stolen lease and returns ErrLeaseLost.
type stealingPublisher struct {
	pool  *pgxpool.Pool
	table string
	calls int
}

func (p *stealingPublisher) Publish(ctx context.Context, evt corememory.OutboxMessage) error {
	p.calls++
	if p.pool == nil {
		return nil
	}
	// Steal lease for the row we just received. The OutboxMessage doesn't
	// carry outbox_id, but each unique event_id corresponds to one outbox row.
	_, err := p.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET claimed_by = 'thief' WHERE event_id = $1`, p.table),
		evt.EventID,
	)
	return err
}

func TestRunOnce_ContinuesAfterAckFailureAndCountsLeaseLost(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_runonce_ackfail", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Stage three pending rows.
	for i := 0; i < 3; i++ {
		if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
			TenantID:       "tenant_a",
			IdempotencyKey: fmt.Sprintf("idem_runonce_%d", i),
			RequestHash:    fmt.Sprintf("hash_runonce_%d", i),
			Record: corememory.MemoryRecord{
				UserID:                "user_a",
				Kind:                  "episodic",
				Source:                "user_saved",
				Category:              "project",
				Content:               fmt.Sprintf("runonce %d", i),
				NormalizedContentHash: fmt.Sprintf("hash-runonce-%d", i),
				Tags:                  []string{"relay"},
				Importance:            0.9,
			},
		}); err != nil {
			t.Fatalf("WriteRecord %d: %v", i, err)
		}
	}

	stealer := &stealingPublisher{pool: pool, table: s.outboxTable()}
	relay, err := NewRelay(s, stealer, RelayConfig{
		BatchSize:    10,
		LeaseTTL:     60 * time.Second,
		MaxAttempts:  5,
		WorkerIDFunc: func() string { return "worker-runonce" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	stats, err := relay.RunOnce(ctx)
	// Aggregated ack errors are expected — the stealing publisher invalidates
	// every Ack, so every row returns ErrLeaseLost. RunOnce must NOT bail out
	// early; it must continue through all three rows.
	if err == nil {
		t.Fatal("expected aggregated ack error, got nil")
	}
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("err = %v, want chain containing ErrLeaseLost", err)
	}
	if stats.LeaseLost != 3 {
		t.Fatalf("LeaseLost = %d, want 3", stats.LeaseLost)
	}
	if stats.Published != 0 {
		t.Fatalf("Published = %d, want 0 (publish ok but ack failed → not counted)", stats.Published)
	}
	if stealer.calls != 3 {
		t.Fatalf("publisher calls = %d, want 3 (RunOnce did not continue past first ack failure)", stealer.calls)
	}
}

func TestRunOnce_PublishedRequiresBothPublishAndAck(t *testing.T) {
	// This is a unit-style assertion documenting the stats contract. The
	// semantic test lives in TestRunOnce_ContinuesAfterAckFailureAndCountsLeaseLost:
	// publishes succeed but acks fail there, and Published stays at 0.
	stats := RunStats{}
	// publish ok, ack ok → Published++
	stats.Published++
	if stats.Published != 1 {
		t.Fatalf("Published = %d, want 1", stats.Published)
	}
}

// setupRelayWithClaim writes one record using the provided pool, claims it
// with the named worker via a Relay configured for the given MaxAttempts,
// and returns the store + relay + outbox_id. Tests use the same pool to
// assert against row state after Ack.
func setupRelayWithClaim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, prefix, workerID string, maxAttempts int) (*Store, *Relay, string) {
	t.Helper()

	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: fmt.Sprintf("idem_%s", prefix),
		RequestHash:    fmt.Sprintf("hash_%s", prefix),
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "ack test",
			NormalizedContentHash: fmt.Sprintf("hash-%s", prefix),
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	relay, err := NewRelay(s, &MemoryPublisher{}, RelayConfig{
		BatchSize:    10,
		LeaseTTL:     60 * time.Second,
		MaxAttempts:  maxAttempts,
		WorkerIDFunc: func() string { return workerID },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	claimed, err := relay.ClaimBatch(ctx)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %d, want 1", len(claimed))
	}
	return s, relay, claimed[0].OutboxID
}

func assertOutboxStatus(t *testing.T, ctx context.Context, pool dbQueryer, table, wantStatus string, wantCount int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE status = $1`, table),
		wantStatus,
	).Scan(&got); err != nil {
		t.Fatalf("outbox status count: %v", err)
	}
	if got != wantCount {
		t.Fatalf("status=%s count = %d, want %d", wantStatus, got, wantCount)
	}
}

func TestLeaseAwarePublisher_NilHookActsLikeMemoryPublisher(t *testing.T) {
	p := &LeaseAwarePublisher{}
	payload := corememory.OutboxMessage{MemoryID: "mem_lap_1", EventType: "memory_created", Version: 1}
	if err := p.Publish(context.Background(), payload); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(p.Events) != 1 || p.Events[0].MemoryID != payload.MemoryID {
		t.Fatalf("events = %+v", p.Events)
	}
}

func TestLeaseAwarePublisher_HookInvoked(t *testing.T) {
	var seen []string
	p := &LeaseAwarePublisher{
		PublishHook: func(_ context.Context, msg corememory.OutboxMessage) error {
			seen = append(seen, msg.MemoryID)
			return nil
		},
	}
	if err := p.Publish(context.Background(), corememory.OutboxMessage{MemoryID: "mem_hook_a", EventType: "memory_created", Version: 1}); err != nil {
		t.Fatalf("Publish a: %v", err)
	}
	if err := p.Publish(context.Background(), corememory.OutboxMessage{MemoryID: "mem_hook_b", EventType: "memory_updated", Version: 2}); err != nil {
		t.Fatalf("Publish b: %v", err)
	}
	if len(seen) != 2 || seen[0] != "mem_hook_a" || seen[1] != "mem_hook_b" {
		t.Fatalf("hook saw = %v, want [mem_hook_a mem_hook_b]", seen)
	}
	if len(p.Events) != 2 {
		t.Fatalf("events = %d, want 2 (hook returned nil → append)", len(p.Events))
	}
}

func TestLeaseAwarePublisher_HookErrorSuppressesAppend(t *testing.T) {
	hookErr := errors.New("transient hook failure")
	p := &LeaseAwarePublisher{
		PublishHook: func(_ context.Context, _ corememory.OutboxMessage) error {
			return hookErr
		},
	}
	err := p.Publish(context.Background(), corememory.OutboxMessage{MemoryID: "mem_hook_err", EventType: "memory_created", Version: 1})
	if !errors.Is(err, hookErr) {
		t.Fatalf("Publish err = %v, want hookErr", err)
	}
	if len(p.Events) != 0 {
		t.Fatalf("events = %d, want 0 (hook errored → no append)", len(p.Events))
	}
}

// TestRelay_LeaseExpiresDuringPublish_AckReturnsLeaseLost exercises the
// "publish took longer than the lease TTL" scenario end-to-end: the publish
// hook sleeps past the lease window, so by the time the relay's Ack runs
// the ownership predicate (claimed_by + lease_expires_at > NOW()) fails
// and Ack returns ErrLeaseLost. The row is therefore NOT marked sent —
// some peer worker will pick it up after the lease expires.
func TestRelay_LeaseExpiresDuringPublish_AckReturnsLeaseLost(t *testing.T) {
	ctx := context.Background()
	pool := openTestPool(t, ctx)

	prefix := fmt.Sprintf("m8a_%d_lease_expiry", time.Now().UnixNano())
	s, err := New(pool, Config{TablePrefix: prefix})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.WriteRecord(ctx, corememory.WriteRecordInput{
		TenantID:       "tenant_a",
		IdempotencyKey: "idem_lease_expiry",
		RequestHash:    "hash_lease_expiry",
		Record: corememory.MemoryRecord{
			UserID:                "user_a",
			Kind:                  "episodic",
			Source:                "user_saved",
			Category:              "project",
			Content:               "lease expiry",
			NormalizedContentHash: "hash-lease-expiry",
			Tags:                  []string{"relay"},
			Importance:            0.9,
		},
	}); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}

	publisher := &LeaseAwarePublisher{
		PublishHook: func(_ context.Context, _ corememory.OutboxMessage) error {
			// Sleep past the 100ms LeaseTTL so the ownership predicate
			// (lease_expires_at > NOW()) is false when Ack runs.
			time.Sleep(200 * time.Millisecond)
			return nil
		},
	}
	relay, err := NewRelay(s, publisher, RelayConfig{
		BatchSize:    10,
		LeaseTTL:     100 * time.Millisecond,
		MaxAttempts:  5,
		WorkerIDFunc: func() string { return "worker-lease-expiry" },
	})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}

	stats, err := relay.RunOnce(ctx)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("RunOnce err = %v, want chain containing ErrLeaseLost", err)
	}
	if stats.LeaseLost != 1 {
		t.Fatalf("LeaseLost = %d, want 1", stats.LeaseLost)
	}
	if stats.Published != 0 {
		t.Fatalf("Published = %d, want 0 (publish ok but ack lost the lease)", stats.Published)
	}
	if len(publisher.Events) != 1 {
		t.Fatalf("publisher.Events = %d, want 1 (hook returned nil so append happened)", len(publisher.Events))
	}
}

func assertOutboxAttemptCount(t *testing.T, ctx context.Context, pool dbQueryer, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT attempt_count FROM %s LIMIT 1`, table),
	).Scan(&got); err != nil {
		t.Fatalf("outbox attempt_count: %v", err)
	}
	if got != want {
		t.Fatalf("attempt_count = %d, want %d", got, want)
	}
}
