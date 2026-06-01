package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/jackc/pgx/v5"
)

// Publisher is implemented by anything able to forward an OutboxMessage to a
// downstream system (vector index, message broker, etc.). Implementations MUST
// be idempotent — the relay may publish the same message more than once on
// lease loss / crash recovery, and the contract is at-least-once.
type Publisher interface {
	Publish(ctx context.Context, evt corememory.OutboxMessage) error
}

var _ corememory.MessagePublisher = (Publisher)(nil)

// RelayConfig tunes the outbox relay worker. Any zero-value field is replaced
// with the package default at construction time; callers may pass an empty
// RelayConfig{} to accept all defaults.
type RelayConfig struct {
	BatchSize    int
	LeaseTTL     time.Duration
	MaxAttempts  int
	WorkerIDFunc func() string
}

func defaultRelayConfig() RelayConfig {
	return RelayConfig{
		BatchSize:    100,
		LeaseTTL:     180 * time.Second,
		MaxAttempts:  5,
		WorkerIDFunc: NewRandomWorkerID,
	}
}

// ClaimedMessage is the per-row payload returned by ClaimBatch. AttemptCount is
// the value AFTER the claim's UPDATE has incremented it, so callers can compare
// against MaxAttempts directly without re-reading the row.
type ClaimedMessage struct {
	OutboxID     string
	Payload      corememory.OutboxMessage
	AttemptCount int
}

// Relay drives the at-least-once delivery loop on top of the outbox table.
// Each worker has a stable in-process identity (workerID) generated once at
// construction; lease ownership is keyed by that identity.
type Relay struct {
	store     *Store
	publisher Publisher
	cfg       RelayConfig
	workerID  string
}

// RunStats summarises the outcome of a RunOnce invocation. Published counts
// rows that were both published AND acked successfully; Failed counts rows
// whose publish errored but whose ack succeeded; LeaseLost counts rows whose
// ack found the lease no longer owned by this worker (typically because the
// lease TTL elapsed before publish completed).
type RunStats struct {
	Published int
	Failed    int
	LeaseLost int
}

func NewRelay(store *Store, publisher Publisher, cfg RelayConfig) (*Relay, error) {
	if store == nil {
		return nil, fmt.Errorf("memory/postgres: relay store is required")
	}
	if publisher == nil {
		return nil, fmt.Errorf("%w: publisher is required", ErrRelayPublishFailed)
	}
	def := defaultRelayConfig()
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = def.BatchSize
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = def.LeaseTTL
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = def.MaxAttempts
	}
	if cfg.WorkerIDFunc == nil {
		cfg.WorkerIDFunc = def.WorkerIDFunc
	}
	return &Relay{
		store:     store,
		publisher: publisher,
		cfg:       cfg,
		workerID:  cfg.WorkerIDFunc(),
	}, nil
}

// ClaimBatch atomically claims up to BatchSize rows that are either pending or
// have an expired lease, marking each as 'processing' with this worker's
// identity, a fresh lease deadline, and an incremented attempt_count. Returns
// the claimed rows with their AttemptCount set to the post-increment value.
func (r *Relay) ClaimBatch(ctx context.Context) ([]ClaimedMessage, error) {
	tx, err := r.store.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("memory/postgres: begin claim tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx,
		fmt.Sprintf(`SELECT outbox_id, payload, attempt_count
FROM %s
WHERE status = $1
   OR (status = $2 AND lease_expires_at < NOW())
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT $3`, r.store.outboxTable()),
		outboxStatusPending, outboxStatusProcessing, r.cfg.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("memory/postgres: relay query claimable: %w", err)
	}

	type claimRow struct {
		outboxID     string
		payload      corememory.OutboxMessage
		attemptCount int
	}
	var pending []claimRow
	for rows.Next() {
		var outboxID string
		var raw []byte
		var attemptCount int
		if err := rows.Scan(&outboxID, &raw, &attemptCount); err != nil {
			rows.Close()
			return nil, fmt.Errorf("memory/postgres: relay scan claimable: %w", err)
		}
		var payload corememory.OutboxMessage
		if err := decodeJSON(raw, &payload); err != nil {
			rows.Close()
			return nil, fmt.Errorf("memory/postgres: relay decode payload: %w", err)
		}
		pending = append(pending, claimRow{outboxID: outboxID, payload: payload, attemptCount: attemptCount})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("memory/postgres: relay rows: %w", err)
	}
	rows.Close()

	if len(pending) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("memory/postgres: commit empty claim tx: %w", err)
		}
		return nil, nil
	}

	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = p.outboxID
	}

	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
SET status = $1,
    claimed_by = $2,
    claimed_at = NOW(),
    lease_expires_at = NOW() + ($3 * INTERVAL '1 millisecond'),
    attempt_count = attempt_count + 1
WHERE outbox_id = ANY($4)`, r.store.outboxTable()),
		outboxStatusProcessing, r.workerID, r.cfg.LeaseTTL.Milliseconds(), ids,
	); err != nil {
		return nil, fmt.Errorf("memory/postgres: relay claim update: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("memory/postgres: commit claim tx: %w", err)
	}

	out := make([]ClaimedMessage, len(pending))
	for i, p := range pending {
		out[i] = ClaimedMessage{
			OutboxID:     p.outboxID,
			Payload:      p.payload,
			AttemptCount: p.attemptCount + 1, // post-increment view for callers
		}
	}
	return out, nil
}

// RunOnce claims a batch, publishes each row, and acks the outcome. Per spec
// §4.6, ack failures are collected (not fatal) so a single hot row can't stall
// progress on the rest of the batch. The aggregated error is returned via
// errors.Join after the loop completes.
//
// Stats semantics:
//   - Published: publish ok AND ack ok (the row is durably 'sent').
//   - Failed:    publish failed AND ack ok (the row is now 'pending' for retry
//     or 'failed' once attempts exhausted).
//   - LeaseLost: ack returned ErrLeaseLost (the row was not mutated by this
//     worker; some other worker now owns it or will when the lease expires).
func (r *Relay) RunOnce(ctx context.Context) (RunStats, error) {
	claimed, err := r.ClaimBatch(ctx)
	if err != nil {
		return RunStats{}, err
	}

	stats := RunStats{}
	var ackErrs []error
	for _, msg := range claimed {
		publishErr := r.publisher.Publish(ctx, msg.Payload)
		ackErr := r.Ack(ctx, msg.OutboxID, publishErr == nil, msg.AttemptCount, publishErr)
		if ackErr != nil {
			if errors.Is(ackErr, ErrLeaseLost) {
				stats.LeaseLost++
			}
			ackErrs = append(ackErrs, ackErr)
			continue
		}
		if publishErr != nil {
			stats.Failed++
			continue
		}
		stats.Published++
	}
	if len(ackErrs) == 0 {
		return stats, nil
	}
	return stats, errors.Join(ackErrs...)
}

// Ack records the publish outcome for a claimed outbox row. The three UPDATEs
// share an ownership predicate so a worker whose lease expired (or was stolen)
// cannot mutate the row out from under whoever owns it now.
//
//   - ok=true  → status='sent', sent_at set, lease columns cleared, last_error cleared.
//   - ok=false AND attemptCount < MaxAttempts → status='pending', last_error set,
//     lease cleared so the next ClaimBatch can pick it up.
//   - ok=false AND attemptCount >= MaxAttempts → status='failed', last_error set,
//     lease cleared so operators can RequeueFailed it later.
//
// On any branch, zero rows-affected means the ownership predicate failed
// (lease expired or claimed_by mismatch) and we return ErrLeaseLost without
// touching the row. attempt_count is NOT incremented here — ClaimBatch did
// that at claim time.
//
// The ownership predicate is:
//
//	WHERE outbox_id=$1 AND claimed_by=$2 AND lease_expires_at > NOW()
func (r *Relay) Ack(ctx context.Context, outboxID string, ok bool, attemptCount int, publishErr error) error {
	table := r.store.outboxTable()
	const ownership = `WHERE outbox_id = $1 AND claimed_by = $2 AND lease_expires_at > NOW()`

	if ok {
		tag, err := r.store.pool.Exec(ctx,
			fmt.Sprintf(`UPDATE %s
SET status = $3,
    sent_at = $4,
    claimed_by = NULL,
    claimed_at = NULL,
    lease_expires_at = NULL,
    last_error = NULL
%s`, table, ownership),
			outboxID, r.workerID, outboxStatusSent, time.Now().UTC(),
		)
		if err != nil {
			return fmt.Errorf("memory/postgres: ack success: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	}

	errMsg := ""
	if publishErr != nil {
		errMsg = publishErr.Error()
	}

	if attemptCount >= r.cfg.MaxAttempts {
		tag, err := r.store.pool.Exec(ctx,
			fmt.Sprintf(`UPDATE %s
SET status = $3,
    last_error = $4,
    claimed_by = NULL,
    claimed_at = NULL,
    lease_expires_at = NULL
%s`, table, ownership),
			outboxID, r.workerID, outboxStatusFailed, errMsg,
		)
		if err != nil {
			return fmt.Errorf("memory/postgres: ack failed-final: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return ErrLeaseLost
		}
		return nil
	}

	tag, err := r.store.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
SET status = $3,
    last_error = $4,
    claimed_by = NULL,
    claimed_at = NULL,
    lease_expires_at = NULL
%s`, table, ownership),
		outboxID, r.workerID, outboxStatusPending, errMsg,
	)
	if err != nil {
		return fmt.Errorf("memory/postgres: ack retry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Release clears every lease currently owned by this worker, flipping each
// owned row back to 'pending' so a peer (or the same worker on next start)
// can immediately re-claim it without waiting for the lease TTL to expire.
//
// By design Release does NOT decrement attempt_count — the claim that
// preceded the release still counts toward the retry budget. This is a
// deliberate spec choice (§4.7): graceful shutdown is not a free retry,
// because the publish may have been only partially completed at the
// downstream system.
//
// Release is intended for use in the gateway shutdown sequence (PreStop /
// terminationGracePeriodSeconds window). Errors are returned to the caller
// but production wiring treats them as best-effort and logs rather than
// propagates.
func (r *Relay) Release(ctx context.Context) error {
	_, err := r.store.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
SET status = $1,
    claimed_by = NULL,
    claimed_at = NULL,
    lease_expires_at = NULL
WHERE claimed_by = $2 AND status = $3`, r.store.outboxTable()),
		outboxStatusPending, r.workerID, outboxStatusProcessing,
	)
	if err != nil {
		return fmt.Errorf("memory/postgres: release relay leases: %w", err)
	}
	return nil
}

// MemoryPublisher is the in-memory Publisher fake used by tests.
type MemoryPublisher struct {
	Events []corememory.OutboxMessage
	Fail   bool
}

func (p *MemoryPublisher) Publish(ctx context.Context, evt corememory.OutboxMessage) error {
	_ = ctx
	if p.Fail {
		return ErrRelayPublishFailed
	}
	p.Events = append(p.Events, evt)
	return nil
}

type rowIter interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

var _ rowIter = (pgx.Rows)(nil)
