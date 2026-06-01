package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	eventTypeMemoryCreated         = "memory_created"
	eventTypeMemoryUpdated         = "memory_updated"
	eventTypeMemoryDeleted         = "memory_deleted"
	eventTypeMemoryPinned          = "memory_pinned"
	eventTypeMemoryUnpinned        = "memory_unpinned"
	eventTypeMemoryDisabled        = "memory_disabled"
	eventTypeMemoryEnabled         = "memory_enabled"
	eventTypeMemoryPromoted        = "memory_promoted"
	eventTypeMemoryDedupeCollapsed = "memory_dedupe_collapsed"
	outboxStatusPending            = "pending"
	outboxStatusProcessing         = "processing"
	outboxStatusSent               = "sent"
	outboxStatusFailed             = "failed"
)

// allowedEventTypes is the write-side allowlist of event-type strings the
// memory outbox / event log will accept. Centralized here so AppendEvent,
// EnqueueOutbox, and mutateRecord share a single source of truth — and so
// typos at call sites are rejected at write time instead of leaking into
// downstream consumers.
var allowedEventTypes = map[string]struct{}{
	eventTypeMemoryCreated:         {},
	eventTypeMemoryUpdated:         {},
	eventTypeMemoryDeleted:         {},
	eventTypeMemoryPinned:          {},
	eventTypeMemoryUnpinned:        {},
	eventTypeMemoryDisabled:        {},
	eventTypeMemoryEnabled:         {},
	eventTypeMemoryPromoted:        {},
	eventTypeMemoryDedupeCollapsed: {},
}

func validateEventType(eventType string) error {
	if _, ok := allowedEventTypes[eventType]; !ok {
		return fmt.Errorf("%w: %q", ErrInvalidEventType, eventType)
	}
	return nil
}

var _ corememory.RecordStore = (*Store)(nil)
var _ corememory.EventStore = (*Store)(nil)
var _ corememory.IdempotencyStore = (*Store)(nil)
var _ corememory.Outbox = (*Store)(nil)

func (s *Store) WriteRecord(ctx context.Context, in corememory.WriteRecordInput) (corememory.WriteRecordResult, error) {
	if in.TenantID == "" {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: tenant_id is required")
	}
	if in.IdempotencyKey == "" {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: idempotency_key is required")
	}
	if in.RequestHash == "" {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: request_hash is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if replay, err := s.lookupIdempotentWrite(ctx, tx, in); err != nil {
		return corememory.WriteRecordResult{}, err
	} else if replay != nil {
		if err := tx.Commit(ctx); err != nil {
			return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: commit replay tx: %w", err)
		}
		return *replay, nil
	}

	now := time.Now().UTC()
	record := in.Record
	record.TenantID = in.TenantID
	if record.MemoryID == "" {
		record.MemoryID = uuid.NewString()
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.UpdatedAt = now
	record.Version = 1

	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			memory_id, tenant_id, user_id, project_id, session_id, kind, source, category, content,
			normalized_content_hash, tags, importance, pinned, disabled, deleted, version, created_at, updated_at,
			deleted_at, last_access_at, hit_count, consolidated_from_event_id
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22
		)`, s.memoryRecordTable()),
		record.MemoryID, record.TenantID, record.UserID, nullableString(record.ProjectID), nullableString(record.SessionID),
		record.Kind, record.Source, record.Category, record.Content, record.NormalizedContentHash,
		nonNilTags(record.Tags), record.Importance, record.Pinned, record.Disabled, record.Deleted, record.Version,
		record.CreatedAt, record.UpdatedAt, record.DeletedAt, record.LastAccessAt, record.HitCount,
		nullableString(record.ConsolidatedFromEventID),
	); err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: insert memory_record: %w", err)
	}

	eventID := uuid.NewString()
	eventPayload := corememory.StoredEvent{
		Version:        record.Version,
		TenantID:       record.TenantID,
		MemoryID:       record.MemoryID,
		EventType:      eventTypeMemoryCreated,
		IdempotencyKey: in.IdempotencyKey,
		Record:         record,
	}
	eventRaw, err := encodeJSON(eventPayload)
	if err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: encode event payload: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (event_id, memory_id, tenant_id, event_type, version, idempotency_key, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, s.memoryEventTable()),
		eventID, record.MemoryID, record.TenantID, eventTypeMemoryCreated, record.Version, in.IdempotencyKey, eventRaw,
	); err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: insert memory_event: %w", err)
	}

	outboxPayload := corememory.OutboxMessage{
		Version:   record.Version,
		TenantID:  record.TenantID,
		MemoryID:  record.MemoryID,
		EventType: eventTypeMemoryCreated,
		EventID:   eventID,
		Record:    record,
	}
	outboxRaw, err := encodeJSON(outboxPayload)
	if err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: encode outbox payload: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			outbox_id, aggregate_type, aggregate_id, tenant_id, event_id, event_type, payload, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, s.outboxTable()),
		uuid.NewString(), "memory", record.MemoryID, record.TenantID, eventID, eventTypeMemoryCreated, outboxRaw, outboxStatusPending,
	); err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: insert outbox_event: %w", err)
	}

	result := corememory.WriteRecordResult{
		MemoryID: record.MemoryID,
		Version:  record.Version,
		Created:  true,
		Record:   record,
	}
	responseRaw, err := encodeJSON(corememory.IdempotencyEntry{
		TenantID:       in.TenantID,
		IdempotencyKey: in.IdempotencyKey,
		RequestHash:    in.RequestHash,
		MemoryID:       record.MemoryID,
		Response:       result,
		CreatedAt:      now,
	})
	if err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: encode idempotency snapshot: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			tenant_id, idempotency_key, request_hash, memory_id, response_snapshot
		) VALUES ($1, $2, $3, $4, $5)`, s.idempotencyTable()),
		in.TenantID, in.IdempotencyKey, in.RequestHash, record.MemoryID, responseRaw,
	); err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: insert idempotency: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return corememory.WriteRecordResult{}, fmt.Errorf("memory/postgres: commit tx: %w", err)
	}
	return result, nil
}

func (s *Store) PatchRecord(ctx context.Context, in corememory.PatchRecordInput) (corememory.PatchRecordResult, error) {
	record, err := s.mutateRecord(ctx, mutationInput{
		tenantID:        in.TenantID,
		memoryID:        in.MemoryID,
		expectedVersion: in.ExpectedVersion,
		eventType:       eventTypeMemoryUpdated,
		apply: func(r *corememory.MemoryRecord, now time.Time) {
			if in.Content != "" {
				r.Content = in.Content
			}
			if in.Category != "" {
				r.Category = in.Category
			}
			if in.Tags != nil {
				r.Tags = in.Tags
			}
			if in.Importance != 0 {
				r.Importance = in.Importance
			}
			r.UpdatedAt = now
		},
	})
	if err != nil {
		return corememory.PatchRecordResult{}, err
	}
	return corememory.PatchRecordResult{MemoryID: record.MemoryID, Version: record.Version, Record: record}, nil
}

func (s *Store) DeleteRecord(ctx context.Context, in corememory.DeleteRecordInput) (corememory.DeleteRecordResult, error) {
	record, err := s.mutateRecord(ctx, mutationInput{
		tenantID:        in.TenantID,
		memoryID:        in.MemoryID,
		expectedVersion: in.ExpectedVersion,
		eventType:       eventTypeMemoryDeleted,
		apply: func(r *corememory.MemoryRecord, now time.Time) {
			r.Deleted = true
			r.DeletedAt = &now
			r.UpdatedAt = now
		},
	})
	if err != nil {
		return corememory.DeleteRecordResult{}, err
	}
	return corememory.DeleteRecordResult{MemoryID: record.MemoryID, Version: record.Version, Record: record}, nil
}

func (s *Store) PinRecord(ctx context.Context, in corememory.PinRecordInput) (corememory.PinRecordResult, error) {
	eventType := eventTypeMemoryPinned
	if !in.Pinned {
		eventType = eventTypeMemoryUnpinned
	}
	record, err := s.mutateRecord(ctx, mutationInput{
		tenantID:        in.TenantID,
		memoryID:        in.MemoryID,
		expectedVersion: in.ExpectedVersion,
		eventType:       eventType,
		apply: func(r *corememory.MemoryRecord, now time.Time) {
			r.Pinned = in.Pinned
			r.UpdatedAt = now
		},
	})
	if err != nil {
		return corememory.PinRecordResult{}, err
	}
	return corememory.PinRecordResult{MemoryID: record.MemoryID, Version: record.Version, Record: record}, nil
}

func (s *Store) DisableRecord(ctx context.Context, in corememory.DisableRecordInput) (corememory.DisableRecordResult, error) {
	eventType := eventTypeMemoryDisabled
	if !in.Disabled {
		eventType = eventTypeMemoryEnabled
	}
	record, err := s.mutateRecord(ctx, mutationInput{
		tenantID:        in.TenantID,
		memoryID:        in.MemoryID,
		expectedVersion: in.ExpectedVersion,
		eventType:       eventType,
		apply: func(r *corememory.MemoryRecord, now time.Time) {
			r.Disabled = in.Disabled
			r.UpdatedAt = now
		},
	})
	if err != nil {
		return corememory.DisableRecordResult{}, err
	}
	return corememory.DisableRecordResult{MemoryID: record.MemoryID, Version: record.Version, Record: record}, nil
}

type mutationInput struct {
	tenantID        string
	memoryID        string
	expectedVersion int64
	eventType       string
	apply           func(*corememory.MemoryRecord, time.Time)
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) GetRecord(ctx context.Context, tenantID, memoryID string) (corememory.MemoryRecord, error) {
	record, err := s.loadRecordForUpdate(ctx, s.pool, tenantID, memoryID)
	if err != nil {
		return corememory.MemoryRecord{}, err
	}
	if record.Deleted || record.Disabled {
		return corememory.MemoryRecord{}, ErrNotFound
	}
	return record, nil
}

func (s *Store) lookupIdempotentWrite(ctx context.Context, q queryRower, in corememory.WriteRecordInput) (*corememory.WriteRecordResult, error) {
	var requestHash string
	var snapshotRaw []byte
	err := q.QueryRow(ctx,
		fmt.Sprintf(`SELECT request_hash, response_snapshot FROM %s WHERE tenant_id = $1 AND idempotency_key = $2`,
			s.idempotencyTable(),
		),
		in.TenantID, in.IdempotencyKey,
	).Scan(&requestHash, &snapshotRaw)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("memory/postgres: read idempotency: %w", err)
	}
	if requestHash != in.RequestHash {
		return nil, ErrIdempotencyConflict
	}

	var snap corememory.IdempotencyEntry
	if err := decodeJSON(snapshotRaw, &snap); err != nil {
		return nil, fmt.Errorf("memory/postgres: decode idempotency snapshot: %w", err)
	}
	return &snap.Response, nil
}

func (s *Store) AppendEvent(ctx context.Context, event corememory.StoredEvent) error {
	if err := validateEventType(event.EventType); err != nil {
		return err
	}
	raw, err := encodeJSON(event)
	if err != nil {
		return fmt.Errorf("memory/postgres: encode event payload: %w", err)
	}

	eventID := uuid.NewString()
	_, err = s.pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (event_id, memory_id, tenant_id, event_type, version, idempotency_key, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, s.memoryEventTable()),
		eventID, event.MemoryID, event.TenantID, event.EventType, event.Version, nullableString(event.IdempotencyKey), raw,
	)
	if err != nil {
		return fmt.Errorf("memory/postgres: append event: %w", err)
	}
	return nil
}

func (s *Store) LoadIdempotency(ctx context.Context, tenantID, idempotencyKey string) (corememory.IdempotencyEntry, error) {
	var snapshotRaw []byte
	err := s.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT response_snapshot FROM %s WHERE tenant_id = $1 AND idempotency_key = $2`, s.idempotencyTable()),
		tenantID, idempotencyKey,
	).Scan(&snapshotRaw)
	if err != nil {
		if isNoRows(err) {
			return corememory.IdempotencyEntry{}, ErrNotFound
		}
		return corememory.IdempotencyEntry{}, fmt.Errorf("memory/postgres: load idempotency: %w", err)
	}

	var entry corememory.IdempotencyEntry
	if err := decodeJSON(snapshotRaw, &entry); err != nil {
		return corememory.IdempotencyEntry{}, fmt.Errorf("memory/postgres: decode idempotency snapshot: %w", err)
	}
	return entry, nil
}

func (s *Store) SaveIdempotency(ctx context.Context, entry corememory.IdempotencyEntry) error {
	raw, err := encodeJSON(entry)
	if err != nil {
		return fmt.Errorf("memory/postgres: encode idempotency snapshot: %w", err)
	}

	_, err = s.pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			tenant_id, idempotency_key, request_hash, memory_id, response_snapshot, created_at, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, idempotency_key) DO UPDATE SET
			request_hash = EXCLUDED.request_hash,
			memory_id = EXCLUDED.memory_id,
			response_snapshot = EXCLUDED.response_snapshot,
			created_at = EXCLUDED.created_at,
			expires_at = EXCLUDED.expires_at`, s.idempotencyTable()),
		entry.TenantID, entry.IdempotencyKey, entry.RequestHash, nullableString(entry.MemoryID), raw, entry.CreatedAt, entry.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("memory/postgres: save idempotency: %w", err)
	}
	return nil
}

// RequeueResult summarises the outcome of a RequeueFailed call. A
// RowsAffected of zero is the documented no-op signal — the row either
// never existed or is not currently in the 'failed' state, so operators
// can distinguish "I requeued one row" from "that ID was a typo or
// already-pending" without an extra read.
type RequeueResult struct {
	RowsAffected int64
}

// FailedOutboxRow is the operator view of a single 'failed' outbox row.
// Field semantics match the corresponding column on the outbox table; we
// expose the AggregateID rather than the EventID because operators
// typically need the memory_id to correlate with application logs.
type FailedOutboxRow struct {
	OutboxID     string
	AggregateID  string
	EventType    string
	AttemptCount int
	LastError    string
	CreatedAt    time.Time
}

// RequeueFailed flips a single 'failed' outbox row back to 'pending' and
// resets attempt_count to 0 so the relay treats it as a fresh delivery
// attempt. last_error is cleared and lease columns are nulled defensively
// (they should already be NULL on a failed row, but we don't trust the
// world).
//
// Returns RowsAffected = 1 on success, 0 if the row is not in the 'failed'
// state (or doesn't exist). The 0-case is intentionally not an error —
// operator tooling typically wants to distinguish "found nothing to do"
// from "request was malformed".
//
// No audit row is written by this method; operators who need provenance
// can query memory_event history alongside the outbox row.
func (s *Store) RequeueFailed(ctx context.Context, outboxID string) (RequeueResult, error) {
	tag, err := s.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
			SET status = $1,
			    attempt_count = 0,
			    last_error = NULL,
			    claimed_by = NULL,
			    claimed_at = NULL,
			    lease_expires_at = NULL
			WHERE outbox_id = $2 AND status = $3`, s.outboxTable()),
		outboxStatusPending, outboxID, outboxStatusFailed,
	)
	if err != nil {
		return RequeueResult{}, fmt.Errorf("memory/postgres: requeue failed: %w", err)
	}
	return RequeueResult{RowsAffected: tag.RowsAffected()}, nil
}

// ListFailed returns up to limit 'failed' outbox rows, newest first. The
// ordering is by created_at DESC so a paginated operator tool surfaces
// recently-failed work first. last_error and attempt_count are populated
// from the columns directly; null last_error becomes "".
func (s *Store) ListFailed(ctx context.Context, limit int) ([]FailedOutboxRow, error) {
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`SELECT outbox_id, aggregate_id, event_type, attempt_count, last_error, created_at
FROM %s
WHERE status = $1
ORDER BY created_at DESC
LIMIT $2`, s.outboxTable()),
		outboxStatusFailed, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("memory/postgres: list failed outbox: %w", err)
	}
	defer rows.Close()

	var out []FailedOutboxRow
	for rows.Next() {
		var row FailedOutboxRow
		var lastError *string
		if err := rows.Scan(&row.OutboxID, &row.AggregateID, &row.EventType, &row.AttemptCount, &lastError, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("memory/postgres: list failed scan: %w", err)
		}
		if lastError != nil {
			row.LastError = *lastError
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory/postgres: list failed rows: %w", err)
	}
	return out, nil
}

func (s *Store) EnqueueOutbox(ctx context.Context, msg corememory.OutboxMessage) error {
	if err := validateEventType(msg.EventType); err != nil {
		return err
	}
	raw, err := encodeJSON(msg)
	if err != nil {
		return fmt.Errorf("memory/postgres: encode outbox payload: %w", err)
	}

	_, err = s.pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			outbox_id, aggregate_type, aggregate_id, tenant_id, event_id, event_type, payload, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, s.outboxTable()),
		uuid.NewString(), "memory", msg.MemoryID, msg.TenantID, msg.EventID, msg.EventType, raw, outboxStatusPending,
	)
	if err != nil {
		return fmt.Errorf("memory/postgres: enqueue outbox: %w", err)
	}
	return nil
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// derefString returns the pointed-to string, or "" when the pointer is nil.
// It is the read-side counterpart of nullableString: nullable TEXT columns are
// scanned into *string and collapsed back to the zero value here.
func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// nonNilTags ensures a nil slice is written as an empty JSON array rather than
// SQL NULL, satisfying the tags JSONB NOT NULL column.
func nonNilTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	return tags
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func (s *Store) mutateRecord(ctx context.Context, in mutationInput) (corememory.MemoryRecord, error) {
	if err := validateEventType(in.eventType); err != nil {
		return corememory.MemoryRecord{}, err
	}
	if in.tenantID == "" || in.memoryID == "" {
		return corememory.MemoryRecord{}, ErrNotFound
	}
	if in.expectedVersion <= 0 {
		return corememory.MemoryRecord{}, ErrVersionConflict
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: begin mutation tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rec, err := s.mutateRecordTx(ctx, tx, in)
	if err != nil {
		return corememory.MemoryRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: commit mutation tx: %w", err)
	}
	return rec, nil
}

// mutateRecordTx is the tx-aware core of mutateRecord. It performs the
// load-for-update, version check, UPDATE, event INSERT, and outbox INSERT
// inside the provided tx. The caller owns Begin/Commit/Rollback.
func (s *Store) mutateRecordTx(ctx context.Context, tx pgx.Tx, in mutationInput) (corememory.MemoryRecord, error) {
	current, err := s.loadRecordForUpdate(ctx, tx, in.tenantID, in.memoryID)
	if err != nil {
		return corememory.MemoryRecord{}, err
	}
	if current.Version != in.expectedVersion {
		return corememory.MemoryRecord{}, ErrVersionConflict
	}

	now := time.Now().UTC()
	in.apply(&current, now)
	current.Version++

	commandTag, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET
			category = $1,
			content = $2,
			tags = $3,
			importance = $4,
			pinned = $5,
			disabled = $6,
			deleted = $7,
			version = $8,
			updated_at = $9,
			deleted_at = $10
		WHERE tenant_id = $11 AND memory_id = $12 AND version = $13`,
			s.memoryRecordTable(),
		),
		current.Category, current.Content, current.Tags, current.Importance, current.Pinned, current.Disabled,
		current.Deleted, current.Version, current.UpdatedAt, current.DeletedAt, in.tenantID, in.memoryID, in.expectedVersion,
	)
	if err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: update memory_record: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return corememory.MemoryRecord{}, ErrVersionConflict
	}

	eventID := uuid.NewString()
	eventPayload := corememory.StoredEvent{
		Version:   current.Version,
		TenantID:  current.TenantID,
		MemoryID:  current.MemoryID,
		EventType: in.eventType,
		Record:    current,
	}
	eventRaw, err := encodeJSON(eventPayload)
	if err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: encode mutation event: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (event_id, memory_id, tenant_id, event_type, version, payload)
			VALUES ($1, $2, $3, $4, $5, $6)`, s.memoryEventTable()),
		eventID, current.MemoryID, current.TenantID, in.eventType, current.Version, eventRaw,
	); err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: insert mutation event: %w", err)
	}

	outboxPayload := corememory.OutboxMessage{
		Version:   current.Version,
		TenantID:  current.TenantID,
		MemoryID:  current.MemoryID,
		EventType: in.eventType,
		EventID:   eventID,
		Record:    current,
	}
	outboxRaw, err := encodeJSON(outboxPayload)
	if err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: encode mutation outbox: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			outbox_id, aggregate_type, aggregate_id, tenant_id, event_id, event_type, payload, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, s.outboxTable()),
		uuid.NewString(), "memory", current.MemoryID, current.TenantID, eventID, in.eventType, outboxRaw, outboxStatusPending,
	); err != nil {
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: insert mutation outbox: %w", err)
	}

	return current, nil
}

func (s *Store) loadRecordForUpdate(ctx context.Context, q queryRower, tenantID, memoryID string) (corememory.MemoryRecord, error) {
	var record corememory.MemoryRecord
	var projectID, sessionID, consolidatedFromEventID *string
	err := q.QueryRow(ctx,
		fmt.Sprintf(`SELECT
			memory_id, tenant_id, user_id, project_id, session_id, kind, source, category, content,
			normalized_content_hash, tags, importance, pinned, disabled, deleted, version, created_at, updated_at,
			deleted_at, last_access_at, hit_count, consolidated_from_event_id
		FROM %s
		WHERE tenant_id = $1 AND memory_id = $2`, s.memoryRecordTable()),
		tenantID, memoryID,
	).Scan(
		&record.MemoryID, &record.TenantID, &record.UserID, &projectID, &sessionID,
		&record.Kind, &record.Source, &record.Category, &record.Content, &record.NormalizedContentHash,
		&record.Tags, &record.Importance, &record.Pinned, &record.Disabled, &record.Deleted, &record.Version,
		&record.CreatedAt, &record.UpdatedAt, &record.DeletedAt, &record.LastAccessAt, &record.HitCount,
		&consolidatedFromEventID,
	)
	if err != nil {
		if isNoRows(err) {
			return corememory.MemoryRecord{}, ErrNotFound
		}
		return corememory.MemoryRecord{}, fmt.Errorf("memory/postgres: load record: %w", err)
	}
	record.ProjectID = derefString(projectID)
	record.SessionID = derefString(sessionID)
	record.ConsolidatedFromEventID = derefString(consolidatedFromEventID)
	return record, nil
}
