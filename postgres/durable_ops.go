package postgres

import (
	"context"
	"fmt"
	"time"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
	"github.com/google/uuid"
)

var _ corememory.Promoter = (*Store)(nil)
var _ corememory.Deduper = (*Store)(nil)
var _ corememory.AccessMarker = (*Store)(nil)

// SessionWorkingRecord pairs a MemoryRecord with the event_id of the most
// recent event appended for that record.
type SessionWorkingRecord struct {
	Record        corememory.MemoryRecord
	LatestEventID string
}

// MarkAccess performs a single bulk UPDATE that increments hit_count by 1 and
// sets last_access_at to in.AccessedAt for every unique memory_id in in.MemoryIDs.
// Duplicate IDs are de-duplicated so they count as a single access.
func (s *Store) MarkAccess(ctx context.Context, in corememory.MarkAccessInput) error {
	if len(in.MemoryIDs) == 0 {
		return nil
	}
	// De-duplicate.
	seen := make(map[string]struct{}, len(in.MemoryIDs))
	unique := make([]string, 0, len(in.MemoryIDs))
	for _, id := range in.MemoryIDs {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			unique = append(unique, id)
		}
	}

	_, err := s.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE %s
SET last_access_at = $1,
    hit_count = hit_count + 1
WHERE tenant_id = $2 AND memory_id = ANY($3)`, s.memoryRecordTable()),
		in.AccessedAt, in.TenantID, unique,
	)
	if err != nil {
		return fmt.Errorf("memory/postgres: mark access: %w", err)
	}
	return nil
}

// Promote transitions a working memory record to episodic kind within a single
// transaction. It supports idempotency replay via IdempotencyKey.
func (s *Store) Promote(ctx context.Context, in corememory.PromoteRecordInput) (corememory.PromoteRecordResult, error) {
	if in.TenantID == "" || in.MemoryID == "" {
		return corememory.PromoteRecordResult{}, ErrNotFound
	}

	// Synthetic request hash for the promote idempotency entry.
	// Keyed on MemoryID + SourceEventID + ExpectedVersion so that a reused
	// IdempotencyKey with a different request is rejected as a conflict.
	syntheticHash := fmt.Sprintf("promote:%s:%s:%d", in.MemoryID, in.SourceEventID, in.ExpectedVersion)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: begin promote tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Idempotency replay.
	if in.IdempotencyKey != "" {
		var requestHash string
		var snapshotRaw []byte
		lookupErr := tx.QueryRow(ctx,
			fmt.Sprintf(`SELECT request_hash, response_snapshot FROM %s WHERE tenant_id = $1 AND idempotency_key = $2`,
				s.idempotencyTable()),
			in.TenantID, in.IdempotencyKey,
		).Scan(&requestHash, &snapshotRaw)
		if lookupErr != nil && !isNoRows(lookupErr) {
			return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: read promote idempotency: %w", lookupErr)
		}
		if lookupErr == nil {
			// Row found — replay.
			if requestHash != syntheticHash {
				return corememory.PromoteRecordResult{}, ErrIdempotencyConflict
			}
			var snap corememory.IdempotencyEntry
			if err := decodeJSON(snapshotRaw, &snap); err != nil {
				return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: decode promote idempotency snapshot: %w", err)
			}
			if err := tx.Commit(ctx); err != nil {
				return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: commit promote replay tx: %w", err)
			}
			r := snap.Response
			return corememory.PromoteRecordResult{
				MemoryID: r.MemoryID,
				Version:  r.Version,
				Record:   r.Record,
				Created:  false,
			}, nil
		}
	}

	// Load the current record (with lock).
	current, err := s.loadRecordForUpdate(ctx, tx, in.TenantID, in.MemoryID)
	if err != nil {
		return corememory.PromoteRecordResult{}, err
	}

	// Version guard.
	if in.ExpectedVersion <= 0 || current.Version != in.ExpectedVersion {
		return corememory.PromoteRecordResult{}, ErrVersionConflict
	}

	oldVersion := current.Version
	now := time.Now().UTC()
	current.Kind = corememory.RecordKindEpisodic
	current.ConsolidatedFromEventID = in.SourceEventID
	current.UpdatedAt = now
	current.Version++

	commandTag, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET
			kind = $1,
			consolidated_from_event_id = $2,
			version = $3,
			updated_at = $4
		WHERE tenant_id = $5 AND memory_id = $6 AND version = $7`,
			s.memoryRecordTable()),
		current.Kind, nullableString(current.ConsolidatedFromEventID),
		current.Version, current.UpdatedAt, in.TenantID, in.MemoryID, oldVersion,
	)
	if err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: update promote record: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return corememory.PromoteRecordResult{}, ErrVersionConflict
	}

	// Append event.
	eventID := uuid.NewString()
	eventPayload := corememory.StoredEvent{
		Version:   current.Version,
		TenantID:  current.TenantID,
		MemoryID:  current.MemoryID,
		EventType: eventTypeMemoryPromoted,
		Record:    current,
	}
	eventRaw, err := encodeJSON(eventPayload)
	if err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: encode promote event: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (event_id, memory_id, tenant_id, event_type, version, payload)
			VALUES ($1, $2, $3, $4, $5, $6)`, s.memoryEventTable()),
		eventID, current.MemoryID, current.TenantID, eventTypeMemoryPromoted, current.Version, eventRaw,
	); err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: insert promote event: %w", err)
	}

	// Append outbox.
	outboxPayload := corememory.OutboxMessage{
		Version:   current.Version,
		TenantID:  current.TenantID,
		MemoryID:  current.MemoryID,
		EventType: eventTypeMemoryPromoted,
		EventID:   eventID,
		Record:    current,
	}
	outboxRaw, err := encodeJSON(outboxPayload)
	if err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: encode promote outbox: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			outbox_id, aggregate_type, aggregate_id, tenant_id, event_id, event_type, payload, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, s.outboxTable()),
		uuid.NewString(), "memory", current.MemoryID, current.TenantID, eventID, eventTypeMemoryPromoted, outboxRaw, outboxStatusPending,
	); err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: insert promote outbox: %w", err)
	}

	// Save idempotency snapshot.
	if in.IdempotencyKey != "" {
		writeResult := corememory.WriteRecordResult{
			MemoryID: current.MemoryID,
			Version:  current.Version,
			Created:  true,
			Record:   current,
		}
		snapRaw, err := encodeJSON(corememory.IdempotencyEntry{
			TenantID:       in.TenantID,
			IdempotencyKey: in.IdempotencyKey,
			RequestHash:    syntheticHash,
			MemoryID:       current.MemoryID,
			Response:       writeResult,
			CreatedAt:      now,
		})
		if err != nil {
			return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: encode promote idempotency snapshot: %w", err)
		}
		if _, err := tx.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s (
				tenant_id, idempotency_key, request_hash, memory_id, response_snapshot
			) VALUES ($1, $2, $3, $4, $5)`, s.idempotencyTable()),
			in.TenantID, in.IdempotencyKey, syntheticHash, current.MemoryID, snapRaw,
		); err != nil {
			return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: insert promote idempotency: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return corememory.PromoteRecordResult{}, fmt.Errorf("memory/postgres: commit promote tx: %w", err)
	}
	return corememory.PromoteRecordResult{
		MemoryID: current.MemoryID,
		Version:  current.Version,
		Record:   current,
		Created:  true,
	}, nil
}

// ResolveDedupe checks whether a winner already exists for the given
// (tenant, dedupe_key) pair. If none exists, the candidate becomes the winner.
// If a winner already exists, the entire collision path (loser soft-delete +
// memory_dedupe_collapsed event + outbox) is executed atomically in one tx.
func (s *Store) ResolveDedupe(ctx context.Context, in corememory.ResolveDedupeInput) (corememory.ResolveDedupeResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: begin dedupe tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Check for an existing winner (FOR UPDATE to serialise concurrent dedupers).
	var winnerID string
	selectErr := tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT winner_memory_id FROM %s WHERE tenant_id = $1 AND dedupe_key = $2 FOR UPDATE`,
			s.dedupeIndexTable()),
		in.TenantID, in.DedupeKey,
	).Scan(&winnerID)

	if isNoRows(selectErr) {
		// First writer — register the candidate as winner.
		if _, err := tx.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s (tenant_id, dedupe_key, winner_memory_id) VALUES ($1, $2, $3)`,
				s.dedupeIndexTable()),
			in.TenantID, in.DedupeKey, in.Candidate.MemoryID,
		); err != nil {
			return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: insert dedupe index: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: commit dedupe first-writer tx: %w", err)
		}
		return corememory.ResolveDedupeResult{
			WinnerID: in.Candidate.MemoryID,
			Action:   corememory.DedupeNoCollision,
		}, nil
	}
	if selectErr != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: query dedupe index: %w", selectErr)
	}

	// Collision: everything below runs in the same tx for atomicity.

	// (a) Soft-delete the loser within this tx; emits memory_deleted event + outbox.
	if _, err := s.mutateRecordTx(ctx, tx, mutationInput{
		tenantID:        in.TenantID,
		memoryID:        in.Candidate.MemoryID,
		expectedVersion: in.Candidate.Version,
		eventType:       eventTypeMemoryDeleted,
		apply: func(r *corememory.MemoryRecord, now time.Time) {
			r.Deleted = true
			r.DeletedAt = &now
			r.UpdatedAt = now
		},
	}); err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: dedupe delete loser: %w", err)
	}

	// (b) Load the winner for its Pinned flag.
	winner, err := s.loadRecordForUpdate(ctx, tx, in.TenantID, winnerID)
	if err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: load dedupe winner: %w", err)
	}

	// (c) Emit memory_dedupe_collapsed event + outbox, attributed to the winner.
	collapseMeta := map[string]any{
		corememory.DedupeCollapsedLoserIDMetadataKey: in.Candidate.MemoryID,
	}
	collapseEventID := uuid.NewString()
	collapseEventPayload := corememory.StoredEvent{
		Version:   winner.Version,
		TenantID:  winner.TenantID,
		MemoryID:  winner.MemoryID,
		EventType: eventTypeMemoryDedupeCollapsed,
		Record:    winner,
		Metadata:  collapseMeta,
	}
	collapseEventRaw, err := encodeJSON(collapseEventPayload)
	if err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: encode dedupe collapse event: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (event_id, memory_id, tenant_id, event_type, version, payload)
			VALUES ($1, $2, $3, $4, $5, $6)`, s.memoryEventTable()),
		collapseEventID, winner.MemoryID, winner.TenantID, eventTypeMemoryDedupeCollapsed, winner.Version, collapseEventRaw,
	); err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: insert dedupe collapse event: %w", err)
	}

	collapseOutboxPayload := corememory.OutboxMessage{
		Version:   winner.Version,
		TenantID:  winner.TenantID,
		MemoryID:  winner.MemoryID,
		EventType: eventTypeMemoryDedupeCollapsed,
		EventID:   collapseEventID,
		Record:    winner,
		Metadata:  collapseMeta,
	}
	collapseOutboxRaw, err := encodeJSON(collapseOutboxPayload)
	if err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: encode dedupe collapse outbox: %w", err)
	}
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s (
			outbox_id, aggregate_type, aggregate_id, tenant_id, event_id, event_type, payload, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`, s.outboxTable()),
		uuid.NewString(), "memory", winner.MemoryID, winner.TenantID, collapseEventID, eventTypeMemoryDedupeCollapsed, collapseOutboxRaw, outboxStatusPending,
	); err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: insert dedupe collapse outbox: %w", err)
	}

	// (d) Commit all collision writes atomically.
	if err := tx.Commit(ctx); err != nil {
		return corememory.ResolveDedupeResult{}, fmt.Errorf("memory/postgres: commit dedupe collision tx: %w", err)
	}

	// (e) Determine action from winner's Pinned flag.
	action := corememory.DedupeMergedExisting
	if winner.Pinned {
		action = corememory.DedupeCollapsedByPin
	}
	return corememory.ResolveDedupeResult{
		WinnerID: winnerID,
		Action:   action,
	}, nil
}

// ListSessionWorking returns all non-deleted, non-disabled working-kind records
// that match the given scope (tenant, user, project, session), along with the
// event_id of the most recently appended event for each record.
func (s *Store) ListSessionWorking(ctx context.Context, tenantID, userID, projectID, sessionID string) ([]SessionWorkingRecord, error) {
	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`SELECT
			r.memory_id, r.tenant_id, r.user_id, r.project_id, r.session_id, r.kind, r.source, r.category, r.content,
			r.normalized_content_hash, r.tags, r.importance, r.pinned, r.disabled, r.deleted, r.version, r.created_at, r.updated_at,
			r.deleted_at, r.last_access_at, r.hit_count, r.consolidated_from_event_id,
			(SELECT e.event_id FROM %s e WHERE e.memory_id = r.memory_id ORDER BY e.version DESC, e.created_at DESC LIMIT 1) AS latest_event_id
		FROM %s r
		WHERE r.tenant_id = $1 AND r.user_id = $2 AND r.project_id = $3 AND r.session_id = $4
		  AND r.kind = $5 AND r.deleted = FALSE AND r.disabled = FALSE
		ORDER BY r.created_at DESC`,
			s.memoryEventTable(), s.memoryRecordTable()),
		tenantID, userID, projectID, sessionID, corememory.RecordKindWorking,
	)
	if err != nil {
		return nil, fmt.Errorf("memory/postgres: list session working: %w", err)
	}
	defer rows.Close()

	var out []SessionWorkingRecord
	for rows.Next() {
		var rec corememory.MemoryRecord
		var latestEventID, projectID, sessionIDVal, consolidatedFromEventID *string
		if err := rows.Scan(
			&rec.MemoryID, &rec.TenantID, &rec.UserID, &projectID, &sessionIDVal,
			&rec.Kind, &rec.Source, &rec.Category, &rec.Content, &rec.NormalizedContentHash,
			&rec.Tags, &rec.Importance, &rec.Pinned, &rec.Disabled, &rec.Deleted, &rec.Version,
			&rec.CreatedAt, &rec.UpdatedAt, &rec.DeletedAt, &rec.LastAccessAt, &rec.HitCount,
			&consolidatedFromEventID, &latestEventID,
		); err != nil {
			return nil, fmt.Errorf("memory/postgres: list session working scan: %w", err)
		}
		rec.ProjectID = derefString(projectID)
		rec.SessionID = derefString(sessionIDVal)
		rec.ConsolidatedFromEventID = derefString(consolidatedFromEventID)
		swr := SessionWorkingRecord{Record: rec}
		if latestEventID != nil {
			swr.LatestEventID = *latestEventID
		}
		out = append(out, swr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("memory/postgres: list session working rows: %w", err)
	}
	return out, nil
}
