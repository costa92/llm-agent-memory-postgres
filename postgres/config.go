package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	TablePrefix string
}

type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

func New(pool *pgxpool.Pool, cfg Config) (*Store, error) {
	if pool == nil {
		return nil, errors.New("memory/postgres: pool is required")
	}
	if cfg.TablePrefix != "" && !isSafeIdent(cfg.TablePrefix) {
		return nil, fmt.Errorf("memory/postgres: invalid table prefix %q", cfg.TablePrefix)
	}
	return &Store{pool: pool, cfg: cfg}, nil
}

func (s *Store) schemaVersionTable() string {
	return s.tableName("schema_version")
}

func (s *Store) memoryRecordTable() string {
	return s.tableName("memory_record")
}

func (s *Store) memoryEventTable() string {
	return s.tableName("memory_event")
}

func (s *Store) idempotencyTable() string {
	return s.tableName("memory_idempotency")
}

func (s *Store) outboxTable() string {
	return s.tableName("outbox_event")
}

func (s *Store) memoryDecisionTraceTable() string {
	return s.tableName("memory_decision_trace")
}

func (s *Store) dedupeIndexTable() string {
	return s.tableName("memory_dedupe_index")
}

func (s *Store) tableName(base string) string {
	if s.cfg.TablePrefix == "" {
		return base
	}
	return s.cfg.TablePrefix + "_" + base
}

func isSafeIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return false
		}
	}
	return !strings.HasPrefix(s, "_")
}
