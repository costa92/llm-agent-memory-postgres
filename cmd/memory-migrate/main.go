package main

import (
	"context"
	"log"
	"os"

	"github.com/costa92/llm-agent-memory-postgres/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := os.Getenv("LLM_AGENT_MEMORY_PG_URL")
	if dsn == "" {
		log.Fatal("LLM_AGENT_MEMORY_PG_URL is required")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("open pgx pool: %v", err)
	}
	defer pool.Close()

	store, err := postgres.New(pool, postgres.Config{})
	if err != nil {
		log.Fatalf("construct postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		log.Fatalf("run migrations: %v", err)
	}
}
