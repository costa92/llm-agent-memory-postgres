// Package memorypostgres provides the Postgres durable-storage backend module
// for github.com/costa92/llm-agent-memory.
//
// This module is intentionally separate from the SDK so concrete schema,
// relay, and migration behavior do not pollute the core memory package's
// release boundary.
package memorypostgres
