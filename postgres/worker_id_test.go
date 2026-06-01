package postgres

import (
	"errors"
	"regexp"
	"testing"
)

var workerIDFormat = regexp.MustCompile(`^[^-]+-[0-9a-f]{32}$`)

func TestNewRandomWorkerID_Format(t *testing.T) {
	id := NewRandomWorkerID()
	if !workerIDFormat.MatchString(id) {
		t.Fatalf("worker id %q does not match %s", id, workerIDFormat)
	}
}

func TestNewRandomWorkerID_UniqueAcrossCalls(t *testing.T) {
	const N = 100
	seen := make(map[string]struct{}, N)
	for i := 0; i < N; i++ {
		id := NewRandomWorkerID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate worker id at i=%d: %s", i, id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewRandomWorkerID_HostnameFailureFallback(t *testing.T) {
	prev := hostnameFn
	t.Cleanup(func() { hostnameFn = prev })
	hostnameFn = func() (string, error) { return "", errors.New("hostname unavailable") }

	id := NewRandomWorkerID()
	if !workerIDFormat.MatchString(id) {
		t.Fatalf("fallback worker id %q does not match format", id)
	}
	// Prefix must be "unknown" when hostname lookup fails.
	if id[:len("unknown-")] != "unknown-" {
		t.Fatalf("worker id %q missing 'unknown-' prefix", id)
	}
}
