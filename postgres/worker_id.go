package postgres

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
)

// hostnameFn is a package-level indirection so tests can simulate hostname
// lookup failures without poking at the OS. Default = os.Hostname.
var hostnameFn = os.Hostname

// NewRandomWorkerID returns a relay worker identity of the form
// "<hostname>-<32-hex-char>" where the hex suffix is 128 bits of crypto-grade
// randomness. On os.Hostname() failure (any error or empty string), the
// hostname is substituted with the literal "unknown" so the identifier
// remains well-formed.
//
// Workers MUST regenerate this on every process start — it is intentionally
// non-stable across restarts so a crashed pod's lease can be reclaimed by
// time rather than identity.
func NewRandomWorkerID() string {
	name, err := hostnameFn()
	if err != nil || name == "" {
		name = "unknown"
	}
	var buf [16]byte
	// crypto/rand.Read on Linux reads from getrandom(2) and cannot fail in
	// practice; we ignore the error for the same reason the stdlib examples
	// do. A zero-buffer worst case still satisfies the format contract.
	_, _ = rand.Read(buf[:])
	return fmt.Sprintf("%s-%s", name, hex.EncodeToString(buf[:]))
}
