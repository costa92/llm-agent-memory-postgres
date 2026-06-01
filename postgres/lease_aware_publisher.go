package postgres

import (
	"context"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

// LeaseAwarePublisher is a test fake that lets tests inject arbitrary
// delay or error behaviour into the Publisher contract. Unlike
// MemoryPublisher, it exposes a PublishHook so a test can sleep past the
// configured LeaseTTL and assert that the relay's Ack returns
// ErrLeaseLost (the "publish took longer than the lease" scenario).
//
//   - PublishHook == nil  → behaves like MemoryPublisher (append to Events).
//   - PublishHook != nil  → invokes the hook; on hook error, returns the
//     error WITHOUT appending to Events, so the test can distinguish
//     "publish ran but raced the lease" from "publish never happened".
//
// Concurrent access is the caller's responsibility — the relay drives one
// publish at a time from RunOnce, so the unsynchronized append is safe in
// the documented usage.
type LeaseAwarePublisher struct {
	Events      []corememory.OutboxMessage
	PublishHook func(ctx context.Context, msg corememory.OutboxMessage) error
}

func (p *LeaseAwarePublisher) Publish(ctx context.Context, evt corememory.OutboxMessage) error {
	if p.PublishHook != nil {
		if err := p.PublishHook(ctx, evt); err != nil {
			return err
		}
	}
	p.Events = append(p.Events, evt)
	return nil
}
