package postgres

import (
	"context"
	"errors"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

// FanoutPublisher forwards the same outbox message to multiple downstream
// publishers in-process. It preserves at-least-once semantics by invoking
// every child even if an earlier child failed, then returns the joined error.
type FanoutPublisher struct {
	children []Publisher
}

func NewFanoutPublisher(children ...Publisher) *FanoutPublisher {
	filtered := make([]Publisher, 0, len(children))
	for _, child := range children {
		if child != nil {
			filtered = append(filtered, child)
		}
	}
	return &FanoutPublisher{children: filtered}
}

func (p *FanoutPublisher) Publish(ctx context.Context, evt corememory.OutboxMessage) error {
	if p == nil {
		return nil
	}
	var errs []error
	for _, child := range p.children {
		if err := child.Publish(ctx, evt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
