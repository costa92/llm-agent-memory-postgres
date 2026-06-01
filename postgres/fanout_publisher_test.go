package postgres

import (
	"context"
	"errors"
	"testing"

	corememory "github.com/costa92/llm-agent-memory-contract/contract"
)

type stubPublisher struct {
	events []corememory.OutboxMessage
	err    error
}

func (p *stubPublisher) Publish(_ context.Context, evt corememory.OutboxMessage) error {
	if p.err != nil {
		return p.err
	}
	p.events = append(p.events, evt)
	return nil
}

func TestFanoutPublisher_PublishesToAllChildren(t *testing.T) {
	first := &stubPublisher{}
	second := &stubPublisher{}
	publisher := NewFanoutPublisher(first, second)

	msg := corememory.OutboxMessage{
		EventType: "memory_created",
		MemoryID:  "mem_1",
		TenantID:  "tenant-a",
		Version:   1,
	}
	if err := publisher.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	if len(first.events) != 1 {
		t.Fatalf("first events = %d, want 1", len(first.events))
	}
	if len(second.events) != 1 {
		t.Fatalf("second events = %d, want 1", len(second.events))
	}
}

func TestFanoutPublisher_ContinuesAfterChildFailureAndReturnsJoinedError(t *testing.T) {
	wantErr := errors.New("child failed")
	first := &stubPublisher{err: wantErr}
	second := &stubPublisher{}
	publisher := NewFanoutPublisher(first, second)

	err := publisher.Publish(context.Background(), corememory.OutboxMessage{
		EventType: "memory_created",
		MemoryID:  "mem_1",
		TenantID:  "tenant-a",
		Version:   1,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want wrapped %v", err, wantErr)
	}
	if len(second.events) != 1 {
		t.Fatalf("second events = %d, want 1", len(second.events))
	}
}

func TestNewFanoutPublisher_SkipsNilChildren(t *testing.T) {
	publisher := NewFanoutPublisher(nil, &stubPublisher{}, nil)
	if publisher == nil {
		t.Fatal("publisher is nil")
	}
}
