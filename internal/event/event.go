package event

import (
	"errors"
	"sync"
	"time"
)

type EventType string

const (
	EventCreated   EventType = "execution.created"
	EventStarted   EventType = "execution.started"
	EventCompleted EventType = "execution.completed"
	EventFailed    EventType = "execution.failed"
	EventCancelled EventType = "execution.cancelled"
)

type Event struct {
	Sequence    uint64    `json:"sequence"`
	ExecutionID string    `json:"execution_id"`
	Type        EventType `json:"type"`
	Timestamp   time.Time `json:"timestamp"`
}

type Publisher interface {
	Publish(Event) error
}

type NoopPublisher struct{}

func (NoopPublisher) Publish(Event) error {
	return nil
}

type RecordingPublisher struct {
	mu     sync.Mutex
	events []Event
}

func NewRecordingPublisher() *RecordingPublisher {
	return &RecordingPublisher{}
}

func (p *RecordingPublisher) Publish(event Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, event)
	return nil
}

func (p *RecordingPublisher) Events() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()

	copied := make([]Event, len(p.events))
	copy(copied, p.events)
	return copied
}

type FailingPublisher struct {
	Err error
}

func (p FailingPublisher) Publish(Event) error {
	if p.Err != nil {
		return p.Err
	}
	return errors.New("publish failed")
}
