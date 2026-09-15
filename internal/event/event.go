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

const defaultHubBufferSize = 16

var errEmptyExecutionID = errors.New("execution ID must not be empty")

type Hub struct {
	mu          sync.Mutex
	subscribers map[string]map[*Subscription]struct{}
	bufferSize  int
}

type Subscription struct {
	hub         *Hub
	executionID string
	events      chan Event
	mu          sync.Mutex
	closed      bool
}

var _ Publisher = (*Hub)(nil)

func NewHub(bufferSize int) *Hub {
	if bufferSize <= 0 {
		bufferSize = defaultHubBufferSize
	}

	return &Hub{
		subscribers: make(map[string]map[*Subscription]struct{}),
		bufferSize:  bufferSize,
	}
}

func (h *Hub) Subscribe(executionID string) (*Subscription, error) {
	if executionID == "" {
		return nil, errEmptyExecutionID
	}

	subscription := &Subscription{
		hub:         h,
		executionID: executionID,
		events:      make(chan Event, h.bufferSize),
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.subscribers[executionID] == nil {
		h.subscribers[executionID] = make(map[*Subscription]struct{})
	}
	h.subscribers[executionID][subscription] = struct{}{}

	return subscription, nil
}

func (h *Hub) Publish(evt Event) error {
	if evt.ExecutionID == "" {
		return errEmptyExecutionID
	}

	h.mu.Lock()
	subscribers := make([]*Subscription, 0, len(h.subscribers[evt.ExecutionID]))
	for subscription := range h.subscribers[evt.ExecutionID] {
		subscribers = append(subscribers, subscription)
	}
	h.mu.Unlock()

	for _, subscription := range subscribers {
		subscription.mu.Lock()
		if !subscription.closed {
			select {
			case subscription.events <- evt:
			default:
				// Drop newest events when a consumer buffer is full until replay exists.
			}
		}
		subscription.mu.Unlock()
	}

	return nil
}

func (s *Subscription) Events() <-chan Event {
	return s.events
}

func (s *Subscription) Close() {
	if s == nil || s.hub == nil {
		return
	}

	s.hub.mu.Lock()
	if subscriptions := s.hub.subscribers[s.executionID]; subscriptions != nil {
		delete(subscriptions, s)
		if len(subscriptions) == 0 {
			delete(s.hub.subscribers, s.executionID)
		}
	}
	s.hub.mu.Unlock()

	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	s.mu.Unlock()
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
