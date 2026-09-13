package event

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestEventTypeConstantsAreDistinctAndStable(t *testing.T) {
	if EventCreated != EventType("execution.created") {
		t.Fatalf("EventCreated = %q, want %q", EventCreated, EventType("execution.created"))
	}
	if EventStarted != EventType("execution.started") {
		t.Fatalf("EventStarted = %q, want %q", EventStarted, EventType("execution.started"))
	}
	if EventCompleted != EventType("execution.completed") {
		t.Fatalf("EventCompleted = %q, want %q", EventCompleted, EventType("execution.completed"))
	}
	if EventFailed != EventType("execution.failed") {
		t.Fatalf("EventFailed = %q, want %q", EventFailed, EventType("execution.failed"))
	}
	if EventCancelled != EventType("execution.cancelled") {
		t.Fatalf("EventCancelled = %q, want %q", EventCancelled, EventType("execution.cancelled"))
	}

	values := []EventType{EventCreated, EventStarted, EventCompleted, EventFailed, EventCancelled}
	seen := make(map[EventType]bool, len(values))
	for _, value := range values {
		if seen[value] {
			t.Fatalf("duplicate EventType %q", value)
		}
		seen[value] = true
	}
}

func TestEventJSONSerializationUsesStableFieldNames(t *testing.T) {
	timestamp := time.Date(2025, time.March, 1, 10, 30, 0, 0, time.UTC)
	event := Event{
		Sequence:    7,
		ExecutionID: "exec_42",
		Type:        EventStarted,
		Timestamp:   timestamp,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if got["sequence"] != float64(7) {
		t.Fatalf("sequence = %#v, want 7", got["sequence"])
	}
	if got["execution_id"] != "exec_42" {
		t.Fatalf("execution_id = %#v, want exec_42", got["execution_id"])
	}
	if got["type"] != "execution.started" {
		t.Fatalf("type = %#v, want execution.started", got["type"])
	}
	if got["timestamp"] == nil {
		t.Fatal("timestamp field missing")
	}
}

func TestRecordingPublisherReceivesEventsExactlyAsPublished(t *testing.T) {
	publisher := NewRecordingPublisher()
	timestamp := time.Now()
	event := Event{Sequence: 3, ExecutionID: "exec_9", Type: EventCompleted, Timestamp: timestamp}

	if err := publisher.Publish(event); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	events := publisher.Events()
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0] != event {
		t.Fatalf("published event = %#v, want %#v", events[0], event)
	}
}

func TestNoopPublisherDoesNotFail(t *testing.T) {
	var publisher NoopPublisher
	if err := publisher.Publish(Event{ExecutionID: "exec_1", Type: EventCreated}); err != nil {
		t.Fatalf("NoopPublisher.Publish() error: %v", err)
	}
}

func TestPublisherPreservesSequenceTimestampAndExecutionID(t *testing.T) {
	timestamp := time.Now().UTC().Round(0)
	publisher := NewRecordingPublisher()
	event := Event{Sequence: 11, ExecutionID: "exec_77", Type: EventFailed, Timestamp: timestamp}

	if err := publisher.Publish(event); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	got := publisher.Events()[0]
	if got.Sequence != event.Sequence || got.ExecutionID != event.ExecutionID || got.Type != event.Type || !got.Timestamp.Equal(event.Timestamp) {
		t.Fatalf("publisher mutated event: got %+v, want %+v", got, event)
	}
}

func TestRecordingPublisherIsSafeForConcurrentUse(t *testing.T) {
	publisher := NewRecordingPublisher()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_ = publisher.Publish(Event{Sequence: uint64(index + 1), ExecutionID: "exec", Type: EventStarted, Timestamp: time.Now()})
		}(i)
	}
	wg.Wait()

	events := publisher.Events()
	if len(events) != 10 {
		t.Fatalf("len(events) = %d, want 10", len(events))
	}
}

func TestFailingPublisherReturnsError(t *testing.T) {
	publisher := FailingPublisher{Err: errors.New("publish failed")}
	if err := publisher.Publish(Event{ExecutionID: "exec"}); err == nil {
		t.Fatal("Publish() error = nil, want non-nil")
	}
}
