package event

import (
	"encoding/json"
	"errors"
	"fmt"
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

func TestHubStartsEmptyAndRejectsEmptyExecutionIDs(t *testing.T) {
	hub := NewHub(1)
	if len(hub.subscribers) != 0 {
		t.Fatalf("subscriber count = %d, want 0", len(hub.subscribers))
	}
	if _, err := hub.Subscribe(""); err == nil {
		t.Fatal("Subscribe(\"\") error = nil")
	}
	if err := hub.Publish(Event{}); err == nil {
		t.Fatal("Publish() with empty execution ID error = nil")
	}
}

func TestHubDeliversOnlyMatchingEvents(t *testing.T) {
	hub := NewHub(4)
	subscription, err := hub.Subscribe("exec_1")
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}
	defer subscription.Close()

	want := Event{Sequence: 1, ExecutionID: "exec_1", Type: EventStarted}
	if err := hub.Publish(Event{Sequence: 2, ExecutionID: "exec_2"}); err != nil {
		t.Fatalf("Publish(other execution) error: %v", err)
	}
	if err := hub.Publish(want); err != nil {
		t.Fatalf("Publish(matching execution) error: %v", err)
	}

	select {
	case got := <-subscription.Events():
		if got != want {
			t.Fatalf("event = %+v, want %+v", got, want)
		}
	default:
		t.Fatal("matching event was not delivered")
	}
}

func TestHubSupportsMultipleSubscribersAndNoSubscribers(t *testing.T) {
	hub := NewHub(2)
	first, _ := hub.Subscribe("exec_1")
	second, _ := hub.Subscribe("exec_1")
	defer first.Close()
	defer second.Close()

	want := Event{Sequence: 1, ExecutionID: "exec_1"}
	if err := hub.Publish(want); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	for name, subscription := range map[string]*Subscription{"first": first, "second": second} {
		select {
		case got := <-subscription.Events():
			if got != want {
				t.Fatalf("%s event = %+v, want %+v", name, got, want)
			}
		default:
			t.Fatalf("%s subscriber did not receive event", name)
		}
	}

	first.Close()
	second.Close()
	if err := hub.Publish(want); err != nil {
		t.Fatalf("Publish() with no subscribers error: %v", err)
	}
}

func TestHubReplaysHistoryAndCloseIsIdempotent(t *testing.T) {
	hub := NewHub(2)
	eventBeforeSubscribe := Event{Sequence: 1, ExecutionID: "exec_1"}
	if err := hub.Publish(eventBeforeSubscribe); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}

	subscription, _ := hub.Subscribe("exec_1")
	if got := <-subscription.Events(); got != eventBeforeSubscribe {
		t.Fatalf("replayed event = %+v, want %+v", got, eventBeforeSubscribe)
	}
	subscription.Close()
	subscription.Close()
	if _, ok := <-subscription.Events(); ok {
		t.Fatal("closed subscription delivered an event")
	}
	if len(hub.subscribers) != 0 {
		t.Fatalf("subscriber count after close = %d, want 0", len(hub.subscribers))
	}
	if err := hub.Publish(Event{Sequence: 2, ExecutionID: "exec_1"}); err != nil {
		t.Fatalf("Publish() after close error: %v", err)
	}
}

func TestHubPreservesOrderingWhenBufferDoesNotOverflow(t *testing.T) {
	hub := NewHub(3)
	subscription, _ := hub.Subscribe("exec_1")
	defer subscription.Close()

	for sequence := uint64(1); sequence <= 3; sequence++ {
		if err := hub.Publish(Event{Sequence: sequence, ExecutionID: "exec_1"}); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}
	for sequence := uint64(1); sequence <= 3; sequence++ {
		got := <-subscription.Events()
		if got.Sequence != sequence {
			t.Fatalf("sequence = %d, want %d", got.Sequence, sequence)
		}
	}
}

func TestHubDropsNewestEventWhenBufferIsFullWithoutBlocking(t *testing.T) {
	hub := NewHub(1)
	subscription, _ := hub.Subscribe("exec_1")
	defer subscription.Close()

	first := Event{Sequence: 1, ExecutionID: "exec_1"}
	second := Event{Sequence: 2, ExecutionID: "exec_1"}
	_ = hub.Publish(first)
	done := make(chan struct{})
	go func() {
		_ = hub.Publish(second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish() blocked on a full subscriber")
	}

	if got := <-subscription.Events(); got != first {
		t.Fatalf("buffered event = %+v, want %+v", got, first)
	}
	select {
	case got := <-subscription.Events():
		t.Fatalf("unexpected event after full buffer: %+v", got)
	default:
	}
}

func TestHubSlowSubscriberDoesNotBlockAnother(t *testing.T) {
	hub := NewHub(1)
	slow, _ := hub.Subscribe("exec_1")
	fast, _ := hub.Subscribe("exec_1")
	defer slow.Close()
	defer fast.Close()

	_ = hub.Publish(Event{Sequence: 1, ExecutionID: "exec_1"})
	done := make(chan struct{})
	go func() {
		_ = hub.Publish(Event{Sequence: 2, ExecutionID: "exec_1"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish() blocked on a slow subscriber")
	}

	if got := <-fast.Events(); got.Sequence != 1 {
		t.Fatalf("fast subscriber first sequence = %d, want 1", got.Sequence)
	}
}

func TestHubConcurrentPublishAndClose(t *testing.T) {
	hub := NewHub(32)
	subscription, _ := hub.Subscribe("exec_1")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(sequence int) {
			defer wg.Done()
			_ = hub.Publish(Event{Sequence: uint64(sequence), ExecutionID: "exec_1"})
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		subscription.Close()
	}()
	wg.Wait()
	subscription.Close()
}

func TestHubSupportsConcurrentSubscriptions(t *testing.T) {
	hub := NewHub(1)
	const subscriptionCount = 20
	subscriptions := make([]*Subscription, subscriptionCount)
	var wg sync.WaitGroup
	for i := range subscriptions {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			subscriptions[index], _ = hub.Subscribe("exec_1")
		}(i)
	}
	wg.Wait()
	if len(hub.subscribers["exec_1"]) != subscriptionCount {
		t.Fatalf("subscriber count = %d, want %d", len(hub.subscribers["exec_1"]), subscriptionCount)
	}
	for _, subscription := range subscriptions {
		subscription.Close()
	}
}

func TestHubRetainsOnlyMostRecentHistory(t *testing.T) {
	hub := NewHub(DefaultSubscriberBufferSize)
	for sequence := uint64(1); sequence <= DefaultHistorySize+4; sequence++ {
		if err := hub.Publish(Event{Sequence: sequence, ExecutionID: "exec_1"}); err != nil {
			t.Fatalf("Publish() error: %v", err)
		}
	}

	subscription, _ := hub.Subscribe("exec_1")
	defer subscription.Close()
	for sequence := uint64(5); sequence <= DefaultHistorySize+4; sequence++ {
		if got := <-subscription.Events(); got.Sequence != sequence {
			t.Fatalf("replayed sequence = %d, want %d", got.Sequence, sequence)
		}
	}
}

func TestHubUnknownExecutionReceivesFutureEvents(t *testing.T) {
	hub := NewHub(2)
	subscription, err := hub.Subscribe("future")
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}
	defer subscription.Close()

	want := Event{Sequence: 1, ExecutionID: "future"}
	if err := hub.Publish(want); err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	if got := <-subscription.Events(); got != want {
		t.Fatalf("future event = %+v, want %+v", got, want)
	}
}

func TestHubReplaysThenDeliversLiveEventExactlyOnce(t *testing.T) {
	hub := NewHub(4)
	history := []Event{
		{Sequence: 1, ExecutionID: "exec_1", Type: EventCreated},
		{Sequence: 2, ExecutionID: "exec_1", Type: EventStarted},
	}
	for _, evt := range history {
		_ = hub.Publish(evt)
	}

	subscription, _ := hub.Subscribe("exec_1")
	defer subscription.Close()
	live := Event{Sequence: 3, ExecutionID: "exec_1", Type: EventCompleted}
	_ = hub.Publish(live)

	for sequence := uint64(1); sequence <= 3; sequence++ {
		if got := <-subscription.Events(); got.Sequence != sequence {
			t.Fatalf("sequence = %d, want %d", got.Sequence, sequence)
		}
	}
	select {
	case got := <-subscription.Events():
		t.Fatalf("duplicate event = %+v", got)
	default:
	}
}

func TestHubMultipleSubscribersReplayAndReceiveFutureEvents(t *testing.T) {
	hub := NewHub(4)
	_ = hub.Publish(Event{Sequence: 1, ExecutionID: "exec_1"})
	first, _ := hub.Subscribe("exec_1")
	second, _ := hub.Subscribe("exec_1")
	defer first.Close()
	defer second.Close()
	_ = hub.Publish(Event{Sequence: 2, ExecutionID: "exec_1"})

	for name, subscription := range map[string]*Subscription{"first": first, "second": second} {
		for sequence := uint64(1); sequence <= 2; sequence++ {
			if got := <-subscription.Events(); got.Sequence != sequence {
				t.Fatalf("%s sequence = %d, want %d", name, got.Sequence, sequence)
			}
		}
	}
}

func TestHubIsolatesHistoryByExecution(t *testing.T) {
	hub := NewHub(2)
	_ = hub.Publish(Event{Sequence: 1, ExecutionID: "exec_1"})
	_ = hub.Publish(Event{Sequence: 1, ExecutionID: "exec_2"})

	first, _ := hub.Subscribe("exec_1")
	second, _ := hub.Subscribe("exec_2")
	defer first.Close()
	defer second.Close()
	if got := <-first.Events(); got.ExecutionID != "exec_1" {
		t.Fatalf("first execution = %q, want exec_1", got.ExecutionID)
	}
	if got := <-second.Events(); got.ExecutionID != "exec_2" {
		t.Fatalf("second execution = %q, want exec_2", got.ExecutionID)
	}
}

func TestHubEvictsOldestExecutionHistory(t *testing.T) {
	hub := NewHub(1)
	for index := 0; index < MaxRetainedExecutions; index++ {
		_ = hub.Publish(Event{Sequence: 1, ExecutionID: fmt.Sprintf("exec_%d", index)})
	}
	_ = hub.Publish(Event{Sequence: 1, ExecutionID: "newest"})

	if _, exists := hub.history["exec_0"]; exists {
		t.Fatal("oldest execution history was not evicted")
	}
	if _, exists := hub.history["exec_1"]; !exists {
		t.Fatal("newer execution history was evicted")
	}
	subscription, _ := hub.Subscribe("newest")
	defer subscription.Close()
	if got := <-subscription.Events(); got.ExecutionID != "newest" {
		t.Fatalf("replayed execution = %q, want newest", got.ExecutionID)
	}
}

func TestHubReplaysTerminalExecution(t *testing.T) {
	hub := NewHub(3)
	for sequence, eventType := range []EventType{EventCreated, EventStarted, EventCompleted} {
		_ = hub.Publish(Event{Sequence: uint64(sequence + 1), ExecutionID: "exec_1", Type: eventType})
	}
	subscription, _ := hub.Subscribe("exec_1")
	defer subscription.Close()
	for sequence := uint64(1); sequence <= 3; sequence++ {
		if got := <-subscription.Events(); got.Sequence != sequence {
			t.Fatalf("terminal replay sequence = %d, want %d", got.Sequence, sequence)
		}
	}
}

func TestHubSubscribePublishBoundaryDoesNotLoseOrDuplicateEvents(t *testing.T) {
	const attempts = 25
	for attempt := 0; attempt < attempts; attempt++ {
		hub := NewHub(3)
		_ = hub.Publish(Event{Sequence: 1, ExecutionID: "exec_1"})

		start := make(chan struct{})
		subscriptions := make(chan *Subscription, 1)
		go func() {
			<-start
			subscription, _ := hub.Subscribe("exec_1")
			subscriptions <- subscription
		}()
		go func() {
			<-start
			_ = hub.Publish(Event{Sequence: 2, ExecutionID: "exec_1"})
		}()
		close(start)
		subscription := <-subscriptions
		_ = hub.Publish(Event{Sequence: 3, ExecutionID: "exec_1"})

		seen := make(map[uint64]int)
		for len(seen) < 3 {
			seen[(<-subscription.Events()).Sequence]++
		}
		for sequence := uint64(1); sequence <= 3; sequence++ {
			if seen[sequence] != 1 {
				t.Fatalf("sequence %d seen %d times", sequence, seen[sequence])
			}
		}
		subscription.Close()
	}
}
