package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/event"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/ratelimit"
	"github.com/gorilla/websocket"
)

const testWebSocketOrigin = "http://localhost:3000"

func TestWebSocketRejectsEmptyExecutionID(t *testing.T) {
	hub := event.NewHub(16)

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws"

	headers := http.Header{}
	headers.Set("Origin", testWebSocketOrigin)

	conn, response, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err == nil {
		conn.Close()
		t.Fatal("websocket dial succeeded without execution_id")
	}

	if response == nil {
		t.Fatal("expected HTTP response for rejected handshake")
	}

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf(
			"status = %d, want %d",
			response.StatusCode,
			http.StatusBadRequest,
		)
	}
}

func TestWebSocketRejectsDisallowedOrigin(t *testing.T) {
	hub := event.NewHub(16)

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http") + "/ws?execution_id=exec_1"

	headers := http.Header{}
	headers.Set("Origin", "https://evil.example")

	_, response, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err == nil {
		t.Fatal("websocket dial succeeded with disallowed origin")
	}

	if response == nil {
		t.Fatal("expected HTTP response for rejected handshake")
	}

	if response.StatusCode != http.StatusForbidden {
		t.Fatalf(
			"status = %d, want %d",
			response.StatusCode,
			http.StatusForbidden,
		)
	}
}

func TestWebSocketReplaysHistoryAndLaterEvents(t *testing.T) {
	hub := event.NewHub(16)

	_ = hub.Publish(event.Event{
		Sequence:    1,
		ExecutionID: "exec_1",
		Type:        event.EventCreated,
		Timestamp:   time.Now(),
	})
	_ = hub.Publish(event.Event{
		Sequence:    2,
		ExecutionID: "exec_1",
		Type:        event.EventStarted,
		Timestamp:   time.Now(),
	})

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	conn, _, err := websocketDial(t, s.URL, "exec_1")
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()

	_ = hub.Publish(event.Event{
		Sequence:    3,
		ExecutionID: "exec_1",
		Type:        event.EventCompleted,
		Timestamp:   time.Now(),
	})

	for _, wantSequence := range []uint64{1, 2, 3} {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage() error: %v", err)
		}

		var evt event.Event
		if err := json.Unmarshal(payload, &evt); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if evt.Sequence != wantSequence {
			t.Fatalf(
				"sequence = %d, want %d",
				evt.Sequence,
				wantSequence,
			)
		}
	}
}

func TestWebSocketDeliversOnlyMatchingExecution(t *testing.T) {
	hub := event.NewHub(16)

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	conn, _, err := websocketDial(t, s.URL, "exec_1")
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()

	_ = hub.Publish(event.Event{
		Sequence:    1,
		ExecutionID: "exec_2",
		Type:        event.EventCreated,
		Timestamp:   time.Now(),
	})

	_ = hub.Publish(event.Event{
		Sequence:    2,
		ExecutionID: "exec_1",
		Type:        event.EventStarted,
		Timestamp:   time.Now(),
	})

	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	var evt event.Event
	if err := json.Unmarshal(payload, &evt); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if evt.ExecutionID != "exec_1" {
		t.Fatalf(
			"execution_id = %q, want exec_1",
			evt.ExecutionID,
		)
	}
}

func TestWebSocketClientDisconnectDoesNotBlockHubPublication(t *testing.T) {
	hub := event.NewHub(16)

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	conn, _, err := websocketDial(t, s.URL, "exec_1")
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	done := make(chan struct{})

	go func() {
		_ = hub.Publish(event.Event{
			Sequence:    1,
			ExecutionID: "exec_1",
			Type:        event.EventCreated,
			Timestamp:   time.Now(),
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish() blocked after WebSocket client disconnect")
	}
}

func TestWebSocketConcurrentConnectionsReceiveIndependentStreams(t *testing.T) {
	hub := event.NewHub(16)

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	first, _, err := websocketDial(t, s.URL, "exec_1")
	if err != nil {
		t.Fatalf("first Dial() error: %v", err)
	}
	defer first.Close()

	second, _, err := websocketDial(t, s.URL, "exec_1")
	if err != nil {
		t.Fatalf("second Dial() error: %v", err)
	}
	defer second.Close()

	_ = hub.Publish(event.Event{
		Sequence:    1,
		ExecutionID: "exec_1",
		Type:        event.EventCreated,
		Timestamp:   time.Now(),
	})

	for _, conn := range []*websocket.Conn{first, second} {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage() error: %v", err)
		}

		var evt event.Event
		if err := json.Unmarshal(payload, &evt); err != nil {
			t.Fatalf("json.Unmarshal() error: %v", err)
		}

		if evt.ExecutionID != "exec_1" {
			t.Fatalf(
				"execution_id = %q, want exec_1",
				evt.ExecutionID,
			)
		}
	}
}

func TestWebSocketUnknownExecutionReceivesFutureEvents(t *testing.T) {
	hub := event.NewHub(16)

	server, err := New(
		fakeStarter{},
		policy.Default(),
		ratelimit.NewDefault(),
		testManager(t),
		hub,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	s := httptest.NewServer(server.Handler())
	defer s.Close()

	conn, _, err := websocketDial(t, s.URL, "future_exec")
	if err != nil {
		t.Fatalf("Dial() error: %v", err)
	}
	defer conn.Close()

	want := event.Event{
		Sequence:    1,
		ExecutionID: "future_exec",
		Type:        event.EventCreated,
		Timestamp:   time.Now(),
	}

	_ = hub.Publish(want)

	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	var got event.Event
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("json.Unmarshal() error: %v", err)
	}

	if got.Sequence != want.Sequence {
		t.Fatalf("sequence = %d, want %d", got.Sequence, want.Sequence)
	}
}

type fakeStarter struct{}

func (fakeStarter) StartWithCompletion(
	string,
	demo.Scenario,
	time.Duration,
	func(),
) error {
	return nil
}

func (fakeStarter) Cancel(string) error {
	return nil
}

func testManager(t *testing.T) *lifecycle.Manager {
	t.Helper()

	manager, err := lifecycle.New(10)
	if err != nil {
		t.Fatalf("lifecycle.New() error: %v", err)
	}

	return manager
}

func websocketDial(
	t *testing.T,
	urlString string,
	executionID string,
) (*websocket.Conn, *http.Response, error) {
	t.Helper()

	parsed, err := url.Parse(urlString)
	if err != nil {
		return nil, nil, err
	}

	parsed.Path = "/ws"

	query := parsed.Query()
	query.Set("execution_id", executionID)
	parsed.RawQuery = query.Encode()
	parsed.Scheme = "ws"

	headers := http.Header{}
	headers.Set("Origin", testWebSocketOrigin)

	return websocket.DefaultDialer.Dial(parsed.String(), headers)
}
