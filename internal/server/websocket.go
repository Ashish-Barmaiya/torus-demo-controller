package server

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	websocketWriteTimeout = 5 * time.Second

	// Used when TORUS_DEMO_ALLOWED_WS_ORIGINS is not set.
	// Origins are exact origin values, including scheme and port.
	defaultWebSocketAllowedOrigins = "http://localhost:3000,http://127.0.0.1:3000"
)

// WebSocket transport is intentionally thin: the Hub owns replay, retention,
// and per-execution subscription semantics. This transport is process-local
// with respect to event history.
var websocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkWebSocketOrigin,
}

func checkWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")

	// Non-browser clients commonly omit Origin.
	if origin == "" {
		return true
	}

	allowedOrigins := os.Getenv("TORUS_DEMO_ALLOWED_WS_ORIGINS")
	if allowedOrigins == "" {
		allowedOrigins = defaultWebSocketAllowedOrigins
	}

	for _, allowed := range strings.Split(allowedOrigins, ",") {
		if strings.TrimSpace(allowed) == origin {
			return true
		}
	}

	return false
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, APIError{
			Code:    ErrorCodeMethodNotAllowed,
			Message: "method not allowed",
		})
		return
	}

	if s.eventHub == nil {
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "event hub unavailable",
		})
		return
	}

	executionID := r.URL.Query().Get("execution_id")
	if executionID == "" {
		writeAPIError(w, http.StatusBadRequest, APIError{
			Code:    ErrorCodeExecutionNotFound,
			Message: "execution_id is required",
		})
		return
	}

	subscription, err := s.eventHub.Subscribe(executionID)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, APIError{
			Code:    ErrorCodeExecutionNotFound,
			Message: "invalid execution_id",
		})
		return
	}
	defer subscription.Close()

	connection, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	writeDone := make(chan struct{})

	go func() {
		defer close(writeDone)
		defer connection.Close()

		for {
			select {
			case <-ctx.Done():
				return

			case evt, ok := <-subscription.Events():
				if !ok {
					cancel()
					return
				}

				payload, err := json.Marshal(evt)
				if err != nil {
					cancel()
					return
				}

				if err := connection.SetWriteDeadline(
					time.Now().Add(websocketWriteTimeout),
				); err != nil {
					cancel()
					return
				}

				if err := connection.WriteMessage(
					websocket.TextMessage,
					payload,
				); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	for {
		_, _, err := connection.ReadMessage()
		if err != nil {
			cancel()
			_ = connection.Close()
			<-writeDone
			return
		}
	}
}
