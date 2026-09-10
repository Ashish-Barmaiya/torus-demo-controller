package identity

import (
	"strings"
	"testing"
)

func TestNewExecutionID(t *testing.T) {
	id, err := NewExecutionID()
	if err != nil {
		t.Fatalf("NewExecutionID() error: %v", err)
	}

	if !strings.HasPrefix(id, "exec_") {
		t.Fatalf("ID = %q, want exec_ prefix", id)
	}

	if len(id) != len("exec_")+2*idBytes {
		t.Fatalf(
			"ID length = %d, want %d",
			len(id),
			len("exec_")+2*idBytes,
		)
	}
}

func TestNewRequestID(t *testing.T) {
	id, err := NewRequestID()
	if err != nil {
		t.Fatalf("NewRequestID() error: %v", err)
	}

	if !strings.HasPrefix(id, "req_") {
		t.Fatalf("ID = %q, want req_ prefix", id)
	}

	if len(id) != len("req_")+2*idBytes {
		t.Fatalf(
			"ID length = %d, want %d",
			len(id),
			len("req_")+2*idBytes,
		)
	}
}

func TestIDsAreUnique(t *testing.T) {
	const count = 1000

	executionIDs := make(map[string]struct{}, count)
	requestIDs := make(map[string]struct{}, count)

	for i := 0; i < count; i++ {
		executionID, err := NewExecutionID()
		if err != nil {
			t.Fatalf("NewExecutionID() error: %v", err)
		}

		if _, exists := executionIDs[executionID]; exists {
			t.Fatalf("duplicate execution ID: %s", executionID)
		}

		executionIDs[executionID] = struct{}{}

		requestID, err := NewRequestID()
		if err != nil {
			t.Fatalf("NewRequestID() error: %v", err)
		}

		if _, exists := requestIDs[requestID]; exists {
			t.Fatalf("duplicate request ID: %s", requestID)
		}

		requestIDs[requestID] = struct{}{}
	}
}
