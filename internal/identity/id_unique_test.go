package identity

import "testing"

func TestGeneratedIDsAreUniqueAcrossExecutions(t *testing.T) {
	const executions = 128
	const requestsPerExecution = 4

	executionIDs := make(map[string]struct{}, executions)
	requestIDs := make(map[string]struct{}, executions*requestsPerExecution)

	for i := 0; i < executions; i++ {
		executionID, err := NewExecutionID()
		if err != nil {
			t.Fatalf("NewExecutionID() error: %v", err)
		}

		if _, exists := executionIDs[executionID]; exists {
			t.Fatalf("duplicate execution ID: %s", executionID)
		}
		executionIDs[executionID] = struct{}{}

		for j := 0; j < requestsPerExecution; j++ {
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
}
