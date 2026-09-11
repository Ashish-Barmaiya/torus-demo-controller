package lifecycle

import (
	"errors"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
)

func testScenario() demo.Scenario {
	return demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		Simulation:   demo.SimulationNormal,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 1,
	}
}

func TestNewExecution(t *testing.T) {
	now := time.Now()
	scenario := testScenario()

	execution := newExecution(
		"exec_test",
		scenario,
		now,
	)

	if execution.ID != "exec_test" {
		t.Fatalf(
			"ID = %q, want %q",
			execution.ID,
			"exec_test",
		)
	}

	if execution.Status != StatusCreated {
		t.Fatalf(
			"Status = %q, want %q",
			execution.Status,
			StatusCreated,
		)
	}

	if !execution.CreatedAt.Equal(now) {
		t.Fatalf(
			"CreatedAt = %v, want %v",
			execution.CreatedAt,
			now,
		)
	}

	if !execution.StartedAt.IsZero() {
		t.Fatal("StartedAt should initially be zero")
	}

	if !execution.CompletedAt.IsZero() {
		t.Fatal("CompletedAt should initially be zero")
	}

	if execution.Result != nil {
		t.Fatal("Result should initially be nil")
	}

	if execution.Error != "" {
		t.Fatal("Error should initially be empty")
	}
}

func TestExecutionSnapshot(t *testing.T) {
	execution := newExecution(
		"exec_test",
		testScenario(),
		time.Now(),
	)

	snapshot := execution.Snapshot()

	if snapshot.ID != execution.ID {
		t.Fatalf(
			"snapshot ID = %q, want %q",
			snapshot.ID,
			execution.ID,
		)
	}

	if snapshot.Status != StatusCreated {
		t.Fatalf(
			"snapshot status = %q, want %q",
			snapshot.Status,
			StatusCreated,
		)
	}
}

func TestExecutionSnapshotClonesRequests(t *testing.T) {
	result := execution.Result{
		ExecutionID: "exec_test",
		Requests: []execution.RequestResult{
			{
				RequestID:  "req_test",
				Index:      0,
				StatusCode: 200,
				Body:       []byte("hello"),
			},
		},
	}

	exec := newExecution(
		"exec_test",
		testScenario(),
		time.Now(),
	)

	exec.Result = &result

	snapshot := exec.Snapshot()

	if snapshot.Result == nil {
		t.Fatal("snapshot result must not be nil")
	}

	if len(snapshot.Result.Requests) != 1 {
		t.Fatalf(
			"requests = %d, want 1",
			len(snapshot.Result.Requests),
		)
	}

	snapshot.Result.Requests[0].RequestID = "changed"

	if exec.Result.Requests[0].RequestID != "req_test" {
		t.Fatal("snapshot mutation changed original request")
	}
}

func TestCloneResultNil(t *testing.T) {
	if got := cloneResult(nil); got != nil {
		t.Fatal("cloneResult(nil) should return nil")
	}
}

func TestCloneResultCopiesRequestSlice(t *testing.T) {
	original := &execution.Result{
		ExecutionID: "exec_test",
		Requests: []execution.RequestResult{
			{
				RequestID: "req_test",
				Index:     0,
			},
		},
	}

	cloned := cloneResult(original)

	if cloned == original {
		t.Fatal("cloneResult must return a distinct Result")
	}

	if len(cloned.Requests) != 1 {
		t.Fatalf(
			"requests = %d, want 1",
			len(cloned.Requests),
		)
	}

	cloned.Requests[0].RequestID = "changed"

	if original.Requests[0].RequestID != "req_test" {
		t.Fatal("mutating clone changed original request")
	}
}

func TestStatusValues(t *testing.T) {
	tests := []struct {
		status Status
	}{
		{StatusCreated},
		{StatusRunning},
		{StatusCompleted},
		{StatusFailed},
		{StatusCancelled},
	}

	for _, tt := range tests {
		if tt.status == "" {
			t.Fatal("status must not be empty")
		}
	}
}

func TestExecutionSnapshotRetainsError(t *testing.T) {
	exec := newExecution(
		"exec_test",
		testScenario(),
		time.Now(),
	)

	exec.Status = StatusFailed
	exec.Error = errors.New("boom").Error()

	snapshot := exec.Snapshot()

	if snapshot.Error != "boom" {
		t.Fatalf(
			"Error = %q, want %q",
			snapshot.Error,
			"boom",
		)
	}
}
