package lifecycle

import (
	"sync"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
)

type Status string

const (
	StatusCreated   Status = "created"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Execution struct {
	mu sync.RWMutex

	ID       string
	Scenario demo.Scenario
	Status   Status

	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time

	Result *execution.Result
	Error  string
}

type ExecutionSnapshot struct {
	ID          string
	Scenario    demo.Scenario
	Status      Status
	CreatedAt   time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	Result      *execution.Result
	Error       string
}

func newExecution(
	id string,
	scenario demo.Scenario,
	now time.Time,
) *Execution {
	return &Execution{
		ID:        id,
		Scenario:  scenario,
		Status:    StatusCreated,
		CreatedAt: now,
	}
}

func (e *Execution) Snapshot() ExecutionSnapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return ExecutionSnapshot{
		ID:          e.ID,
		Scenario:    e.Scenario,
		Status:      e.Status,
		CreatedAt:   e.CreatedAt,
		StartedAt:   e.StartedAt,
		CompletedAt: e.CompletedAt,
		Result:      cloneResult(e.Result),
		Error:       e.Error,
	}
}

func cloneResult(result *execution.Result) *execution.Result {
	if result == nil {
		return nil
	}

	cloned := *result

	cloned.Requests = append(
		[]execution.RequestResult(nil),
		result.Requests...,
	)

	return &cloned
}
