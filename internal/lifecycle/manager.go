package lifecycle

import (
	"fmt"
	"sync"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
)

type Manager struct {
	mu sync.RWMutex

	executions map[string]*Execution

	maxRetainedExecutions int
}

func New(maxRetainedExecutions int) (*Manager, error) {
	if maxRetainedExecutions <= 0 {
		return nil, fmt.Errorf(
			"max retained executions must be greater than zero",
		)
	}

	return &Manager{
		executions:            make(map[string]*Execution),
		maxRetainedExecutions: maxRetainedExecutions,
	}, nil
}

func (m *Manager) Create(
	id string,
	scenario demo.Scenario,
) (*Execution, error) {
	if id == "" {
		return nil, fmt.Errorf("execution ID must not be empty")
	}

	execution := newExecution(
		id,
		scenario,
		time.Now(),
	)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.executions[id]; exists {
		return nil, fmt.Errorf(
			"execution %q already exists",
			id,
		)
	}

	m.executions[id] = execution

	m.evictCompletedLocked()

	return execution, nil
}

func (m *Manager) Get(id string) (*ExecutionSnapshot, bool) {
	m.mu.RLock()
	execution, ok := m.executions[id]
	m.mu.RUnlock()

	if !ok {
		return nil, false
	}

	snapshot := execution.Snapshot()

	return &snapshot, true
}

func (m *Manager) Start(id string) error {
	execution, ok := m.lookup(id)
	if !ok {
		return fmt.Errorf("execution %q not found", id)
	}

	execution.mu.Lock()
	defer execution.mu.Unlock()

	if execution.Status != StatusCreated {
		return fmt.Errorf(
			"execution %q cannot start from status %q",
			id,
			execution.Status,
		)
	}

	execution.Status = StatusRunning
	execution.StartedAt = time.Now()

	return nil
}

func (m *Manager) Complete(
	id string,
	result execution.Result,
) error {
	execution, ok := m.lookup(id)
	if !ok {
		return fmt.Errorf("execution %q not found", id)
	}

	execution.mu.Lock()
	defer execution.mu.Unlock()

	if execution.Status != StatusRunning {
		return fmt.Errorf(
			"execution %q cannot complete from status %q",
			id,
			execution.Status,
		)
	}

	execution.Status = StatusCompleted
	execution.CompletedAt = time.Now()
	execution.Result = &result
	execution.Error = ""

	return nil
}

func (m *Manager) Fail(id string, err error) error {
	if err == nil {
		return fmt.Errorf("execution error must not be nil")
	}

	execution, ok := m.lookup(id)
	if !ok {
		return fmt.Errorf("execution %q not found", id)
	}

	execution.mu.Lock()
	defer execution.mu.Unlock()

	if execution.Status != StatusRunning {
		return fmt.Errorf(
			"execution %q cannot fail from status %q",
			id,
			execution.Status,
		)
	}

	execution.Status = StatusFailed
	execution.CompletedAt = time.Now()
	execution.Error = err.Error()

	return nil
}

func (m *Manager) Cancel(id string) error {
	execution, ok := m.lookup(id)
	if !ok {
		return fmt.Errorf("execution %q not found", id)
	}

	execution.mu.Lock()
	defer execution.mu.Unlock()

	if execution.Status != StatusRunning {
		return fmt.Errorf(
			"execution %q cannot cancel from status %q",
			id,
			execution.Status,
		)
	}

	execution.Status = StatusCancelled
	execution.CompletedAt = time.Now()
	execution.Error = "execution cancelled"

	return nil
}

func (m *Manager) lookup(id string) (*Execution, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	execution, ok := m.executions[id]

	return execution, ok
}

func (m *Manager) evictCompletedLocked() {
	for len(m.executions) > m.maxRetainedExecutions {
		var oldestID string
		var oldestCompletedAt time.Time

		for id, execution := range m.executions {
			snapshot := execution.Snapshot()

			switch snapshot.Status {
			case StatusCompleted, StatusFailed, StatusCancelled:
			default:
				continue
			}

			if oldestID == "" ||
				snapshot.CompletedAt.Before(oldestCompletedAt) {
				oldestID = id
				oldestCompletedAt = snapshot.CompletedAt
			}
		}

		if oldestID == "" {
			return
		}

		delete(m.executions, oldestID)
	}
}
