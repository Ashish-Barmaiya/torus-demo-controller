package executionservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
)

var (
	ErrExecutionNotFound  = errors.New("execution not found")
	ErrExecutionNotActive = errors.New("execution not active")
)

type LifecycleManager interface {
	Create(string, demo.Scenario) (*lifecycle.Execution, error)
	Start(string) error
	Complete(string, execution.Result) error
	Fail(string, error) error
	Cancel(string) error
	Get(string) (*lifecycle.ExecutionSnapshot, bool)
}

type Executor interface {
	Execute(
		context.Context,
		string,
		demo.Scenario,
	) (execution.Result, error)
}

type activeExecutions struct {
	mu      sync.Mutex
	entries map[string]context.CancelFunc
}

func newActiveExecutions() *activeExecutions {
	return &activeExecutions{entries: make(map[string]context.CancelFunc)}
}

func (a *activeExecutions) register(id string, cancel context.CancelFunc) {
	if cancel == nil {
		return
	}
	if id == "" {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries[id] = cancel
}

func (a *activeExecutions) remove(id string) {
	if id == "" {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.entries, id)
}

func (a *activeExecutions) cancel(id string) (context.CancelFunc, bool) {
	if id == "" {
		return nil, false
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	cancel, ok := a.entries[id]
	if !ok {
		return nil, false
	}
	delete(a.entries, id)
	return cancel, true
}

type Service struct {
	executor         Executor
	lifecycleManager LifecycleManager
	activeExecutions *activeExecutions
}

func New(
	executor Executor,
	lifecycleManager LifecycleManager,
) (*Service, error) {
	if executor == nil {
		return nil, fmt.Errorf("executor must not be nil")
	}
	if lifecycleManager == nil {
		return nil, fmt.Errorf("lifecycle manager must not be nil")
	}

	return &Service{
		executor:         executor,
		lifecycleManager: lifecycleManager,
		activeExecutions: newActiveExecutions(),
	}, nil
}

func (s *Service) Execute(
	ctx context.Context,
	executionID string,
	scenario demo.Scenario,
) (execution.Result, error) {
	if _, err := s.lifecycleManager.Create(executionID, scenario); err != nil {
		return execution.Result{}, fmt.Errorf("create execution: %w", err)
	}

	if err := s.lifecycleManager.Start(executionID); err != nil {
		return execution.Result{}, fmt.Errorf("start execution: %w", err)
	}

	return s.executeAndRecord(ctx, executionID, scenario)
}

func (s *Service) Start(
	executionID string,
	scenario demo.Scenario,
	timeout time.Duration,
) error {
	return s.start(executionID, scenario, timeout, nil)
}

func (s *Service) StartWithCompletion(
	executionID string,
	scenario demo.Scenario,
	timeout time.Duration,
	onCompletion func(),
) error {
	return s.start(executionID, scenario, timeout, onCompletion)
}

func (s *Service) Cancel(executionID string) error {
	if executionID == "" {
		return ErrExecutionNotFound
	}

	snapshot, ok := s.lifecycleManager.Get(executionID)
	if !ok {
		return ErrExecutionNotFound
	}
	if snapshot.Status == lifecycle.StatusCompleted ||
		snapshot.Status == lifecycle.StatusFailed ||
		snapshot.Status == lifecycle.StatusCancelled {
		return ErrExecutionNotActive
	}

	cancel, ok := s.activeExecutions.cancel(executionID)
	if !ok || cancel == nil {
		return ErrExecutionNotActive
	}

	cancel()
	return nil
}

func (s *Service) start(
	executionID string,
	scenario demo.Scenario,
	timeout time.Duration,
	onCompletion func(),
) error {
	if timeout <= 0 {
		return fmt.Errorf("execution timeout must be greater than zero")
	}

	if _, err := s.lifecycleManager.Create(executionID, scenario); err != nil {
		return fmt.Errorf("create execution: %w", err)
	}

	if err := s.lifecycleManager.Start(executionID); err != nil {
		return fmt.Errorf("start execution: %w", err)
	}

	executionCtx, cancel := context.WithTimeout(context.Background(), timeout)
	s.activeExecutions.register(executionID, cancel)

	go func() {
		defer func() {
			s.activeExecutions.remove(executionID)
			cancel()

			if onCompletion != nil {
				onCompletion()
			}
		}()

		_, _ = s.executeAndRecord(
			executionCtx,
			executionID,
			scenario,
		)
	}()

	return nil
}

func (s *Service) executeAndRecord(
	ctx context.Context,
	executionID string,
	scenario demo.Scenario,
) (execution.Result, error) {
	result, executionErr := s.executor.Execute(ctx, executionID, scenario)
	if executionErr != nil {
		return result, s.finishError(ctx, executionID, executionErr)
	}

	if ctx.Err() == context.DeadlineExceeded {
		if lifecycleErr := s.lifecycleManager.Fail(executionID, ctx.Err()); lifecycleErr != nil {
			return result, fmt.Errorf("fail execution: %w", lifecycleErr)
		}
		return result, ctx.Err()
	}

	if ctx.Err() == context.Canceled {
		if lifecycleErr := s.lifecycleManager.Cancel(executionID); lifecycleErr != nil {
			return result, fmt.Errorf("cancel execution: %w", lifecycleErr)
		}
		return result, nil
	}

	if err := s.lifecycleManager.Complete(executionID, result); err != nil {
		return result, fmt.Errorf("complete execution: %w", err)
	}

	return result, nil
}

func (s *Service) finishError(
	ctx context.Context,
	executionID string,
	executionErr error,
) error {
	if ctx.Err() == context.DeadlineExceeded {
		if err := s.lifecycleManager.Fail(executionID, ctx.Err()); err != nil {
			return fmt.Errorf("fail execution: %w", err)
		}
		return executionErr
	}

	if ctx.Err() == context.Canceled {
		if err := s.lifecycleManager.Cancel(executionID); err != nil {
			return fmt.Errorf("cancel execution: %w", err)
		}
		return executionErr
	}

	if err := s.lifecycleManager.Fail(executionID, executionErr); err != nil {
		return fmt.Errorf("fail execution: %w", err)
	}

	return executionErr
}
