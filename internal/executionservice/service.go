package executionservice

import (
	"context"
	"fmt"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
)

type LifecycleManager interface {
	Create(string, demo.Scenario) (*lifecycle.Execution, error)
	Start(string) error
	Complete(string, execution.Result) error
	Fail(string, error) error
	Cancel(string) error
}

type Executor interface {
	Execute(
		context.Context,
		string,
		demo.Scenario,
	) (execution.Result, error)
}

type Service struct {
	executor         Executor
	lifecycleManager LifecycleManager
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

	result, err := s.executor.Execute(ctx, executionID, scenario)
	if err != nil {
		return result, s.finishError(ctx, executionID, err)
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
