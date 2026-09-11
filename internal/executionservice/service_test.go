package executionservice

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
)

type fakeExecutor struct {
	result  execution.Result
	err     error
	block   bool
	started chan struct{}
	release chan struct{}

	called      bool
	executionID string
	scenario    demo.Scenario
}

func (f *fakeExecutor) Execute(
	ctx context.Context,
	executionID string,
	scenario demo.Scenario,
) (execution.Result, error) {
	f.called = true
	f.executionID = executionID
	f.scenario = scenario

	if f.block {
		close(f.started)
		select {
		case <-f.release:
		case <-ctx.Done():
			return f.result, ctx.Err()
		}
	}

	return f.result, f.err
}

type recordingLifecycle struct {
	createdID string
	startedID string
	completed execution.Result
	failedErr error
	cancelled string

	createErr   error
	startErr    error
	completeErr error
	failErr     error
	cancelErr   error
}

func (m *recordingLifecycle) Create(id string, scenario demo.Scenario) (*lifecycle.Execution, error) {
	m.createdID = id
	if m.createErr != nil {
		return nil, m.createErr
	}
	return &lifecycle.Execution{ID: id, Scenario: scenario}, nil
}

func (m *recordingLifecycle) Start(id string) error {
	m.startedID = id
	return m.startErr
}

func (m *recordingLifecycle) Complete(id string, result execution.Result) error {
	m.completed = result
	return m.completeErr
}

func (m *recordingLifecycle) Fail(id string, err error) error {
	m.failedErr = err
	return m.failErr
}

func (m *recordingLifecycle) Cancel(id string) error {
	m.cancelled = id
	return m.cancelErr
}

func testScenario() demo.Scenario {
	return demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		RequestCount: 1,
	}
}

func testManager(t *testing.T) *lifecycle.Manager {
	t.Helper()
	manager, err := lifecycle.New(10)
	if err != nil {
		t.Fatalf("lifecycle.New() error: %v", err)
	}
	return manager
}

func TestNewRejectsNilDependencies(t *testing.T) {
	manager := testManager(t)
	executor := &fakeExecutor{}

	if _, err := New(nil, manager); err == nil {
		t.Fatal("New() should reject nil executor")
	}
	if _, err := New(executor, nil); err == nil {
		t.Fatal("New() should reject nil lifecycle manager")
	}
}

func TestExecuteCompletesAndPassesInputs(t *testing.T) {
	manager := testManager(t)
	executor := &fakeExecutor{}
	service, err := New(executor, manager)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	scenario := testScenario()
	result := execution.Result{ExecutionID: "exec_1", Scenario: scenario}
	executor.result = result

	got, err := service.Execute(context.Background(), "exec_1", scenario)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if !reflect.DeepEqual(got, result) {
		t.Fatalf("result = %+v, want %+v", got, result)
	}
	if executor.executionID != "exec_1" || !reflect.DeepEqual(executor.scenario, scenario) {
		t.Fatalf("executor inputs = %q, %+v", executor.executionID, executor.scenario)
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok || snapshot.Status != lifecycle.StatusCompleted {
		t.Fatalf("lifecycle snapshot = %+v, found = %v", snapshot, ok)
	}
	if snapshot.Result == nil || !reflect.DeepEqual(*snapshot.Result, result) {
		t.Fatalf("stored result = %+v, want %+v", snapshot.Result, result)
	}
}

func TestExecuteTreatsHTTPAndRequestFailuresAsCompleted(t *testing.T) {
	for name, result := range map[string]execution.Result{
		"http failure":    {Requests: []execution.RequestResult{{StatusCode: 503}}},
		"request failure": {Requests: []execution.RequestResult{{StatusCode: 0, Error: "connection refused"}}},
	} {
		t.Run(name, func(t *testing.T) {
			manager := testManager(t)
			executor := &fakeExecutor{result: result}
			service, err := New(executor, manager)
			if err != nil {
				t.Fatalf("New() error: %v", err)
			}
			if _, err := service.Execute(context.Background(), "exec_1", testScenario()); err != nil {
				t.Fatalf("Execute() error: %v", err)
			}
			snapshot, _ := manager.Get("exec_1")
			if snapshot.Status != lifecycle.StatusCompleted {
				t.Fatalf("status = %q, want completed", snapshot.Status)
			}
		})
	}
}

func TestExecuteFailsOnExecutorError(t *testing.T) {
	manager := testManager(t)
	executor := &fakeExecutor{err: errors.New("executor failed")}
	service, _ := New(executor, manager)

	_, err := service.Execute(context.Background(), "exec_1", testScenario())
	if !errors.Is(err, executor.err) {
		t.Fatalf("error = %v, want %v", err, executor.err)
	}
	snapshot, _ := manager.Get("exec_1")
	if snapshot.Status != lifecycle.StatusFailed || snapshot.Error != executor.err.Error() {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestExecuteCallerCancellation(t *testing.T) {
	manager := testManager(t)

	executor := &fakeExecutor{
		block:   true,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	service, _ := New(executor, manager)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		_, err := service.Execute(ctx, "exec_1", testScenario())
		done <- err
	}()

	<-executor.started
	cancel()

	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok {
		t.Fatal("execution not found")
	}

	if snapshot.Status != lifecycle.StatusCancelled {
		t.Fatalf(
			"status = %q, want %q",
			snapshot.Status,
			lifecycle.StatusCancelled,
		)
	}

	if snapshot.Error != "execution cancelled" {
		t.Fatalf(
			"lifecycle error = %q, want %q",
			snapshot.Error,
			"execution cancelled",
		)
	}
}

func TestExecutePolicyTimeoutFails(t *testing.T) {
	manager := testManager(t)
	executor := &fakeExecutor{
		block:   true,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	service, _ := New(executor, manager)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := service.Execute(ctx, "exec_1", testScenario())
	<-executor.started
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	snapshot, _ := manager.Get("exec_1")
	if snapshot.Status != lifecycle.StatusFailed || snapshot.Error != context.DeadlineExceeded.Error() {
		t.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestExecuteCreateAndStartFailuresDoNotExecute(t *testing.T) {
	for name, lifecycleManager := range map[string]*recordingLifecycle{
		"create": {createErr: errors.New("create failed")},
		"start":  {startErr: errors.New("start failed")},
	} {
		t.Run(name, func(t *testing.T) {
			executor := &fakeExecutor{}
			service, _ := New(executor, lifecycleManager)
			_, err := service.Execute(context.Background(), "exec_1", testScenario())
			if err == nil {
				t.Fatal("Execute() should fail")
			}
			if executor.called {
				t.Fatal("executor must not be called")
			}
		})
	}
}

func TestExecuteReturnsLifecycleOperationErrors(t *testing.T) {
	cases := []struct {
		name string
		set  func(*recordingLifecycle)
		want string
	}{
		{"complete", func(m *recordingLifecycle) { m.completeErr = errors.New("complete failed") }, "complete execution"},
		{"fail", func(m *recordingLifecycle) { m.failErr = errors.New("fail failed") }, "fail execution"},
		{"cancel", func(m *recordingLifecycle) { m.cancelErr = errors.New("cancel failed") }, "cancel execution"},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			manager := &recordingLifecycle{}
			test.set(manager)
			executor := &fakeExecutor{}
			service, _ := New(executor, manager)
			ctx := context.Background()
			if test.name == "fail" {
				executor.err = errors.New("executor failed")
			}
			if test.name == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				executor.block = true
				executor.started = make(chan struct{})
				executor.release = make(chan struct{})
			}
			_, err := service.Execute(ctx, "exec_1", testScenario())
			if err == nil || !contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func contains(value, fragment string) bool {
	return strings.HasPrefix(value, fragment)
}
