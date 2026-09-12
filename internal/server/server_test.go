package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/executionservice"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/ratelimit"
)

type fakeExecutor struct {
	mu sync.Mutex

	result execution.Result
	err    error

	started  chan struct{}
	release  chan struct{}
	finished chan struct{}

	executionID string
	scenario    demo.Scenario
}

func (f *fakeExecutor) Execute(
	ctx context.Context,
	executionID string,
	scenario demo.Scenario,
) (execution.Result, error) {
	f.mu.Lock()
	f.executionID = executionID
	f.scenario = scenario
	f.mu.Unlock()

	if f.started != nil {
		f.started <- struct{}{}
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			if f.finished != nil {
				close(f.finished)
			}
			return execution.Result{}, ctx.Err()
		}
	}

	result := f.result
	result.ExecutionID = executionID
	if result.Scenario == (demo.Scenario{}) {
		result.Scenario = scenario
	}
	if f.finished != nil {
		close(f.finished)
	}
	return result, f.err
}

func (f *fakeExecutor) ID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executionID
}

type observedLifecycle struct {
	manager *lifecycle.Manager
	done    chan struct{}
	once    sync.Once
}

func (o *observedLifecycle) Create(id string, scenario demo.Scenario) (*lifecycle.Execution, error) {
	return o.manager.Create(id, scenario)
}

func (o *observedLifecycle) Start(id string) error {
	return o.manager.Start(id)
}

func (o *observedLifecycle) Complete(id string, result execution.Result) error {
	err := o.manager.Complete(id, result)
	o.signal()
	return err
}

func (o *observedLifecycle) Fail(id string, err error) error {
	lifecycleErr := o.manager.Fail(id, err)
	o.signal()
	return lifecycleErr
}

func (o *observedLifecycle) Cancel(id string) error {
	err := o.manager.Cancel(id)
	o.signal()
	return err
}

func (o *observedLifecycle) signal() {
	o.once.Do(func() { close(o.done) })
}

type failingStarter struct {
	err error
}

func (s failingStarter) StartWithCompletion(string, demo.Scenario, time.Duration, func()) error {
	return s.err
}

func TestServerExecutionStatusEndpoint(t *testing.T) {
	t.Run("unknown execution", func(t *testing.T) {
		server, _, _ := newTestServer(t, &fakeExecutor{}, policy.Default(), ratelimit.NewDefault())
		response := serveGetExecution(server.Handler(), "missing-exec")
		if response.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
		}
		assertRecorderAPIError(t, response, ErrorCodeExecutionNotFound, "execution not found")
	})

	t.Run("created", func(t *testing.T) {
		server, manager, _ := newTestServer(t, &fakeExecutor{}, policy.Default(), ratelimit.NewDefault())
		executionID := "exec_created"
		_, err := manager.Create(executionID, demo.Scenario{Service: demo.ServiceUsers, Operation: demo.OperationGetUser, Simulation: demo.SimulationNormal})
		if err != nil {
			t.Fatalf("manager.Create() error: %v", err)
		}
		response := serveGetExecution(server.Handler(), executionID)
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
		}
		var payload executionResponse
		if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Status != lifecycle.StatusCreated {
			t.Fatalf("status = %q, want %q", payload.Status, lifecycle.StatusCreated)
		}
		if payload.Result != nil {
			t.Fatal("result = non-nil, want nil for created execution")
		}
	})

	t.Run("running", func(t *testing.T) {
		executor := &fakeExecutor{started: make(chan struct{}, 1), release: make(chan struct{})}
		server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())
		response := serveRun(server.Handler(), "127.0.0.1:12345")
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", response.Code)
		}
		<-executor.started
		executionID := executor.ID()
		statusResponse := serveGetExecution(server.Handler(), executionID)
		if statusResponse.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", statusResponse.Code, http.StatusOK)
		}
		var payload executionResponse
		if err := json.NewDecoder(statusResponse.Body).Decode(&payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Status != lifecycle.StatusRunning {
			t.Fatalf("status = %q, want %q", payload.Status, lifecycle.StatusRunning)
		}
		if payload.Result != nil {
			t.Fatal("result = non-nil, want nil for running execution")
		}
		close(executor.release)
		waitForLifecycle(t, observer)
		if snapshot, ok := manager.Get(executionID); !ok || snapshot.Status != lifecycle.StatusCompleted {
			t.Fatalf("snapshot = %+v, ok=%v; want completed", snapshot, ok)
		}
	})

	t.Run("completed", func(t *testing.T) {
		executor := &fakeExecutor{
			result: execution.Result{
				Requests: []execution.RequestResult{{RequestID: "req_1", Index: 0, StatusCode: 200}},
			},
			finished: make(chan struct{}),
		}
		server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())
		response := serveRun(server.Handler(), "127.0.0.1:12345")
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", response.Code)
		}
		<-executor.finished
		waitForLifecycle(t, observer)
		executionID := executor.ID()
		statusResponse := serveGetExecution(server.Handler(), executionID)
		if statusResponse.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", statusResponse.Code, http.StatusOK)
		}
		var payload executionResponse
		if err := json.NewDecoder(statusResponse.Body).Decode(&payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Status != lifecycle.StatusCompleted {
			t.Fatalf("status = %q, want %q", payload.Status, lifecycle.StatusCompleted)
		}
		if payload.Result == nil {
			t.Fatal("result = nil, want execution result")
		}
		if payload.Result.ExecutionID != executionID {
			t.Fatalf("result.ExecutionID = %q, want %q", payload.Result.ExecutionID, executionID)
		}
		if len(payload.Result.Requests) != 1 || payload.Result.Requests[0].RequestID != "req_1" {
			t.Fatalf("result requests = %+v, want preserved request IDs", payload.Result.Requests)
		}
		if _, ok := manager.Get(executionID); !ok {
			t.Fatal("manager.Get() returned not found for completed execution")
		}
	})

	t.Run("failed", func(t *testing.T) {
		executor := &fakeExecutor{err: errors.New("executor failed"), finished: make(chan struct{})}
		server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())
		response := serveRun(server.Handler(), "127.0.0.1:12345")
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", response.Code)
		}
		<-executor.finished
		waitForLifecycle(t, observer)
		executionID := executor.ID()
		statusResponse := serveGetExecution(server.Handler(), executionID)
		if statusResponse.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", statusResponse.Code, http.StatusOK)
		}
		var payload executionResponse
		if err := json.NewDecoder(statusResponse.Body).Decode(&payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Status != lifecycle.StatusFailed {
			t.Fatalf("status = %q, want %q", payload.Status, lifecycle.StatusFailed)
		}
		if payload.Error == "" {
			t.Fatal("error = empty string, want execution failure message")
		}
		if _, ok := manager.Get(executionID); !ok {
			t.Fatal("manager.Get() returned not found for failed execution")
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		executor := &fakeExecutor{started: make(chan struct{}, 1), release: make(chan struct{})}
		server, manager, _ := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())
		response := serveRun(server.Handler(), "127.0.0.1:12345")
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", response.Code)
		}
		<-executor.started
		executionID := executor.ID()
		if err := manager.Cancel(executionID); err != nil {
			t.Fatalf("manager.Cancel() error: %v", err)
		}
		statusResponse := serveGetExecution(server.Handler(), executionID)
		if statusResponse.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", statusResponse.Code, http.StatusOK)
		}
		var payload executionResponse
		if err := json.NewDecoder(statusResponse.Body).Decode(&payload); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if payload.Status != lifecycle.StatusCancelled {
			t.Fatalf("status = %q, want %q", payload.Status, lifecycle.StatusCancelled)
		}
		close(executor.release)
	})

	t.Run("method not allowed", func(t *testing.T) {
		server, _, _ := newTestServer(t, &fakeExecutor{}, policy.Default(), ratelimit.NewDefault())
		response := serveExecutionMethod(server.Handler(), http.MethodPost, "exec_123")
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
		}
		assertRecorderAPIError(t, response, ErrorCodeMethodNotAllowed, "method not allowed")
	})

	t.Run("malformed path", func(t *testing.T) {
		server, _, _ := newTestServer(t, &fakeExecutor{}, policy.Default(), ratelimit.NewDefault())
		for _, path := range []string{"/api/v1/executions", "/api/v1/executions/", "/api/v1/executions/foo/bar"} {
			response := serveCustomGet(server.Handler(), path)
			if response.Code != http.StatusNotFound {
				t.Fatalf("path %q: status = %d, want %d", path, response.Code, http.StatusNotFound)
			}
			assertRecorderAPIError(t, response, ErrorCodeExecutionNotFound, "execution not found")
		}
	})
}

func TestServerRunReturnsAcceptedWithMinimalResponse(t *testing.T) {
	executor := &fakeExecutor{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())

	response := serveRun(server.Handler(), "127.0.0.1:12345")
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}

	var body map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("response fields = %d, want 1", len(body))
	}
	var executionID string
	if err := json.Unmarshal(body["execution_id"], &executionID); err != nil {
		t.Fatalf("decode execution_id: %v", err)
	}
	if executionID == "" {
		t.Fatal("execution_id must not be empty")
	}
	<-executor.started
	snapshot, ok := manager.Get(executionID)
	if !ok || snapshot.Status != lifecycle.StatusRunning {
		t.Fatalf("snapshot = %+v, found = %v; want running", snapshot, ok)
	}
	close(executor.release)
	waitForLifecycle(t, observer)
}

func TestServerExecutionContinuesAfterAcceptedResponse(t *testing.T) {
	executor := &fakeExecutor{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())

	response := serveRun(server.Handler(), "127.0.0.1:12345")
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", response.Code)
	}
	<-executor.started
	if executor.ID() == "" {
		t.Fatal("executor did not receive execution ID")
	}

	close(executor.release)
	<-executor.finished
	waitForLifecycle(t, observer)
	if snapshot, _ := manager.Get(executor.ID()); snapshot.Status != lifecycle.StatusCompleted {
		t.Fatalf("status = %q, want completed", snapshot.Status)
	}
}

func TestServerRequestCancellationDoesNotCancelExecution(t *testing.T) {
	executor := &fakeExecutor{
		started:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())

	requestContext, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/run", strings.NewReader(validRequestBody())).WithContext(requestContext)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", response.Code)
	}

	<-executor.started
	cancel()
	select {
	case <-executor.finished:
		t.Fatal("request cancellation stopped the execution")
	default:
	}
	close(executor.release)
	<-executor.finished
	waitForLifecycle(t, observer)
	if snapshot, _ := manager.Get(executor.ID()); snapshot.Status != lifecycle.StatusCompleted {
		t.Fatalf("status = %q, want completed", snapshot.Status)
	}
}

func TestServerAsyncFailureAndTimeout(t *testing.T) {
	t.Run("executor failure", func(t *testing.T) {
		executor := &fakeExecutor{err: errors.New("executor failed"), finished: make(chan struct{})}
		server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())
		response := serveRun(server.Handler(), "127.0.0.1:12345")
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", response.Code)
		}
		<-executor.finished
		waitForLifecycle(t, observer)
		snapshot, _ := manager.Get(executor.ID())
		if snapshot.Status != lifecycle.StatusFailed || snapshot.Error != "executor failed" {
			t.Fatalf("snapshot = %+v", snapshot)
		}
	})

	t.Run("policy timeout", func(t *testing.T) {
		executor := &fakeExecutor{started: make(chan struct{}, 1), release: make(chan struct{})}
		executionPolicy := policy.Default()
		executionPolicy.MaxExecutionDuration = time.Millisecond
		server, manager, observer := newTestServer(t, executor, executionPolicy, ratelimit.NewDefault())
		response := serveRun(server.Handler(), "127.0.0.1:12345")
		if response.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202", response.Code)
		}
		<-executor.started
		waitForLifecycle(t, observer)
		snapshot, _ := manager.Get(executor.ID())
		if snapshot.Status != lifecycle.StatusFailed || snapshot.Error != context.DeadlineExceeded.Error() {
			t.Fatalf("snapshot = %+v", snapshot)
		}
	})
}

func TestServerCompletedEmbeddedFailuresReturnAccepted(t *testing.T) {
	for name, result := range map[string]execution.Result{
		"http failure":    {Requests: []execution.RequestResult{{StatusCode: http.StatusServiceUnavailable}}},
		"request failure": {Requests: []execution.RequestResult{{StatusCode: 0, Error: "connection refused"}}},
	} {
		t.Run(name, func(t *testing.T) {
			executor := &fakeExecutor{result: result, finished: make(chan struct{})}
			server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.NewDefault())
			response := serveRun(server.Handler(), "127.0.0.1:12345")
			if response.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", response.Code)
			}
			<-executor.finished
			waitForLifecycle(t, observer)
			snapshot, _ := manager.Get(executor.ID())
			if snapshot.Status != lifecycle.StatusCompleted {
				t.Fatalf("status = %q, want completed", snapshot.Status)
			}
		})
	}
}

func TestServerStartFailureReturnsInternalError(t *testing.T) {
	manager, err := lifecycle.New(10)
	if err != nil {
		t.Fatalf("lifecycle.New() error: %v", err)
	}
	server, err := New(failingStarter{err: errors.New("start failed")}, policy.Default(), ratelimit.New(1, 100, time.Hour), manager)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	response := serveRun(server.Handler(), "127.0.0.1:12345")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.Code)
	}
	assertRecorderAPIError(t, response, ErrorCodeInternal, "internal server error")

	second := serveRun(server.Handler(), "127.0.0.1:12345")
	if second.Code != http.StatusInternalServerError {
		t.Fatalf("second status = %d, want 500", second.Code)
	}
}

func TestServerRateLimitFollowsExecutionLifetime(t *testing.T) {
	executor := &fakeExecutor{started: make(chan struct{}, 2), release: make(chan struct{})}
	server, manager, observer := newTestServer(t, executor, policy.Default(), ratelimit.New(1, 100, time.Hour))
	handler := server.Handler()

	first := serveRun(handler, "127.0.0.1:12345")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", first.Code)
	}
	<-executor.started

	second := serveRun(handler, "127.0.0.1:54321")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429", second.Code)
	}

	close(executor.release)
	waitForLifecycle(t, observer)
	if snapshot, _ := manager.Get(executor.ID()); snapshot.Status != lifecycle.StatusCompleted {
		t.Fatalf("first status = %q, want completed", snapshot.Status)
	}

	third := serveRun(handler, "127.0.0.1:67890")
	if third.Code != http.StatusAccepted {
		t.Fatalf("third status = %d, want 202", third.Code)
	}
}

func TestServerValidationAndMethodErrors(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		body       string
		wantStatus int
		wantCode   ErrorCode
	}{
		{"method", http.MethodGet, "", http.StatusMethodNotAllowed, ErrorCodeMethodNotAllowed},
		{"json", http.MethodPost, `{"service":`, http.StatusBadRequest, ErrorCodeInvalidJSON},
		{"scenario", http.MethodPost, `{"service":"payments","operation":"get_user"}`, http.StatusBadRequest, ErrorCodeInvalidScenario},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _, _ := newTestServer(t, &fakeExecutor{}, policy.Default(), ratelimit.NewDefault())
			req := httptest.NewRequest(test.method, "/api/v1/run", strings.NewReader(test.body))
			req.RemoteAddr = "127.0.0.1:12345"
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, req)
			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.Code, test.wantStatus)
			}
			assertRecorderAPIError(t, response, test.wantCode, "")
		})
	}
}

func newTestServer(t *testing.T, executor executionservice.Executor, executionPolicy policy.Policy, limiter *ratelimit.Limiter) (*Server, *lifecycle.Manager, *observedLifecycle) {
	t.Helper()
	manager, err := lifecycle.New(10)
	if err != nil {
		t.Fatalf("lifecycle.New() error: %v", err)
	}
	observed := &observedLifecycle{manager: manager, done: make(chan struct{})}
	service, err := executionservice.New(executor, observed)
	if err != nil {
		t.Fatalf("executionservice.New() error: %v", err)
	}
	server, err := New(service, executionPolicy, limiter, manager)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return server, manager, observed
}

func serveRun(handler http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/run", strings.NewReader(validRequestBody()))
	req.RemoteAddr = remoteAddr
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func serveGetExecution(handler http.Handler, executionID string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/executions/"+executionID, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func serveExecutionMethod(handler http.Handler, method, executionID string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/api/v1/executions/"+executionID, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func serveCustomGet(handler http.Handler, path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func waitForLifecycle(t *testing.T, observer *observedLifecycle) {
	t.Helper()
	select {
	case <-observer.done:
		return
	case <-time.After(time.Second):
		t.Fatal("execution did not finish")
	}
}

func validRequestBody() string {
	return `{"service":"users","operation":"get_user","simulation":"normal","request_size":"0b","response_size":"1kb"}`
}

func assertRecorderAPIError(t *testing.T, response *httptest.ResponseRecorder, code ErrorCode, message string) {
	t.Helper()
	var body errorResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode API error: %v", err)
	}
	if body.Error.Code != code {
		t.Fatalf("error code = %q, want %q", body.Error.Code, code)
	}
	if message != "" && body.Error.Message != message {
		t.Fatalf("error message = %q, want %q", body.Error.Message, message)
	}
}
