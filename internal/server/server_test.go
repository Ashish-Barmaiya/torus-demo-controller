package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/ratelimit"
)

type fakeExecutor struct {
	result      execution.Result
	err         error
	executionID string

	called   bool
	scenario demo.Scenario
	ctx      context.Context
	block    bool
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	err     error
}

func (e *blockingExecutor) Execute(
	ctx context.Context,
	executionID string,
	scenario demo.Scenario,
) (execution.Result, error) {
	e.started <- struct{}{}
	select {
	case <-e.release:
	case <-ctx.Done():
		return execution.Result{}, ctx.Err()
	}

	return execution.Result{
		ExecutionID: executionID,
		Scenario:    scenario,
	}, e.err
}

func (f *fakeExecutor) Execute(
	ctx context.Context,
	executionID string,
	scenario demo.Scenario,
) (execution.Result, error) {
	f.called = true
	f.executionID = executionID
	f.scenario = scenario
	f.ctx = ctx
	f.result.ExecutionID = executionID

	for i := range f.result.Requests {
		if f.result.Requests[i].RequestID == "" {
			f.result.Requests[i].RequestID = fmt.Sprintf("req_%d", i)
		}
	}

	if f.block {
		<-ctx.Done()
		return f.result, ctx.Err()
	}

	return f.result, f.err
}

func TestServerHealth(t *testing.T) {
	executor := &fakeExecutor{}

	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequest(
		http.MethodGet,
		ts.URL+"/health",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusOK,
		)
	}

	var body map[string]string

	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if body["status"] != "ok" {
		t.Fatalf(
			"status body = %q, want %q",
			body["status"],
			"ok",
		)
	}
}

func TestServerRun(t *testing.T) {
	executor := &fakeExecutor{
		result: execution.Result{
			Scenario: demo.Scenario{
				Service:      demo.ServiceUsers,
				Operation:    demo.OperationGetUser,
				Simulation:   demo.SimulationNormal,
				RequestSize:  demo.RequestSizeNone,
				ResponseSize: demo.ResponseSize1KB,
				RequestCount: 2,
			},
			TotalDuration: 150 * time.Millisecond,
			Requests: []execution.RequestResult{
				{
					Index:      0,
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					BodySize:   1024,
					Duration:   75 * time.Millisecond,
				},
				{
					Index:      1,
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					BodySize:   1024,
					Duration:   74 * time.Millisecond,
				},
			},
		},
	}

	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	requestBody := `{
		"service": "users",
		"operation": "get_user",
		"simulation": "normal",
		"request_size": "0b",
		"response_size": "1kb",
		"request_count": 2
	}`

	req, err := http.NewRequest(
		http.MethodPost,
		ts.URL+"/api/v1/run",
		jsonBody(requestBody),
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusOK,
		)
	}

	if !executor.called {
		t.Fatal("executor was not called")
	}

	if executor.executionID == "" {
		t.Fatal("executor did not receive a non-empty execution ID")
	}

	if executor.scenario.RequestCount != 2 {
		t.Fatalf(
			"request count = %d, want %d",
			executor.scenario.RequestCount,
			2,
		)
	}

	var response runResponse

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.Scenario.Operation != demo.OperationGetUser {
		t.Fatalf(
			"operation = %q, want %q",
			response.Scenario.Operation,
			demo.OperationGetUser,
		)
	}

	if response.ExecutionID == "" {
		t.Fatal("execution_id must not be empty")
	}

	if response.ExecutionID != executor.executionID {
		t.Fatalf("execution_id = %q, want %q", response.ExecutionID, executor.executionID)
	}

	if len(response.Requests) != 2 {
		t.Fatalf(
			"requests = %d, want %d",
			len(response.Requests),
			2,
		)
	}

	seen := make(map[string]struct{}, len(response.Requests))
	for _, requestResult := range response.Requests {
		if requestResult.RequestID == "" {
			t.Fatal("request_id must not be empty")
		}
		if _, exists := seen[requestResult.RequestID]; exists {
			t.Fatalf("duplicate request ID: %s", requestResult.RequestID)
		}
		seen[requestResult.RequestID] = struct{}{}
	}

	if response.Requests[0].BodySize != 1024 {
		t.Fatalf(
			"request 0 body size = %d, want 1024",
			response.Requests[0].BodySize,
		)
	}
}

func TestServerRunDefaultsRequestCount(t *testing.T) {
	executor := &fakeExecutor{
		result: execution.Result{
			Scenario: demo.Scenario{
				Service:      demo.ServiceUsers,
				Operation:    demo.OperationGetUser,
				Simulation:   demo.SimulationNormal,
				RequestSize:  demo.RequestSizeNone,
				ResponseSize: demo.ResponseSize1KB,
				RequestCount: 1,
			},
		},
	}

	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	requestBody := `{
		"service": "users",
		"operation": "get_user",
		"simulation": "normal",
		"request_size": "0b",
		"response_size": "1kb"
	}`

	req, err := http.NewRequest(
		http.MethodPost,
		ts.URL+"/api/v1/run",
		jsonBody(requestBody),
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusOK,
		)
	}

	if executor.scenario.RequestCount != 1 {
		t.Fatalf(
			"request count = %d, want %d",
			executor.scenario.RequestCount,
			1,
		)
	}
}

func TestServerRunRejectsInvalidScenario(t *testing.T) {
	executor := &fakeExecutor{}

	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	requestBody := `{
		"service": "users",
		"operation": "get_order",
		"simulation": "normal",
		"response_size": "1kb"
	}`

	req, err := http.NewRequest(
		http.MethodPost,
		ts.URL+"/api/v1/run",
		jsonBody(requestBody),
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusBadRequest,
		)
	}

	assertAPIError(t, resp, ErrorCodeInvalidScenario, `operation "get_order" does not belong to service "users"`)

	if executor.called {
		t.Fatal("executor must not be called for invalid scenario")
	}
}

func TestServerRunRejectsInvalidService(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{})
	defer server.Close()

	resp := postRun(t, server, `{"service":"payments","operation":"get_user","simulation":"normal","request_size":"0b","response_size":"1kb"}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	assertAPIError(t, resp, ErrorCodeInvalidScenario, `unsupported service "payments"`)
}

func TestServerRunRejectsInvalidJSON(t *testing.T) {
	executor := &fakeExecutor{}

	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequest(
		http.MethodPost,
		ts.URL+"/api/v1/run",
		jsonBody(`{"service":`),
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusBadRequest,
		)
	}

	assertAPIError(t, resp, ErrorCodeInvalidJSON, "invalid JSON body")

	if executor.called {
		t.Fatal("executor must not be called for invalid JSON")
	}
}

func TestServerMethodNotAllowed(t *testing.T) {
	executor := &fakeExecutor{}

	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	req, err := http.NewRequest(
		http.MethodGet,
		ts.URL+"/api/v1/run",
		nil,
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusMethodNotAllowed,
		)
	}

	if executor.called {
		t.Fatal("executor must not be called")
	}
}

func TestServerExecutorError(t *testing.T) {
	executor := &fakeExecutor{
		block: true,
	}
	executionPolicy := policy.Default()
	executionPolicy.MaxExecutionDuration = 20 * time.Millisecond

	server, err := New(executor, executionPolicy)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	requestBody := `{
		"service": "users",
		"operation": "get_user",
		"simulation": "normal",
		"request_size": "0b",
		"response_size": "1kb"
	}`

	req, err := http.NewRequest(
		http.MethodPost,
		ts.URL+"/api/v1/run",
		jsonBody(requestBody),
	)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf(
			"status = %d, want %d",
			resp.StatusCode,
			http.StatusGatewayTimeout,
		)
	}

	assertAPIError(t, resp, ErrorCodeExecutionTimeout, "execution timed out")

	if executor.ctx == nil {
		t.Fatal("executor did not receive a context")
	}
	if _, ok := executor.ctx.Deadline(); !ok {
		t.Fatal("executor context has no deadline")
	}
}

func TestServerPolicyRejection(t *testing.T) {
	executor := &fakeExecutor{}
	executionPolicy := policy.Default()
	executionPolicy.MaxRequests = 1

	server, err := New(executor, executionPolicy)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()

	resp := postRun(t, ts, `{"service":"users","operation":"get_user","simulation":"normal","request_size":"0b","response_size":"1kb","request_count":2}`)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	assertAPIError(t, resp, ErrorCodePolicyRejected, "request count 2 exceeds maximum 1")
	if executor.called {
		t.Fatal("executor must not be called when policy rejects scenario")
	}
}

func TestServerPreservesRequestNetworkError(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{
		result: execution.Result{
			Requests: []execution.RequestResult{
				{StatusCode: http.StatusOK, Status: "200 OK"},
				{StatusCode: 0, Error: "connection refused"},
				{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable"},
			},
		},
	})
	defer server.Close()

	resp := postRun(t, server, validRequestBody())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body runResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Requests) != 3 {
		t.Fatalf("requests = %d, want 3", len(body.Requests))
	}
	if body.Requests[0].StatusCode != http.StatusOK || body.Requests[0].Error != "" {
		t.Fatalf("successful request = %+v", body.Requests[0])
	}
	if body.Requests[1].StatusCode != 0 || body.Requests[1].Error != "connection refused" {
		t.Fatalf("failed request = %+v", body.Requests[1])
	}
	if body.Requests[2].StatusCode != http.StatusServiceUnavailable || body.Requests[2].Error != "" {
		t.Fatalf("HTTP failure request = %+v", body.Requests[2])
	}
}

func TestServerPreservesAllRequestNetworkErrors(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{
		result: execution.Result{
			Requests: []execution.RequestResult{
				{StatusCode: 0, Error: "connection refused"},
				{StatusCode: 0, Error: "context deadline exceeded"},
			},
		},
	})
	defer server.Close()

	resp := postRun(t, server, validRequestBody())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestServerInternalError(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{err: errors.New("unexpected failure")})
	defer server.Close()

	resp := postRun(t, server, validRequestBody())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	assertAPIError(t, resp, ErrorCodeInternal, "internal server error")
}

func TestServerRateLimitRejectsSameClient(t *testing.T) {
	executor := &fakeExecutor{}
	server, err := New(executor, policy.Default(), ratelimit.New(1, 1, time.Hour))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	first := serveRun(server.Handler(), "127.0.0.1:12345")
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d", first.Code, http.StatusOK)
	}

	second := serveRun(server.Handler(), "127.0.0.1:54321")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want %d", second.Code, http.StatusTooManyRequests)
	}
	assertRecorderAPIError(t, second, ErrorCodeRateLimited, "rate limit exceeded")
	if !executor.called {
		t.Fatal("executor should be called for the admitted request")
	}
}

func TestServerRateLimitUsesIndependentClientIPs(t *testing.T) {
	executor := &blockingExecutor{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	server, err := New(executor, policy.Default(), ratelimit.New(1, 100, time.Hour))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	results := make(chan *httptest.ResponseRecorder, 2)
	go func() { results <- serveRun(server.Handler(), "127.0.0.1:12345") }()
	go func() { results <- serveRun(server.Handler(), "127.0.0.2:12345") }()

	<-executor.started
	<-executor.started
	close(executor.release)

	for range 2 {
		if response := <-results; response.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
		}
	}
}

func TestServerRateLimitReleasesSlotAfterExecutionError(t *testing.T) {
	executor := &fakeExecutor{
		err: errors.New("unexpected failure"),
	}

	server, err := New(
		executor,
		policy.Default(),
		ratelimit.New(1, 100, time.Hour),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	handler := server.Handler()

	first := serveRun(handler, "127.0.0.1:12345")
	if first.Code != http.StatusInternalServerError {
		t.Fatalf(
			"first status = %d, want %d",
			first.Code,
			http.StatusInternalServerError,
		)
	}

	second := serveRun(handler, "127.0.0.1:54321")
	if second.Code != http.StatusInternalServerError {
		t.Fatalf(
			"second status = %d, want %d",
			second.Code,
			http.StatusInternalServerError,
		)
	}
}

func TestServerDefaultLimiterIsSharedByHandler(t *testing.T) {
	server, err := New(&fakeExecutor{}, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	for i := 0; i < ratelimit.DefaultMaxConcurrentExecutions; i++ {
		if !server.limiter.Allow("127.0.0.1") {
			t.Fatalf("direct admission %d should succeed", i)
		}
	}
	defer func() {
		for i := 0; i < ratelimit.DefaultMaxConcurrentExecutions; i++ {
			server.limiter.Done("127.0.0.1")
		}
	}()

	response := serveRun(server.Handler(), "127.0.0.1:12345")
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusTooManyRequests)
	}
}

func TestClientKey(t *testing.T) {
	tests := []struct {
		remoteAddr string
		want       string
	}{
		{remoteAddr: "127.0.0.1:12345", want: "127.0.0.1"},
		{remoteAddr: "[::1]:54321", want: "::1"},
		{remoteAddr: "127.0.0.1", want: "127.0.0.1"},
		{remoteAddr: "unusual", want: "unusual"},
	}

	for _, test := range tests {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/run", nil)
		req.RemoteAddr = test.remoteAddr
		if got := clientKey(req); got != test.want {
			t.Errorf("clientKey(%q) = %q, want %q", test.remoteAddr, got, test.want)
		}
	}
}

func TestServerCompletedUpstreamResponse(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{
		result: execution.Result{
			Scenario: demo.Scenario{Service: demo.ServiceUsers, Operation: demo.OperationGetUser},
			Requests: []execution.RequestResult{{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
			}},
		},
	})
	defer server.Close()

	resp := postRun(t, server, validRequestBody())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body runResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Requests) != 1 || body.Requests[0].StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("requests = %+v, want one 503 result", body.Requests)
	}
	if body.Requests[0].Error != "" {
		t.Fatalf("completed response error = %q, want empty", body.Requests[0].Error)
	}
}

func TestServerMethodNotAllowedReturnsJSONError(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{})
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/run", nil)
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
	assertAPIError(t, resp, ErrorCodeMethodNotAllowed, "method not allowed")
}

func newTestServer(t *testing.T, executor Executor) *httptest.Server {
	t.Helper()
	server, err := New(executor, policy.Default())
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return httptest.NewServer(server.Handler())
}

func newTestServerWithLimiter(t *testing.T, executor Executor, limiter *ratelimit.Limiter) *httptest.Server {
	t.Helper()
	server, err := New(executor, policy.Default(), limiter)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return httptest.NewServer(server.Handler())
}

func validRequestBody() string {
	return `{"service":"users","operation":"get_user","simulation":"normal","request_size":"0b","response_size":"1kb"}`
}

func postRun(t *testing.T, server *httptest.Server, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/api/v1/run", jsonBody(body))
	if err != nil {
		t.Fatalf("NewRequest() error: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	return resp
}

func assertAPIError(t *testing.T, resp *http.Response, code ErrorCode, message string) {
	t.Helper()
	if contentType := resp.Header.Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("content type = %q, want application/json", contentType)
	}

	var body errorResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode API error: %v", err)
	}
	if body.Error.Code != code {
		t.Fatalf("error code = %q, want %q", body.Error.Code, code)
	}
	if body.Error.Message != message {
		t.Fatalf("error message = %q, want %q", body.Error.Message, message)
	}
}

func jsonBody(value string) *strings.Reader {
	return strings.NewReader(value)
}

func serveRun(handler http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/run", strings.NewReader(validRequestBody()))
	req.RemoteAddr = remoteAddr
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func assertRecorderAPIError(t *testing.T, response *httptest.ResponseRecorder, code ErrorCode, message string) {
	t.Helper()
	var body errorResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode API error: %v", err)
	}
	if body.Error.Code != code || body.Error.Message != message {
		t.Fatalf("error = %+v, want code %q and message %q", body.Error, code, message)
	}
}

func TestServerRateLimitRejectsConcurrentExecutionForSameClient(t *testing.T) {
	executor := &blockingExecutor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}

	server, err := New(
		executor,
		policy.Default(),
		ratelimit.New(1, 100, time.Hour),
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	handler := server.Handler()

	firstResult := make(chan *httptest.ResponseRecorder, 1)

	go func() {
		firstResult <- serveRun(
			handler,
			"127.0.0.1:12345",
		)
	}()

	<-executor.started

	second := serveRun(
		handler,
		"127.0.0.1:54321",
	)

	if second.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"second status = %d, want %d",
			second.Code,
			http.StatusTooManyRequests,
		)
	}

	assertRecorderAPIError(
		t,
		second,
		ErrorCodeRateLimited,
		"rate limit exceeded",
	)

	close(executor.release)

	first := <-firstResult

	if first.Code != http.StatusOK {
		t.Fatalf(
			"first status = %d, want %d",
			first.Code,
			http.StatusOK,
		)
	}
}

func TestServerPolicyRejectionDoesNotConsumeRateLimit(t *testing.T) {
	executor := &fakeExecutor{}

	executionPolicy := policy.Default()
	executionPolicy.MaxRequests = 1

	limiter := ratelimit.New(1, 1, time.Hour)

	server, err := New(
		executor,
		executionPolicy,
		limiter,
	)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/run",
		strings.NewReader(
			`{"service":"users","operation":"get_user","simulation":"normal","request_size":"0b","response_size":"1kb","request_count":2}`,
		),
	)
	req.RemoteAddr = "127.0.0.1:12345"
	req.Header.Set("Content-Type", "application/json")

	first := httptest.NewRecorder()
	server.Handler().ServeHTTP(first, req)

	if first.Code != http.StatusBadRequest {
		t.Fatalf(
			"status = %d, want %d",
			first.Code,
			http.StatusBadRequest,
		)
	}

	assertRecorderAPIError(
		t,
		first,
		ErrorCodePolicyRejected,
		"request count 2 exceeds maximum 1",
	)

	allowed := serveRun(
		server.Handler(),
		"127.0.0.1:12345",
	)

	if allowed.Code != http.StatusOK {
		t.Fatalf(
			"second status = %d, want %d",
			allowed.Code,
			http.StatusOK,
		)
	}
}
