package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
)

type fakeExecutor struct {
	result execution.Result
	err    error

	called   bool
	scenario demo.Scenario
}

func (f *fakeExecutor) Execute(
	ctx context.Context,
	scenario demo.Scenario,
) (execution.Result, error) {
	f.called = true
	f.scenario = scenario

	return f.result, f.err
}

func TestServerHealth(t *testing.T) {
	executor := &fakeExecutor{}

	server, err := New(executor)
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

	server, err := New(executor)
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

	if len(response.Requests) != 2 {
		t.Fatalf(
			"requests = %d, want %d",
			len(response.Requests),
			2,
		)
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

	server, err := New(executor)
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

	server, err := New(executor)
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

	server, err := New(executor)
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

	server, err := New(executor)
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
		err: context.DeadlineExceeded,
	}

	server, err := New(executor)
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

	assertAPIError(t, resp, ErrorCodeExecutionTimeout, "scenario execution timed out")
}

func TestServerUpstreamError(t *testing.T) {
	server := newTestServer(t, &fakeExecutor{
		result: execution.Result{
			Requests: []execution.RequestResult{{StatusCode: 0, Error: "connection refused"}},
		},
	})
	defer server.Close()

	resp := postRun(t, server, validRequestBody())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	assertAPIError(t, resp, ErrorCodeUpstreamError, "upstream request failed")
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
	server, err := New(executor)
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
