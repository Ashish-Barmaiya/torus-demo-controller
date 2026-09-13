package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/executionservice"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/identity"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/ratelimit"
)

type ErrorCode string

const (
	ErrorCodeInvalidJSON        ErrorCode = "INVALID_JSON"
	ErrorCodeInvalidScenario    ErrorCode = "INVALID_SCENARIO"
	ErrorCodePolicyRejected     ErrorCode = "POLICY_REJECTED"
	ErrorCodeRateLimited        ErrorCode = "RATE_LIMITED"
	ErrorCodeExecutionTimeout   ErrorCode = "EXECUTION_TIMEOUT"
	ErrorCodeUpstreamError      ErrorCode = "UPSTREAM_ERROR"
	ErrorCodeInternal           ErrorCode = "INTERNAL_ERROR"
	ErrorCodeMethodNotAllowed   ErrorCode = "METHOD_NOT_ALLOWED"
	ErrorCodeExecutionNotFound  ErrorCode = "EXECUTION_NOT_FOUND"
	ErrorCodeExecutionNotActive ErrorCode = "EXECUTION_NOT_ACTIVE"
)

type APIError struct {
	Code    ErrorCode
	Message string
}

func (e APIError) Error() string {
	return e.Message
}

type errorResponse struct {
	Error apiErrorBody `json:"error"`
}

type apiErrorBody struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

type Server struct {
	executionService ExecutionStarter
	manager          *lifecycle.Manager
	policy           policy.Policy
	limiter          *ratelimit.Limiter
}

type ExecutionStarter interface {
	StartWithCompletion(
		string,
		demo.Scenario,
		time.Duration,
		func(),
	) error
	Cancel(string) error
}

func New(
	executionService ExecutionStarter,
	executionPolicy policy.Policy,
	limiter *ratelimit.Limiter,
	manager *lifecycle.Manager,
) (*Server, error) {
	if executionService == nil {
		return nil, fmt.Errorf("execution service must not be nil")
	}
	if limiter == nil {
		return nil, fmt.Errorf("limiter must not be nil")
	}
	if manager == nil {
		return nil, fmt.Errorf("lifecycle manager must not be nil")
	}

	return &Server{
		executionService: executionService,
		manager:          manager,
		policy:           executionPolicy,
		limiter:          limiter,
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/run", s.handleRun)
	mux.HandleFunc("/api/v1/executions/", s.handleExecution)
	mux.HandleFunc("/api/v1/executions", s.handleExecution)

	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, APIError{
			Code:    ErrorCodeMethodNotAllowed,
			Message: "method not allowed",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

type startResponse struct {
	ExecutionID string `json:"execution_id"`
}

type cancelResponse struct {
	ExecutionID string `json:"execution_id"`
}

type executionResponse struct {
	ExecutionID string            `json:"execution_id"`
	Scenario    demo.Scenario     `json:"scenario"`
	Status      lifecycle.Status  `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	StartedAt   *time.Time        `json:"started_at"`
	CompletedAt *time.Time        `json:"completed_at"`
	Result      *execution.Result `json:"result"`
	Error       string            `json:"error"`
}

func (s *Server) handleExecution(w http.ResponseWriter, r *http.Request) {
	if id, ok := s.parseExecutionStatusID(r); ok {
		if r.Method != http.MethodGet {
			writeAPIError(w, http.StatusMethodNotAllowed, APIError{
				Code:    ErrorCodeMethodNotAllowed,
				Message: "method not allowed",
			})
			return
		}
		s.handleExecutionStatusForID(w, id)
		return
	}
	if id, ok := s.parseCancelExecutionID(r); ok {
		if r.Method != http.MethodPost {
			writeAPIError(w, http.StatusMethodNotAllowed, APIError{
				Code:    ErrorCodeMethodNotAllowed,
				Message: "method not allowed",
			})
			return
		}
		s.handleCancelExecutionForID(w, id)
		return
	}

	writeAPIError(w, http.StatusNotFound, APIError{
		Code:    ErrorCodeExecutionNotFound,
		Message: "execution not found",
	})
}

func (s *Server) handleExecutionStatusForID(w http.ResponseWriter, executionID string) {
	snapshot, found := s.manager.Get(executionID)
	if !found {
		writeAPIError(w, http.StatusNotFound, APIError{
			Code:    ErrorCodeExecutionNotFound,
			Message: "execution not found",
		})
		return
	}

	response := executionResponse{
		ExecutionID: snapshot.ID,
		Scenario:    snapshot.Scenario,
		Status:      snapshot.Status,
		CreatedAt:   snapshot.CreatedAt,
		Result:      snapshot.Result,
		Error:       snapshot.Error,
	}

	if !snapshot.StartedAt.IsZero() {
		startedAt := snapshot.StartedAt
		response.StartedAt = &startedAt
	}
	if !snapshot.CompletedAt.IsZero() {
		completedAt := snapshot.CompletedAt
		response.CompletedAt = &completedAt
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelExecutionForID(w http.ResponseWriter, executionID string) {
	err := s.executionService.Cancel(executionID)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, cancelResponse{ExecutionID: executionID})
	case errors.Is(err, executionservice.ErrExecutionNotFound):
		writeAPIError(w, http.StatusNotFound, APIError{
			Code:    ErrorCodeExecutionNotFound,
			Message: "execution not found",
		})
	case errors.Is(err, executionservice.ErrExecutionNotActive):
		writeAPIError(w, http.StatusConflict, APIError{
			Code:    ErrorCodeExecutionNotActive,
			Message: "execution not active",
		})
	default:
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "internal server error",
		})
	}
}

func (s *Server) parseExecutionStatusID(r *http.Request) (string, bool) {
	if r == nil {
		return "", false
	}

	path := r.URL.Path
	prefix := "/api/v1/executions"

	if path == prefix || path == prefix+"/" {
		return "", false
	}
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}

	remainder := strings.TrimPrefix(path, prefix)
	if remainder == "" || remainder == "/" {
		return "", false
	}
	if !strings.HasPrefix(remainder, "/") {
		return "", false
	}

	segments := strings.Split(strings.Trim(remainder, "/"), "/")
	if len(segments) != 1 || segments[0] == "" {
		return "", false
	}

	return segments[0], true
}

func (s *Server) parseCancelExecutionID(r *http.Request) (string, bool) {
	if r == nil {
		return "", false
	}

	path := r.URL.Path
	prefix := "/api/v1/executions"

	if !strings.HasPrefix(path, prefix) {
		return "", false
	}

	remainder := strings.TrimPrefix(path, prefix)
	if remainder == "" || remainder == "/" {
		return "", false
	}
	if !strings.HasPrefix(remainder, "/") {
		return "", false
	}

	segments := strings.Split(strings.Trim(remainder, "/"), "/")
	if len(segments) != 2 || segments[0] == "" || segments[1] != "cancel" {
		return "", false
	}

	return segments[0], true
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, APIError{
			Code:    ErrorCodeMethodNotAllowed,
			Message: "method not allowed",
		})
		return
	}

	defer r.Body.Close()

	var request demo.RunScenarioRequest

	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&request); err != nil {
		writeAPIError(w, http.StatusBadRequest, APIError{
			Code:    ErrorCodeInvalidJSON,
			Message: "invalid JSON body",
		})
		return
	}

	scenario := request.Scenario()

	if err := scenario.Validate(); err != nil {
		writeAPIError(w, http.StatusBadRequest, APIError{
			Code:    ErrorCodeInvalidScenario,
			Message: err.Error(),
		})
		return
	}

	if err := s.policy.Validate(scenario); err != nil {
		writeAPIError(w, http.StatusBadRequest, APIError{
			Code:    ErrorCodePolicyRejected,
			Message: err.Error(),
		})
		return
	}

	client := clientKey(r)
	if !s.limiter.Allow(client) {
		writeAPIError(w, http.StatusTooManyRequests, APIError{
			Code:    ErrorCodeRateLimited,
			Message: "rate limit exceeded",
		})
		return
	}

	executionID, err := identity.NewExecutionID()
	if err != nil {
		s.limiter.Done(client)
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "failed to create execution ID",
		})
		return
	}

	err = s.executionService.StartWithCompletion(
		executionID,
		scenario,
		s.policy.MaxExecutionDuration,
		func() {
			s.limiter.Done(client)
		},
	)
	if err != nil {
		s.limiter.Done(client)
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "internal server error",
		})
		return
	}

	writeJSON(w, http.StatusAccepted, startResponse{ExecutionID: executionID})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, apiErr APIError) {
	writeJSON(w, status, errorResponse{
		Error: apiErrorBody(apiErr),
	})
}

func apiErrorStatus(apiErr APIError) int {
	switch apiErr.Code {
	case ErrorCodeInvalidJSON, ErrorCodeInvalidScenario, ErrorCodePolicyRejected:
		return http.StatusBadRequest
	case ErrorCodeRateLimited:
		return http.StatusTooManyRequests
	case ErrorCodeExecutionTimeout:
		return http.StatusGatewayTimeout
	case ErrorCodeUpstreamError:
		return http.StatusBadGateway
	case ErrorCodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	case ErrorCodeExecutionNotFound:
		return http.StatusNotFound
	case ErrorCodeExecutionNotActive:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func clientKey(r *http.Request) string {
	if r == nil {
		return "unknown"
	}

	if r.RemoteAddr == "" {
		return "unknown"
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}

	if ip := net.ParseIP(r.RemoteAddr); ip != nil {
		return ip.String()
	}

	if strings.HasPrefix(r.RemoteAddr, "[") && strings.Contains(r.RemoteAddr, "]") {
		return strings.Trim(r.RemoteAddr, "[]")
	}

	return r.RemoteAddr
}
