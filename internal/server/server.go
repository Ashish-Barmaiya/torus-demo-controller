package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/identity"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/lifecycle"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/ratelimit"
)

type Executor interface {
	Execute(
		context.Context,
		string,
		demo.Scenario,
	) (execution.Result, error)
}

type ErrorCode string

const (
	ErrorCodeInvalidJSON      ErrorCode = "INVALID_JSON"
	ErrorCodeInvalidScenario  ErrorCode = "INVALID_SCENARIO"
	ErrorCodePolicyRejected   ErrorCode = "POLICY_REJECTED"
	ErrorCodeRateLimited      ErrorCode = "RATE_LIMITED"
	ErrorCodeExecutionTimeout ErrorCode = "EXECUTION_TIMEOUT"
	ErrorCodeUpstreamError    ErrorCode = "UPSTREAM_ERROR"
	ErrorCodeInternal         ErrorCode = "INTERNAL_ERROR"
	ErrorCodeMethodNotAllowed ErrorCode = "METHOD_NOT_ALLOWED"
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
	executor         Executor
	policy           policy.Policy
	limiter          *ratelimit.Limiter
	lifecycleManager *lifecycle.Manager
}

func New(
	executor Executor,
	executionPolicy policy.Policy,
	limiter *ratelimit.Limiter,
	lifecycleManager *lifecycle.Manager,
) (*Server, error) {
	if executor == nil {
		return nil, fmt.Errorf("executor must not be nil")
	}
	if limiter == nil {
		return nil, fmt.Errorf("limiter must not be nil")
	}
	if lifecycleManager == nil {
		return nil, fmt.Errorf("lifecycle manager must not be nil")
	}

	return &Server{
		executor:         executor,
		policy:           executionPolicy,
		limiter:          limiter,
		lifecycleManager: lifecycleManager,
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/v1/run", s.handleRun)

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

type runResponse struct {
	ExecutionID   string           `json:"execution_id"`
	Scenario      demo.Scenario    `json:"scenario"`
	TotalDuration time.Duration    `json:"total_duration"`
	Requests      []requestSummary `json:"requests"`
}

type requestSummary struct {
	RequestID  string        `json:"request_id"`
	Index      int           `json:"index"`
	StatusCode int           `json:"status_code"`
	Status     string        `json:"status"`
	BodySize   int64         `json:"body_size"`
	Duration   time.Duration `json:"duration"`
	Error      string        `json:"error,omitempty"`
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
	defer s.limiter.Done(client)

	executionID, err := identity.NewExecutionID()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "failed to create execution ID",
		})
		return
	}

	if _, err := s.lifecycleManager.Create(executionID, scenario); err != nil {
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "failed to create execution",
		})
		return
	}

	if err := s.lifecycleManager.Start(executionID); err != nil {
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "failed to start execution",
		})
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		s.policy.MaxExecutionDuration,
	)
	defer cancel()

	result, err := s.executor.Execute(
		ctx,
		executionID,
		scenario,
	)

	if errors.Is(ctx.Err(), context.DeadlineExceeded) &&
		!errors.Is(r.Context().Err(), context.DeadlineExceeded) {
		if lifecycleErr := s.lifecycleManager.Fail(executionID, context.DeadlineExceeded); lifecycleErr != nil {
			writeAPIError(w, http.StatusInternalServerError, APIError{
				Code:    ErrorCodeInternal,
				Message: "internal server error",
			})
			return
		}
		apiErr := APIError{
			Code:    ErrorCodeExecutionTimeout,
			Message: "execution timed out",
		}
		writeAPIError(w, apiErrorStatus(apiErr), apiErr)
		return
	}

	if err != nil {
		if r.Context().Err() != nil {
			if lifecycleErr := s.lifecycleManager.Cancel(executionID); lifecycleErr != nil {
				writeAPIError(w, http.StatusInternalServerError, APIError{
					Code:    ErrorCodeInternal,
					Message: "internal server error",
				})
				return
			}
		} else if lifecycleErr := s.lifecycleManager.Fail(executionID, err); lifecycleErr != nil {
			writeAPIError(w, http.StatusInternalServerError, APIError{
				Code:    ErrorCodeInternal,
				Message: "internal server error",
			})
			return
		}
		apiErr := classifyExecutionError(err)
		writeAPIError(w, apiErrorStatus(apiErr), apiErr)
		return
	}

	if r.Context().Err() != nil {
		if lifecycleErr := s.lifecycleManager.Cancel(executionID); lifecycleErr != nil {
			writeAPIError(w, http.StatusInternalServerError, APIError{
				Code:    ErrorCodeInternal,
				Message: "internal server error",
			})
			return
		}
		return
	}

	if err := s.lifecycleManager.Complete(executionID, result); err != nil {
		writeAPIError(w, http.StatusInternalServerError, APIError{
			Code:    ErrorCodeInternal,
			Message: "internal server error",
		})
		return
	}

	response := runResponse{
		ExecutionID:   result.ExecutionID,
		Scenario:      result.Scenario,
		TotalDuration: result.TotalDuration,
		Requests:      make([]requestSummary, len(result.Requests)),
	}

	for i, requestResult := range result.Requests {
		response.Requests[i] = requestSummary{
			RequestID:  requestResult.RequestID,
			Index:      requestResult.Index,
			StatusCode: requestResult.StatusCode,
			Status:     requestResult.Status,
			BodySize:   requestResult.BodySize,
			Duration:   requestResult.Duration,
			Error:      requestResult.Error,
		}
	}

	writeJSON(w, http.StatusOK, response)
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

func classifyExecutionError(err error) APIError {
	if errors.Is(err, context.DeadlineExceeded) {
		return APIError{
			Code:    ErrorCodeExecutionTimeout,
			Message: "execution timed out",
		}
	}

	return APIError{
		Code:    ErrorCodeInternal,
		Message: "internal server error",
	}
}
