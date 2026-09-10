package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/policy"
)

type Executor interface {
	Execute(context.Context, demo.Scenario) (execution.Result, error)
}

type ErrorCode string

const (
	ErrorCodeInvalidJSON      ErrorCode = "INVALID_JSON"
	ErrorCodeInvalidScenario  ErrorCode = "INVALID_SCENARIO"
	ErrorCodePolicyRejected   ErrorCode = "POLICY_REJECTED"
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
	executor Executor
	policy   policy.Policy
}

func New(executor Executor, executionPolicy policy.Policy) (*Server, error) {
	if executor == nil {
		return nil, fmt.Errorf("executor must not be nil")
	}

	return &Server{
		executor: executor,
		policy:   executionPolicy,
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
	Scenario      demo.Scenario    `json:"scenario"`
	TotalDuration time.Duration    `json:"total_duration"`
	Requests      []requestSummary `json:"requests"`
}

type requestSummary struct {
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

	ctx, cancel := context.WithTimeout(
		r.Context(),
		s.policy.MaxExecutionDuration,
	)
	defer cancel()

	result, err := s.executor.Execute(
		ctx,
		scenario,
	)
	if err != nil {
		apiErr := classifyExecutionError(err, ctx, r.Context(), result)
		writeAPIError(w, apiErrorStatus(apiErr), apiErr)
		return
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) &&
		!errors.Is(r.Context().Err(), context.DeadlineExceeded) {
		apiErr := APIError{
			Code:    ErrorCodeExecutionTimeout,
			Message: "execution timed out",
		}
		writeAPIError(w, apiErrorStatus(apiErr), apiErr)
		return
	}

	if apiErr := classifyRequestResults(result); apiErr != nil {
		writeAPIError(w, apiErrorStatus(*apiErr), *apiErr)
		return
	}

	response := runResponse{
		Scenario:      result.Scenario,
		TotalDuration: result.TotalDuration,
		Requests:      make([]requestSummary, len(result.Requests)),
	}

	for i, requestResult := range result.Requests {
		response.Requests[i] = requestSummary{
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

func classifyExecutionError(
	err error,
	ctx context.Context,
	requestContext context.Context,
	result execution.Result,
) APIError {
	if !errors.Is(requestContext.Err(), context.DeadlineExceeded) &&
		(errors.Is(ctx.Err(), context.DeadlineExceeded) ||
			errors.Is(err, context.DeadlineExceeded)) {
		return APIError{
			Code:    ErrorCodeExecutionTimeout,
			Message: "execution timed out",
		}
	}

	if hasUpstreamFailure(result) {
		return APIError{
			Code:    ErrorCodeUpstreamError,
			Message: "upstream request failed",
		}
	}

	return APIError{
		Code:    ErrorCodeInternal,
		Message: "internal server error",
	}
}

func classifyRequestResults(result execution.Result) *APIError {
	if !hasUpstreamFailure(result) {
		return nil
	}

	apiErr := APIError{
		Code:    ErrorCodeUpstreamError,
		Message: "upstream request failed",
	}
	return &apiErr
}

func hasUpstreamFailure(result execution.Result) bool {
	for _, requestResult := range result.Requests {
		if requestResult.StatusCode == 0 && requestResult.Error != "" {
			return true
		}
	}

	return false
}
