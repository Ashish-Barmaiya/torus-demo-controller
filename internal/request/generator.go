package request

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
)

const (
	SimulationHeader      = "X-Demo-Simulation"
	SimulationDelayHeader = "X-Demo-Simulation-Delay"
	ResponseSizeHeader    = "X-Demo-Response-Size"
)

const defaultSimulationDelayMS = 750

type Generator struct {
	baseURL  string
	idSource IDSource
}

func NewGenerator(baseURL string, idSource IDSource) *Generator {
	return &Generator{
		baseURL:  strings.TrimRight(baseURL, "/"),
		idSource: idSource,
	}
}

func (g *Generator) Build(scenario demo.Scenario) (*http.Request, error) {
	if err := scenario.Validate(); err != nil {
		return nil, fmt.Errorf("validate scenario: %w", err)
	}

	if g.baseURL == "" {
		return nil, fmt.Errorf("base URL must not be empty")
	}

	if g.idSource == nil {
		return nil, fmt.Errorf("ID source must not be nil")
	}

	method := scenario.Operation.Method()
	path, body, err := g.buildOperation(
		scenario.Operation,
		scenario.RequestSize,
	)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(
		method,
		g.baseURL+path,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set(SimulationHeader, string(scenario.Simulation))

	if scenario.Simulation == demo.SimulationSlow {
		req.Header.Set(
			SimulationDelayHeader,
			fmt.Sprintf("%d", defaultSimulationDelayMS),
		)
	}

	req.Header.Set(ResponseSizeHeader, string(scenario.ResponseSize))

	return req, nil
}

func (g *Generator) buildOperation(
	operation demo.Operation,
	requestSize demo.RequestSize,
) (string, []byte, error) {
	switch operation {
	case demo.OperationGetUsers:
		if requestSize != demo.RequestSizeNone {
			return "", nil, fmt.Errorf(
				"request size is not supported for %q",
				operation,
			)
		}

		return "/api/v1/users", nil, nil

	case demo.OperationGetUser:
		if requestSize != demo.RequestSizeNone {
			return "", nil, fmt.Errorf(
				"request size is not supported for %q",
				operation,
			)
		}

		return "/api/v1/users/" + g.idSource.User().PathID, nil, nil

	case demo.OperationCreateUser:
		body, err := generateJSONBody(
			requestSize.Bytes(),
			map[string]any{
				"name":  "Demo User",
				"email": "demo.user@example.com",
				"plan":  "pro",
			},
		)
		if err != nil {
			return "", nil, err
		}

		return "/api/v1/users", body, nil

	case demo.OperationUpdateUser:
		body, err := generateJSONBody(
			requestSize.Bytes(),
			map[string]any{
				"plan":   "enterprise",
				"status": "active",
			},
		)
		if err != nil {
			return "", nil, err
		}

		return "/api/v1/users/" + g.idSource.User().PathID, body, nil

	case demo.OperationDeleteUser:
		if requestSize != demo.RequestSizeNone {
			return "", nil, fmt.Errorf(
				"request size is not supported for %q",
				operation,
			)
		}

		return "/api/v1/users/" + g.idSource.User().PathID, nil, nil

	case demo.OperationGetOrders:
		if requestSize != demo.RequestSizeNone {
			return "", nil, fmt.Errorf(
				"request size is not supported for %q",
				operation,
			)
		}

		return "/api/v1/orders", nil, nil

	case demo.OperationGetOrder:
		if requestSize != demo.RequestSizeNone {
			return "", nil, fmt.Errorf(
				"request size is not supported for %q",
				operation,
			)
		}

		return "/api/v1/orders/" + g.idSource.Order().PathID, nil, nil

	case demo.OperationCreateOrder:
		body, err := generateJSONBody(
			requestSize.Bytes(),
			map[string]any{
				"customer_id": g.idSource.User().ID,
				"currency":    "USD",
				"total":       129900,
			},
		)
		if err != nil {
			return "", nil, err
		}

		return "/api/v1/orders", body, nil

	case demo.OperationUpdateOrder:
		body, err := generateJSONBody(
			requestSize.Bytes(),
			map[string]any{
				"status": "processing",
				"total":  149900,
			},
		)
		if err != nil {
			return "", nil, err
		}

		return "/api/v1/orders/" + g.idSource.Order().PathID, body, nil

	case demo.OperationDeleteOrder:
		if requestSize != demo.RequestSizeNone {
			return "", nil, fmt.Errorf(
				"request size is not supported for %q",
				operation,
			)
		}

		return "/api/v1/orders/" + g.idSource.Order().PathID, nil, nil

	default:
		return "", nil, fmt.Errorf(
			"unsupported operation %q",
			operation,
		)
	}
}
