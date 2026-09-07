package request

import (
	"bytes"
	"encoding/json"
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
	path, body, err := g.buildOperation(scenario.Operation)
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
) (string, []byte, error) {
	switch operation {
	case demo.OperationGetUsers:
		return "/api/v1/users", nil, nil

	case demo.OperationGetUser:
		return "/api/v1/users/" + g.idSource.UserID(), nil, nil

	case demo.OperationCreateUser:
		return "/api/v1/users", g.createUserBody(), nil

	case demo.OperationUpdateUser:
		return "/api/v1/users/" + g.idSource.UserID(),
			g.updateUserBody(),
			nil

	case demo.OperationDeleteUser:
		return "/api/v1/users/" + g.idSource.UserID(), nil, nil

	case demo.OperationGetOrders:
		return "/api/v1/orders", nil, nil

	case demo.OperationGetOrder:
		return "/api/v1/orders/" + g.idSource.OrderID(), nil, nil

	case demo.OperationCreateOrder:
		return "/api/v1/orders", g.createOrderBody(), nil

	case demo.OperationUpdateOrder:
		return "/api/v1/orders/" + g.idSource.OrderID(),
			g.updateOrderBody(),
			nil

	case demo.OperationDeleteOrder:
		return "/api/v1/orders/" + g.idSource.OrderID(), nil, nil

	default:
		return "", nil, fmt.Errorf(
			"unsupported operation %q",
			operation,
		)
	}
}

func (g *Generator) createUserBody() []byte {
	body := map[string]any{
		"name":  "Demo User",
		"email": "demo.user@example.com",
		"plan":  "pro",
	}

	return mustJSON(body)
}

func (g *Generator) updateUserBody() []byte {
	body := map[string]any{
		"plan":   "enterprise",
		"status": "active",
	}

	return mustJSON(body)
}

func (g *Generator) createOrderBody() []byte {
	body := map[string]any{
		"customer_id": g.idSource.UserID(),
		"currency":    "USD",
		"total":       129900,
	}

	return mustJSON(body)
}

func (g *Generator) updateOrderBody() []byte {
	body := map[string]any{
		"status": "processing",
		"total":  149900,
	}

	return mustJSON(body)
}

func mustJSON(value any) []byte {
	body, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal static demo request: %v", err))
	}

	return body
}
