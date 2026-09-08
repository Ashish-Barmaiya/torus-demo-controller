package request

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
)

type fixedIDSource struct {
	userID  string
	orderID string
}

func (s fixedIDSource) UserID() string {
	return s.userID
}

func (s fixedIDSource) OrderID() string {
	return s.orderID
}

func newTestGenerator() *Generator {
	return NewGenerator(
		"http://torus:8080",
		fixedIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)
}

func validScenario(
	service demo.Service,
	operation demo.Operation,
) demo.Scenario {
	requestSize := demo.RequestSizeNone

	switch operation {
	case demo.OperationCreateUser,
		demo.OperationUpdateUser,
		demo.OperationCreateOrder,
		demo.OperationUpdateOrder:
		requestSize = demo.RequestSize1KB
	}

	return demo.Scenario{
		Service:      service,
		Operation:    operation,
		Simulation:   demo.SimulationNormal,
		RequestSize:  requestSize,
		ResponseSize: demo.ResponseSize64KB,
		RequestCount: 1,
	}
}

func TestBuildGetUsers(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceUsers,
			demo.OperationGetUsers,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodGet {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodGet)
	}

	if req.URL.Path != "/api/v1/users" {
		t.Fatalf("path = %q, want %q", req.URL.Path, "/api/v1/users")
	}

	if got := req.Header.Get(ResponseSizeHeader); got != "64kb" {
		t.Fatalf("response size header = %q, want %q", got, "64kb")
	}

	if got := req.Header.Get(SimulationHeader); got != "normal" {
		t.Fatalf("simulation header = %q, want %q", got, "normal")
	}
}

func TestBuildGetUser(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceUsers,
			demo.OperationGetUser,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodGet {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodGet)
	}

	if req.URL.Path != "/api/v1/users/usr_000005" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildCreateUser(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceUsers,
			demo.OperationCreateUser,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodPost {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodPost)
	}

	if req.URL.Path != "/api/v1/users" {
		t.Fatalf("path = %q", req.URL.Path)
	}

	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	bodyString := string(body)

	for _, expected := range []string{
		`"name":"Demo User"`,
		`"email":"demo.user@example.com"`,
		`"plan":"pro"`,
	} {
		if !strings.Contains(bodyString, expected) {
			t.Fatalf(
				"body missing %q: %s",
				expected,
				bodyString,
			)
		}
	}
}

func TestBuildUpdateUser(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceUsers,
			demo.OperationUpdateUser,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodPatch {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodPatch)
	}

	if req.URL.Path != "/api/v1/users/usr_000005" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildDeleteUser(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceUsers,
			demo.OperationDeleteUser,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodDelete {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodDelete)
	}

	if req.URL.Path != "/api/v1/users/usr_000005" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildGetOrders(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceOrders,
			demo.OperationGetOrders,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodGet {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodGet)
	}

	if req.URL.Path != "/api/v1/orders" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildGetOrder(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceOrders,
			demo.OperationGetOrder,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.URL.Path != "/api/v1/orders/ord_000007" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildCreateOrder(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceOrders,
			demo.OperationCreateOrder,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodPost {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodPost)
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	bodyString := string(body)

	for _, expected := range []string{
		`"customer_id":"usr_000005"`,
		`"currency":"USD"`,
		`"total":129900`,
	} {
		if !strings.Contains(bodyString, expected) {
			t.Fatalf(
				"body missing %q: %s",
				expected,
				bodyString,
			)
		}
	}
}

func TestBuildUpdateOrder(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceOrders,
			demo.OperationUpdateOrder,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodPatch {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodPatch)
	}

	if req.URL.Path != "/api/v1/orders/ord_000007" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildDeleteOrder(t *testing.T) {
	req, err := newTestGenerator().Build(
		validScenario(
			demo.ServiceOrders,
			demo.OperationDeleteOrder,
		),
	)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if req.Method != http.MethodDelete {
		t.Fatalf("method = %q, want %q", req.Method, http.MethodDelete)
	}

	if req.URL.Path != "/api/v1/orders/ord_000007" {
		t.Fatalf("path = %q", req.URL.Path)
	}
}

func TestBuildSlowSimulation(t *testing.T) {
	scenario := validScenario(
		demo.ServiceUsers,
		demo.OperationGetUser,
	)
	scenario.Simulation = demo.SimulationSlow

	req, err := newTestGenerator().Build(scenario)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if got := req.Header.Get(SimulationHeader); got != "slow" {
		t.Fatalf("simulation = %q, want %q", got, "slow")
	}

	if got := req.Header.Get(SimulationDelayHeader); got != "750" {
		t.Fatalf(
			"simulation delay = %q, want %q",
			got,
			"750",
		)
	}
}

func TestBuildErrorSimulation(t *testing.T) {
	scenario := validScenario(
		demo.ServiceUsers,
		demo.OperationGetUser,
	)
	scenario.Simulation = demo.SimulationError

	req, err := newTestGenerator().Build(scenario)
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}

	if got := req.Header.Get(SimulationHeader); got != "error" {
		t.Fatalf("simulation = %q, want %q", got, "error")
	}

	if got := req.Header.Get(SimulationDelayHeader); got != "" {
		t.Fatalf(
			"unexpected simulation delay header: %q",
			got,
		)
	}
}

func TestBuildRejectsInvalidScenario(t *testing.T) {
	scenario := demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetOrder,
		Simulation:   demo.SimulationNormal,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 1,
	}

	_, err := newTestGenerator().Build(scenario)

	if err == nil {
		t.Fatal("expected invalid scenario to be rejected")
	}
}

func TestBuildRejectsEmptyBaseURL(t *testing.T) {
	generator := NewGenerator(
		"",
		fixedIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	_, err := generator.Build(
		validScenario(
			demo.ServiceUsers,
			demo.OperationGetUser,
		),
	)

	if err == nil {
		t.Fatal("expected empty base URL to be rejected")
	}
}

func TestBuildRequestSize(t *testing.T) {
	tests := []struct {
		name      string
		operation demo.Operation
		size      demo.RequestSize
		wantBytes int
	}{
		{
			name:      "create user 1kb",
			operation: demo.OperationCreateUser,
			size:      demo.RequestSize1KB,
			wantBytes: 1 << 10,
		},
		{
			name:      "update user 64kb",
			operation: demo.OperationUpdateUser,
			size:      demo.RequestSize64KB,
			wantBytes: 64 << 10,
		},
		{
			name:      "create order 1mb",
			operation: demo.OperationCreateOrder,
			size:      demo.RequestSize1MB,
			wantBytes: 1 << 20,
		},
		{
			name:      "update order 4mb",
			operation: demo.OperationUpdateOrder,
			size:      demo.RequestSize4MB,
			wantBytes: 4 << 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scenario := demo.Scenario{
				Service:      tt.operation.Service(),
				Operation:    tt.operation,
				Simulation:   demo.SimulationNormal,
				RequestSize:  tt.size,
				ResponseSize: demo.ResponseSizeNone,
				RequestCount: 1,
			}

			req, err := newTestGenerator().Build(scenario)
			if err != nil {
				t.Fatalf("Build() error: %v", err)
			}

			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if len(body) != tt.wantBytes {
				t.Fatalf(
					"body size = %d, want %d",
					len(body),
					tt.wantBytes,
				)
			}
		})
	}
}

func TestBuildRejectsRequestSizeForBodylessOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation demo.Operation
	}{
		{
			name:      "get user",
			operation: demo.OperationGetUser,
		},
		{
			name:      "get users",
			operation: demo.OperationGetUsers,
		},
		{
			name:      "delete user",
			operation: demo.OperationDeleteUser,
		},
		{
			name:      "get order",
			operation: demo.OperationGetOrder,
		},
		{
			name:      "get orders",
			operation: demo.OperationGetOrders,
		},
		{
			name:      "delete order",
			operation: demo.OperationDeleteOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scenario := demo.Scenario{
				Service:      tt.operation.Service(),
				Operation:    tt.operation,
				Simulation:   demo.SimulationNormal,
				RequestSize:  demo.RequestSize1KB,
				ResponseSize: demo.ResponseSizeNone,
				RequestCount: 1,
			}

			_, err := newTestGenerator().Build(scenario)
			if err == nil {
				t.Fatal("expected request size to be rejected")
			}
		})
	}
}
