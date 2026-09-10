package demo

import (
	"fmt"
	"net/http"
	"testing"
)

func TestServiceValid(t *testing.T) {
	tests := []struct {
		name    string
		service Service
		want    bool
	}{
		{
			name:    "users",
			service: ServiceUsers,
			want:    true,
		},
		{
			name:    "orders",
			service: ServiceOrders,
			want:    true,
		},
		{
			name:    "invalid",
			service: "invalid",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.service.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOperationMapping(t *testing.T) {
	tests := []struct {
		operation Operation
		service   Service
		method    string
	}{
		{
			operation: OperationGetUsers,
			service:   ServiceUsers,
			method:    http.MethodGet,
		},
		{
			operation: OperationGetUser,
			service:   ServiceUsers,
			method:    http.MethodGet,
		},
		{
			operation: OperationCreateUser,
			service:   ServiceUsers,
			method:    http.MethodPost,
		},
		{
			operation: OperationUpdateUser,
			service:   ServiceUsers,
			method:    http.MethodPatch,
		},
		{
			operation: OperationDeleteUser,
			service:   ServiceUsers,
			method:    http.MethodDelete,
		},
		{
			operation: OperationGetOrders,
			service:   ServiceOrders,
			method:    http.MethodGet,
		},
		{
			operation: OperationGetOrder,
			service:   ServiceOrders,
			method:    http.MethodGet,
		},
		{
			operation: OperationCreateOrder,
			service:   ServiceOrders,
			method:    http.MethodPost,
		},
		{
			operation: OperationUpdateOrder,
			service:   ServiceOrders,
			method:    http.MethodPatch,
		},
		{
			operation: OperationDeleteOrder,
			service:   ServiceOrders,
			method:    http.MethodDelete,
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.operation), func(t *testing.T) {
			if got := tt.operation.Service(); got != tt.service {
				t.Fatalf(
					"Service() = %q, want %q",
					got,
					tt.service,
				)
			}

			if got := tt.operation.Method(); got != tt.method {
				t.Fatalf(
					"Method() = %q, want %q",
					got,
					tt.method,
				)
			}
		})
	}
}

func TestSimulationValid(t *testing.T) {
	tests := []Simulation{
		SimulationNormal,
		SimulationSlow,
		SimulationError,
	}

	for _, simulation := range tests {
		t.Run(string(simulation), func(t *testing.T) {
			if !simulation.Valid() {
				t.Fatalf("expected %q to be valid", simulation)
			}
		})
	}

	if Simulation("offline").Valid() {
		t.Fatal("offline must not be a supported request-scoped simulation")
	}
}

func TestRequestSizeValid(t *testing.T) {
	tests := []RequestSize{
		RequestSizeNone,
		RequestSize1KB,
		RequestSize16KB,
		RequestSize64KB,
		RequestSize256KB,
		RequestSize1MB,
		RequestSize4MB,
	}

	for _, size := range tests {
		t.Run(string(size), func(t *testing.T) {
			if !size.Valid() {
				t.Fatalf("expected %q to be valid", size)
			}
		})
	}

	if RequestSize("2mb").Valid() {
		t.Fatal("2mb must not be supported")
	}
}

func TestResponseSizeValid(t *testing.T) {
	tests := []ResponseSize{
		ResponseSizeNone,
		ResponseSize1KB,
		ResponseSize16KB,
		ResponseSize64KB,
		ResponseSize256KB,
		ResponseSize1MB,
		ResponseSize4MB,
	}

	for _, size := range tests {
		t.Run(string(size), func(t *testing.T) {
			if !size.Valid() {
				t.Fatalf("expected %q to be valid", size)
			}
		})
	}

	if ResponseSize("2mb").Valid() {
		t.Fatal("2mb must not be supported")
	}
}

func TestScenarioValidate(t *testing.T) {
	valid := Scenario{
		Service:      ServiceUsers,
		Operation:    OperationGetUser,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize64KB,
		RequestCount: 10,
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid scenario rejected: %v", err)
	}
}

func TestScenarioValidateRejectsInvalidService(t *testing.T) {
	scenario := Scenario{
		Service:      "invalid",
		Operation:    OperationGetUser,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize64KB,
		RequestCount: 1,
	}

	if err := scenario.Validate(); err == nil {
		t.Fatal("expected invalid service to be rejected")
	}
}

func TestScenarioValidateRejectsOperationFromAnotherService(t *testing.T) {
	scenario := Scenario{
		Service:      ServiceUsers,
		Operation:    OperationGetOrder,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize64KB,
		RequestCount: 1,
	}

	if err := scenario.Validate(); err == nil {
		t.Fatal("expected mismatched service/operation to be rejected")
	}
}

func TestScenarioValidateRejectsInvalidSimulation(t *testing.T) {
	scenario := Scenario{
		Service:      ServiceUsers,
		Operation:    OperationGetUser,
		Simulation:   "invalid",
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize64KB,
		RequestCount: 1,
	}

	if err := scenario.Validate(); err == nil {
		t.Fatal("expected invalid simulation to be rejected")
	}
}

func TestScenarioValidateRejectsInvalidResponseSize(t *testing.T) {
	scenario := Scenario{
		Service:      ServiceUsers,
		Operation:    OperationGetUser,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: "2mb",
		RequestCount: 1,
	}

	if err := scenario.Validate(); err == nil {
		t.Fatal("expected invalid response size to be rejected")
	}
}

func TestScenarioValidateRequestCount(t *testing.T) {
	tests := []int{
		0,
		51,
	}

	for _, count := range tests {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			scenario := Scenario{
				Service:      ServiceUsers,
				Operation:    OperationGetUser,
				Simulation:   SimulationNormal,
				RequestSize:  RequestSizeNone,
				ResponseSize: ResponseSize1KB,
				RequestCount: count,
			}

			if err := scenario.Validate(); err == nil {
				t.Fatalf(
					"expected request count %d to be rejected",
					count,
				)
			}
		})
	}
}

func TestResponseSizeBytes(t *testing.T) {
	tests := []struct {
		size  ResponseSize
		bytes int64
	}{
		{ResponseSizeNone, 0},
		{ResponseSize1KB, 1 << 10},
		{ResponseSize16KB, 16 << 10},
		{ResponseSize64KB, 64 << 10},
		{ResponseSize256KB, 256 << 10},
		{ResponseSize1MB, 1 << 20},
		{ResponseSize4MB, 4 << 20},
	}

	for _, tt := range tests {
		t.Run(string(tt.size), func(t *testing.T) {
			if got := tt.size.Bytes(); got != tt.bytes {
				t.Fatalf(
					"Bytes() = %d, want %d",
					got,
					tt.bytes,
				)
			}
		})
	}
}
