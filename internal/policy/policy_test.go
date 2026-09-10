package policy

import (
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
)

func validScenario() demo.Scenario {
	return demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		Simulation:   demo.SimulationNormal,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 1,
	}
}

func TestDefaultPolicy(t *testing.T) {
	policy := Default()

	if policy.MaxRequests != 50 {
		t.Fatalf("MaxRequests = %d, want 50", policy.MaxRequests)
	}

	if policy.MaxConcurrency != 10 {
		t.Fatalf("MaxConcurrency = %d, want 10", policy.MaxConcurrency)
	}

	if policy.MaxRequestBodyBytes != 4<<20 {
		t.Fatalf(
			"MaxRequestBodyBytes = %d, want %d",
			policy.MaxRequestBodyBytes,
			4<<20,
		)
	}

	if policy.MaxResponseBodyBytes != 4<<20 {
		t.Fatalf(
			"MaxResponseBodyBytes = %d, want %d",
			policy.MaxResponseBodyBytes,
			4<<20,
		)
	}

	if policy.MaxAggregateRequestBytes != 16<<20 {
		t.Fatalf(
			"MaxAggregateRequestBytes = %d, want %d",
			policy.MaxAggregateRequestBytes,
			16<<20,
		)
	}

	if policy.MaxAggregateResponseBytes != 16<<20 {
		t.Fatalf(
			"MaxAggregateResponseBytes = %d, want %d",
			policy.MaxAggregateResponseBytes,
			16<<20,
		)
	}

	if policy.MaxExecutionDuration != 10*time.Second {
		t.Fatalf(
			"MaxExecutionDuration = %s, want 10s",
			policy.MaxExecutionDuration,
		)
	}
}

func TestPolicyAllowsNormalScenario(t *testing.T) {
	policy := Default()

	if err := policy.Validate(validScenario()); err != nil {
		t.Fatalf("valid scenario rejected: %v", err)
	}
}

func TestPolicyAllowsRequestCountAtLimit(t *testing.T) {
	policy := Default()
	scenario := validScenario()
	scenario.RequestCount = policy.MaxRequests

	if err := policy.Validate(scenario); err != nil {
		t.Fatalf("request count at limit rejected: %v", err)
	}
}

func TestPolicyRejectsRequestCountAboveLimit(t *testing.T) {
	policy := Default()
	scenario := validScenario()
	scenario.RequestCount = policy.MaxRequests + 1

	if err := policy.Validate(scenario); err == nil {
		t.Fatal("expected request count limit rejection")
	}
}

func TestPolicyAllowsPerRequestRequestSizeAtLimit(t *testing.T) {
	policy := Default()
	scenario := validScenario()
	scenario.Operation = demo.OperationCreateUser
	scenario.RequestSize = demo.RequestSize4MB
	scenario.ResponseSize = demo.ResponseSizeNone

	if err := policy.Validate(scenario); err != nil {
		t.Fatalf("4MB request rejected: %v", err)
	}
}

func TestPolicyAllowsPerRequestResponseSizeAtLimit(t *testing.T) {
	policy := Default()
	scenario := validScenario()
	scenario.ResponseSize = demo.ResponseSize4MB

	if err := policy.Validate(scenario); err != nil {
		t.Fatalf("4MB response rejected: %v", err)
	}
}

func TestPolicyRejectsAggregateRequestSize(t *testing.T) {
	policy := Default()

	scenario := demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationCreateUser,
		Simulation:   demo.SimulationNormal,
		RequestSize:  demo.RequestSize4MB,
		ResponseSize: demo.ResponseSizeNone,
		RequestCount: 5,
	}

	if err := policy.Validate(scenario); err == nil {
		t.Fatal("expected aggregate request size rejection")
	}
}

func TestPolicyRejectsAggregateResponseSize(t *testing.T) {
	policy := Default()

	scenario := validScenario()
	scenario.ResponseSize = demo.ResponseSize4MB
	scenario.RequestCount = 5

	if err := policy.Validate(scenario); err == nil {
		t.Fatal("expected aggregate response size rejection")
	}
}

func TestPolicyAllowsAggregateResponseAtLimit(t *testing.T) {
	policy := Default()

	scenario := validScenario()
	scenario.ResponseSize = demo.ResponseSize4MB
	scenario.RequestCount = 4

	if err := policy.Validate(scenario); err != nil {
		t.Fatalf(
			"aggregate response at limit rejected: %v",
			err,
		)
	}
}

func TestPolicyRejectsInvalidConcurrencyConfiguration(t *testing.T) {
	policy := Default()
	policy.MaxConcurrency = 0

	if err := policy.Validate(validScenario()); err == nil {
		t.Fatal("expected invalid concurrency configuration")
	}
}

func TestPolicyRejectsInvalidExecutionDurationConfiguration(t *testing.T) {
	policy := Default()
	policy.MaxExecutionDuration = 0

	if err := policy.Validate(validScenario()); err == nil {
		t.Fatal("expected invalid execution duration configuration")
	}
}

func TestPolicyPreservesScenarioValidation(t *testing.T) {
	policy := Default()

	scenario := validScenario()
	scenario.Service = "invalid"

	if err := policy.Validate(scenario); err == nil {
		t.Fatal("expected invalid scenario to be rejected")
	}
}
