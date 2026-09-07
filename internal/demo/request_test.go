package demo

import "testing"

func TestRunScenarioRequestDefaultsRequestCount(t *testing.T) {
	request := RunScenarioRequest{
		Service:      ServiceUsers,
		Operation:    OperationGetUser,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize1KB,
	}

	scenario := request.Scenario()

	if scenario.RequestCount != 1 {
		t.Fatalf(
			"request count = %d, want %d",
			scenario.RequestCount,
			1,
		)
	}
}

func TestRunScenarioRequestPreservesRequestCount(t *testing.T) {
	request := RunScenarioRequest{
		Service:      ServiceUsers,
		Operation:    OperationGetUser,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize1KB,
		RequestCount: 10,
	}

	scenario := request.Scenario()

	if scenario.RequestCount != 10 {
		t.Fatalf(
			"request count = %d, want %d",
			scenario.RequestCount,
			10,
		)
	}
}

func TestRunScenarioRequestScenarioValidation(t *testing.T) {
	request := RunScenarioRequest{
		Service:      ServiceUsers,
		Operation:    OperationGetUser,
		Simulation:   SimulationNormal,
		RequestSize:  RequestSizeNone,
		ResponseSize: ResponseSize1MB,
		RequestCount: 5,
	}

	if err := request.Scenario().Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}
