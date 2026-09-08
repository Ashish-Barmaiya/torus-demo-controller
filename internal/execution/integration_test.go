package execution

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/request"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/torus"
)

func TestExecuteThroughTorus(t *testing.T) {
	torusURL := "http://localhost:8080"

	generator := request.NewGenerator(
		torusURL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(10 * time.Second)

	executor, err := NewExecutor(
		generator,
		client,
		2,
	)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	scenario := demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		Simulation:   demo.SimulationNormal,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 1,
	}

	result, err := executor.Execute(
		context.Background(),
		scenario,
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if len(result.Requests) != 1 {
		t.Fatalf(
			"request count = %d, want 1",
			len(result.Requests),
		)
	}

	requestResult := result.Requests[0]

	if requestResult.StatusCode != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			requestResult.StatusCode,
			http.StatusOK,
		)
	}

	if requestResult.BodySize != int64(1<<10) {
		t.Fatalf(
			"body size = %d, want %d",
			requestResult.BodySize,
			1<<10,
		)
	}

	body := string(requestResult.Body)

	if !strings.Contains(body, `"id":"usr_000005"`) {
		t.Fatalf(
			"expected user ID in response, got: %s",
			body[:min(len(body), 500)],
		)
	}

	if requestResult.Error != "" {
		t.Fatalf(
			"unexpected execution error: %s",
			requestResult.Error,
		)
	}
}

func TestExecuteOrdersThroughTorus(t *testing.T) {
	torusURL := "http://localhost:8080"

	generator := request.NewGenerator(
		torusURL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(10 * time.Second)

	executor, err := NewExecutor(
		generator,
		client,
		2,
	)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	scenario := demo.Scenario{
		Service:      demo.ServiceOrders,
		Operation:    demo.OperationGetOrder,
		Simulation:   demo.SimulationNormal,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 1,
	}

	result, err := executor.Execute(
		context.Background(),
		scenario,
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if len(result.Requests) != 1 {
		t.Fatalf(
			"request count = %d, want 1",
			len(result.Requests),
		)
	}

	requestResult := result.Requests[0]

	if requestResult.StatusCode != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			requestResult.StatusCode,
			http.StatusOK,
		)
	}

	if requestResult.BodySize != int64(1<<10) {
		t.Fatalf(
			"body size = %d, want %d",
			requestResult.BodySize,
			1<<10,
		)
	}

	if !strings.Contains(
		string(requestResult.Body),
		`"id":"ord_000007"`,
	) {
		t.Fatalf(
			"expected order ID in response, got: %s",
			string(requestResult.Body)[:min(len(requestResult.Body), 500)],
		)
	}
}

func TestExecuteSlowSimulationThroughTorus(t *testing.T) {
	torusURL := "http://localhost:8080"

	generator := request.NewGenerator(
		torusURL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(10 * time.Second)

	executor, err := NewExecutor(
		generator,
		client,
		1,
	)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	scenario := demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		Simulation:   demo.SimulationSlow,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 1,
	}

	start := time.Now()

	result, err := executor.Execute(
		context.Background(),
		scenario,
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	elapsed := time.Since(start)

	if result.Requests[0].StatusCode != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			result.Requests[0].StatusCode,
			http.StatusOK,
		)
	}

	if elapsed < 750*time.Millisecond {
		t.Fatalf(
			"request completed too quickly: %s",
			elapsed,
		)
	}
}

func TestConcurrentSlowRequestsThroughTorus(t *testing.T) {
	torusURL := "http://localhost:8080"

	generator := request.NewGenerator(
		torusURL,
		request.NewRandomIDSource(42),
	)

	client := torus.NewClient(10 * time.Second)

	executor, err := NewExecutor(
		generator,
		client,
		3,
	)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	scenario := demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		Simulation:   demo.SimulationSlow,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: 6,
	}

	start := time.Now()

	result, err := executor.Execute(
		context.Background(),
		scenario,
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	elapsed := time.Since(start)

	if len(result.Requests) != 6 {
		t.Fatalf(
			"requests = %d, want 6",
			len(result.Requests),
		)
	}

	for i, requestResult := range result.Requests {
		if requestResult.StatusCode != http.StatusOK {
			t.Fatalf(
				"request %d status = %d, want %d",
				i,
				requestResult.StatusCode,
				http.StatusOK,
			)
		}

		if requestResult.BodySize != int64(1<<10) {
			t.Fatalf(
				"request %d body size = %d, want %d",
				i,
				requestResult.BodySize,
				1<<10,
			)
		}
	}

	if elapsed < 1500*time.Millisecond {
		t.Fatalf(
			"execution completed too quickly: %s",
			elapsed,
		)
	}

	if elapsed > 4*time.Second {
		t.Fatalf(
			"execution took unexpectedly long: %s",
			elapsed,
		)
	}
}
