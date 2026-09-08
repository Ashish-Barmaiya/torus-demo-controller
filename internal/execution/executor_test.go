package execution

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/request"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/torus"
)

func testScenario(count int) demo.Scenario {
	return demo.Scenario{
		Service:      demo.ServiceUsers,
		Operation:    demo.OperationGetUser,
		Simulation:   demo.SimulationNormal,
		RequestSize:  demo.RequestSizeNone,
		ResponseSize: demo.ResponseSize1KB,
		RequestCount: count,
	}
}

func TestExecutorExecute(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}),
	)
	defer server.Close()

	idSource := testIDSource{
		userID:  "usr_000005",
		orderID: "ord_000007",
	}

	generator := request.NewGenerator(
		server.URL,
		idSource,
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 2)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	result, err := executor.Execute(
		context.Background(),
		testScenario(5),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if len(result.Requests) != 5 {
		t.Fatalf(
			"requests = %d, want %d",
			len(result.Requests),
			5,
		)
	}

	for i, requestResult := range result.Requests {
		if requestResult.Index != i {
			t.Fatalf(
				"request index = %d, want %d",
				requestResult.Index,
				i,
			)
		}

		if requestResult.StatusCode != http.StatusOK {
			t.Fatalf(
				"request %d status = %d, want %d",
				i,
				requestResult.StatusCode,
				http.StatusOK,
			)
		}

		if requestResult.Error != "" {
			t.Fatalf(
				"request %d unexpected error: %s",
				i,
				requestResult.Error,
			)
		}
	}
}

func TestExecutorRespectsConcurrencyLimit(t *testing.T) {
	var active int32
	var maxActive int32

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			current := atomic.AddInt32(&active, 1)

			for {
				old := atomic.LoadInt32(&maxActive)

				if current <= old {
					break
				}

				if atomic.CompareAndSwapInt32(
					&maxActive,
					old,
					current,
				) {
					break
				}
			}

			time.Sleep(100 * time.Millisecond)

			atomic.AddInt32(&active, -1)

			w.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 2)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	_, err = executor.Execute(
		context.Background(),
		testScenario(10),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if maxActive > 2 {
		t.Fatalf(
			"max concurrent requests = %d, want <= 2",
			maxActive,
		)
	}
}

func TestExecutorContextCancellation(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Second)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 2)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := executor.Execute(
		ctx,
		testScenario(5),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	for _, requestResult := range result.Requests {
		if requestResult.Error == "" {
			t.Fatal("expected cancelled request error")
		}
	}
}

func TestExecutorRejectsInvalidScenario(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("server should not receive request")
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(time.Second)

	executor, err := NewExecutor(generator, client, 2)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	scenario := testScenario(1)
	scenario.Service = "invalid"

	_, err = executor.Execute(
		context.Background(),
		scenario,
	)

	if err == nil {
		t.Fatal("expected invalid scenario error")
	}
}

func TestExecutorPropagatesResponse(t *testing.T) {
	const responseBody = `{"message":"hello"}`

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Test", "true")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(responseBody))
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 1)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	result, err := executor.Execute(
		context.Background(),
		testScenario(1),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	requestResult := result.Requests[0]

	if requestResult.StatusCode != http.StatusCreated {
		t.Fatalf(
			"status = %d, want %d",
			requestResult.StatusCode,
			http.StatusCreated,
		)
	}

	if requestResult.BodySize != int64(len(responseBody)) {
		t.Fatalf(
			"body size = %d, want %d",
			requestResult.BodySize,
			len(responseBody),
		)
	}

	if string(requestResult.Body) != responseBody {
		t.Fatalf(
			"body = %q, want %q",
			string(requestResult.Body),
			responseBody,
		)
	}
}

func TestExecutorErrorResponse(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(
				w,
				"backend unavailable",
				http.StatusServiceUnavailable,
			)
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 1)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	result, err := executor.Execute(
		context.Background(),
		testScenario(1),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	requestResult := result.Requests[0]

	if requestResult.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf(
			"status = %d, want %d",
			requestResult.StatusCode,
			http.StatusServiceUnavailable,
		)
	}
}

func TestExecutorTotalDuration(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(50 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 1)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	result, err := executor.Execute(
		context.Background(),
		testScenario(1),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if result.TotalDuration < 50*time.Millisecond {
		t.Fatalf(
			"total duration = %s, expected >= 50ms",
			result.TotalDuration,
		)
	}
}

func TestExecutorDoesNotRequireResponseBody(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 1)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	result, err := executor.Execute(
		context.Background(),
		testScenario(1),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if result.Requests[0].StatusCode != http.StatusNoContent {
		t.Fatalf(
			"status = %d, want %d",
			result.Requests[0].StatusCode,
			http.StatusNoContent,
		)
	}
}

func TestExecutorRequestCountOne(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	generator := request.NewGenerator(
		server.URL,
		testIDSource{
			userID:  "usr_000005",
			orderID: "ord_000007",
		},
	)

	client := torus.NewClient(5 * time.Second)

	executor, err := NewExecutor(generator, client, 1)
	if err != nil {
		t.Fatalf("NewExecutor() error: %v", err)
	}

	result, err := executor.Execute(
		context.Background(),
		testScenario(1),
	)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if len(result.Requests) != 1 {
		t.Fatalf(
			"requests = %d, want 1",
			len(result.Requests),
		)
	}
}
