package lifecycle

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/execution"
)

func TestNewRejectsInvalidRetention(t *testing.T) {
	for _, value := range []int{0, -1} {
		t.Run(
			"retention_"+string(rune('0'+value*-1)),
			func(t *testing.T) {
				if _, err := New(value); err == nil {
					t.Fatalf(
						"New(%d) should return an error",
						value,
					)
				}
			},
		)
	}
}

func TestCreateAndGet(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	exec, err := manager.Create(
		"exec_1",
		testScenario(),
	)
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if exec == nil {
		t.Fatal("Create() returned nil execution")
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok {
		t.Fatal("Get() did not find execution")
	}

	if snapshot.ID != "exec_1" {
		t.Fatalf(
			"ID = %q, want %q",
			snapshot.ID,
			"exec_1",
		)
	}

	if snapshot.Status != StatusCreated {
		t.Fatalf(
			"Status = %q, want %q",
			snapshot.Status,
			StatusCreated,
		)
	}
}

func TestCreateRejectsEmptyID(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create("", testScenario()); err == nil {
		t.Fatal("expected empty ID to be rejected")
	}
}

func TestCreateRejectsDuplicateID(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("first Create() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err == nil {
		t.Fatal("expected duplicate ID to be rejected")
	}
}

func TestGetUnknownExecution(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, ok := manager.Get("missing"); ok {
		t.Fatal("Get() should not find missing execution")
	}
}

func TestStart(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if err := manager.Start("exec_1"); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok {
		t.Fatal("execution disappeared")
	}

	if snapshot.Status != StatusRunning {
		t.Fatalf(
			"Status = %q, want %q",
			snapshot.Status,
			StatusRunning,
		)
	}

	if snapshot.StartedAt.IsZero() {
		t.Fatal("StartedAt must be populated")
	}
}

func TestComplete(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if err := manager.Start("exec_1"); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	result := execution.Result{
		ExecutionID: "exec_1",
		Requests: []execution.RequestResult{
			{
				RequestID:  "req_1",
				Index:      0,
				StatusCode: 200,
			},
		},
	}

	if err := manager.Complete(
		"exec_1",
		result,
	); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok {
		t.Fatal("execution disappeared")
	}

	if snapshot.Status != StatusCompleted {
		t.Fatalf(
			"Status = %q, want %q",
			snapshot.Status,
			StatusCompleted,
		)
	}

	if snapshot.CompletedAt.IsZero() {
		t.Fatal("CompletedAt must be populated")
	}

	if snapshot.Result == nil {
		t.Fatal("Result must be retained")
	}

	if snapshot.Result.ExecutionID != "exec_1" {
		t.Fatalf(
			"ExecutionID = %q, want %q",
			snapshot.Result.ExecutionID,
			"exec_1",
		)
	}

	if snapshot.Error != "" {
		t.Fatalf("Error = %q, want empty", snapshot.Error)
	}
}

func TestFail(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if err := manager.Start("exec_1"); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if err := manager.Fail(
		"exec_1",
		errors.New("upstream failure"),
	); err != nil {
		t.Fatalf("Fail() error: %v", err)
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok {
		t.Fatal("execution disappeared")
	}

	if snapshot.Status != StatusFailed {
		t.Fatalf(
			"Status = %q, want %q",
			snapshot.Status,
			StatusFailed,
		)
	}

	if snapshot.Error != "upstream failure" {
		t.Fatalf(
			"Error = %q, want %q",
			snapshot.Error,
			"upstream failure",
		)
	}

	if snapshot.CompletedAt.IsZero() {
		t.Fatal("CompletedAt must be populated")
	}
}

func TestCancel(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if err := manager.Start("exec_1"); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if err := manager.Cancel("exec_1"); err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	snapshot, ok := manager.Get("exec_1")
	if !ok {
		t.Fatal("execution disappeared")
	}

	if snapshot.Status != StatusCancelled {
		t.Fatalf(
			"Status = %q, want %q",
			snapshot.Status,
			StatusCancelled,
		)
	}

	if snapshot.Error != "execution cancelled" {
		t.Fatalf(
			"Error = %q, want %q",
			snapshot.Error,
			"execution cancelled",
		)
	}

	if snapshot.CompletedAt.IsZero() {
		t.Fatal("CompletedAt must be populated")
	}
}

func TestInvalidStateTransitions(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if err := manager.Complete(
		"exec_1",
		execution.Result{},
	); err == nil {
		t.Fatal("Complete() should reject created state")
	}

	if err := manager.Fail(
		"exec_1",
		errors.New("failure"),
	); err == nil {
		t.Fatal("Fail() should reject created state")
	}

	if err := manager.Cancel("exec_1"); err == nil {
		t.Fatal("Cancel() should reject created state")
	}

	if err := manager.Start("exec_1"); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if err := manager.Start("exec_1"); err == nil {
		t.Fatal("Start() should reject running state")
	}

	if err := manager.Complete(
		"exec_1",
		execution.Result{},
	); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	if err := manager.Complete(
		"exec_1",
		execution.Result{},
	); err == nil {
		t.Fatal("Complete() should reject completed state")
	}

	if err := manager.Start("exec_1"); err == nil {
		t.Fatal("Start() should reject completed state")
	}
}

func TestOperationsOnUnknownExecution(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if err := manager.Start("missing"); err == nil {
		t.Fatal("Start() should reject unknown execution")
	}

	if err := manager.Complete(
		"missing",
		execution.Result{},
	); err == nil {
		t.Fatal("Complete() should reject unknown execution")
	}

	if err := manager.Fail(
		"missing",
		errors.New("failure"),
	); err == nil {
		t.Fatal("Fail() should reject unknown execution")
	}

	if err := manager.Cancel("missing"); err == nil {
		t.Fatal("Cancel() should reject unknown execution")
	}
}

func TestFailRejectsNilError(t *testing.T) {
	manager, err := New(10)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	if err := manager.Fail("exec_1", nil); err == nil {
		t.Fatal("Fail() should reject nil error")
	}
}

func TestRetentionEvictsOldestTerminalExecution(t *testing.T) {
	manager, err := New(2)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	for i := 1; i <= 3; i++ {
		id := "exec_" + string(rune('0'+i))

		if _, err := manager.Create(
			id,
			testScenario(),
		); err != nil {
			t.Fatalf("Create(%s) error: %v", id, err)
		}

		if err := manager.Start(id); err != nil {
			t.Fatalf("Start(%s) error: %v", id, err)
		}

		if err := manager.Complete(
			id,
			execution.Result{ExecutionID: id},
		); err != nil {
			t.Fatalf("Complete(%s) error: %v", id, err)
		}

		time.Sleep(time.Millisecond)
	}

	if _, ok := manager.Get("exec_1"); ok {
		t.Fatal("oldest completed execution should have been evicted")
	}

	if _, ok := manager.Get("exec_2"); !ok {
		t.Fatal("exec_2 should still be retained")
	}

	if _, ok := manager.Get("exec_3"); !ok {
		t.Fatal("exec_3 should still be retained")
	}
}

func TestRetentionDoesNotEvictRunningExecution(t *testing.T) {
	manager, err := New(1)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	if _, err := manager.Create(
		"exec_1",
		testScenario(),
	); err != nil {
		t.Fatalf("Create(exec_1) error: %v", err)
	}

	if err := manager.Start("exec_1"); err != nil {
		t.Fatalf("Start(exec_1) error: %v", err)
	}

	if _, err := manager.Create(
		"exec_2",
		testScenario(),
	); err != nil {
		t.Fatalf("Create(exec_2) error: %v", err)
	}

	if _, ok := manager.Get("exec_1"); !ok {
		t.Fatal("running execution must not be evicted")
	}

	if _, ok := manager.Get("exec_2"); !ok {
		t.Fatal("new execution must be retained")
	}
}

func TestConcurrentManagerAccess(t *testing.T) {
	manager, err := New(100)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	const count = 50

	var wg sync.WaitGroup

	wg.Add(count)

	for i := 0; i < count; i++ {
		id := "exec_" + string(rune(i))

		go func(id string) {
			defer wg.Done()

			if _, err := manager.Create(
				id,
				testScenario(),
			); err != nil {
				t.Errorf("Create(%s) error: %v", id, err)
				return
			}

			if err := manager.Start(id); err != nil {
				t.Errorf("Start(%s) error: %v", id, err)
				return
			}

			if _, ok := manager.Get(id); !ok {
				t.Errorf("Get(%s) failed", id)
			}
		}(id)
	}

	wg.Wait()
}
