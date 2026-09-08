package request

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestRandomIDSourceUser(t *testing.T) {
	source := NewRandomIDSource(42)

	for i := 0; i < 100; i++ {
		assertUserRef(t, source.User())
	}
}

func TestRandomIDSourceOrder(t *testing.T) {
	source := NewRandomIDSource(42)

	for i := 0; i < 100; i++ {
		assertOrderRef(t, source.Order())
	}
}

func TestRandomIDSourceDeterministic(t *testing.T) {
	first := NewRandomIDSource(1234)
	second := NewRandomIDSource(1234)

	for i := 0; i < 100; i++ {
		if got, want := first.User(), second.User(); got != want {
			t.Fatalf("user reference = %+v, want %+v", got, want)
		}

		if got, want := first.Order(), second.Order(); got != want {
			t.Fatalf("order reference = %+v, want %+v", got, want)
		}
	}
}

func TestRandomIDSourceConcurrent(t *testing.T) {
	source := NewRandomIDSource(42)
	const goroutineCount = 16
	const callsPerGoroutine = 100

	var waitGroup sync.WaitGroup
	waitGroup.Add(goroutineCount)

	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer waitGroup.Done()

			for j := 0; j < callsPerGoroutine; j++ {
				assertUserRef(t, source.User())
				assertOrderRef(t, source.Order())
			}
		}()
	}

	waitGroup.Wait()
}

func assertUserRef(t *testing.T, user UserRef) {
	t.Helper()

	value, err := strconv.Atoi(user.PathID)
	if err != nil {
		t.Fatalf("invalid user path ID %q: %v", user.PathID, err)
	}

	if value < 1 || value > 20 {
		t.Fatalf("user path ID out of range %q", user.PathID)
	}

	wantID := fmt.Sprintf("usr_%06d", value)
	if user.ID != wantID {
		t.Fatalf("user ID = %q, want %q", user.ID, wantID)
	}
}

func assertOrderRef(t *testing.T, order OrderRef) {
	t.Helper()

	value, err := strconv.Atoi(order.PathID)
	if err != nil {
		t.Fatalf("invalid order path ID %q: %v", order.PathID, err)
	}

	if value < 1 || value > 20 {
		t.Fatalf("order path ID out of range %q", order.PathID)
	}

	wantID := fmt.Sprintf("ord_%06d", value)
	if order.ID != wantID {
		t.Fatalf("order ID = %q, want %q", order.ID, wantID)
	}
}
