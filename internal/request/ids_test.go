package request

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestRandomIDSourceUserID(t *testing.T) {
	source := NewRandomIDSource(42)

	for i := 0; i < 100; i++ {
		id := source.UserID()

		const prefix = "usr_"

		if !strings.HasPrefix(id, prefix) {
			t.Fatalf("invalid user ID %q", id)
		}

		value, err := strconv.Atoi(strings.TrimPrefix(id, prefix))
		if err != nil {
			t.Fatalf("invalid user ID %q: %v", id, err)
		}

		if value < 1 || value > 20 {
			t.Fatalf("user ID out of range %q", id)
		}

		if id != fmt.Sprintf("usr_%06d", value) {
			t.Fatalf("invalid user ID format %q", id)
		}
	}
}

func TestRandomIDSourceOrderID(t *testing.T) {
	source := NewRandomIDSource(42)

	for i := 0; i < 100; i++ {
		id := source.OrderID()

		const prefix = "ord_"

		if !strings.HasPrefix(id, prefix) {
			t.Fatalf("invalid order ID %q", id)
		}

		value, err := strconv.Atoi(strings.TrimPrefix(id, prefix))
		if err != nil {
			t.Fatalf("invalid order ID %q: %v", id, err)
		}

		if value < 1 || value > 20 {
			t.Fatalf("order ID out of range %q", id)
		}

		if id != fmt.Sprintf("ord_%06d", value) {
			t.Fatalf("invalid order ID format %q", id)
		}
	}
}

func TestRandomIDSourceDeterministic(t *testing.T) {
	first := NewRandomIDSource(1234)
	second := NewRandomIDSource(1234)

	for i := 0; i < 100; i++ {
		if got, want := first.UserID(), second.UserID(); got != want {
			t.Fatalf("user ID = %q, want %q", got, want)
		}

		if got, want := first.OrderID(), second.OrderID(); got != want {
			t.Fatalf("order ID = %q, want %q", got, want)
		}
	}
}
