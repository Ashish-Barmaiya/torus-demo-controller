package request

import (
	"encoding/json"
	"testing"
)

func TestGenerateJSONBodyDefault(t *testing.T) {
	tests := []struct {
		name string
		data map[string]any
		keys []string
	}{
		{
			name: "user",
			data: map[string]any{
				"name":  "Demo User",
				"email": "demo.user@example.com",
				"plan":  "pro",
			},
			keys: []string{"name", "email", "plan"},
		},
		{
			name: "order",
			data: map[string]any{
				"customer_id": "usr_000005",
				"currency":    "USD",
				"total":       129900,
			},
			keys: []string{"customer_id", "currency", "total"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := generateJSONBody(0, tt.data)
			if err != nil {
				t.Fatalf("generateJSONBody() error: %v", err)
			}

			if len(body) == 0 {
				t.Fatal("expected non-empty body")
			}

			var decoded map[string]any
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}

			for _, key := range tt.keys {
				if _, ok := decoded[key]; !ok {
					t.Fatalf("request body missing %q", key)
				}
			}
		})
	}
}

func TestGenerateJSONBodyExactSize(t *testing.T) {
	tests := []struct {
		name   string
		target int64
	}{
		{
			name:   "1kb",
			target: 1 << 10,
		},
		{
			name:   "16kb",
			target: 16 << 10,
		},
		{
			name:   "64kb",
			target: 64 << 10,
		},
		{
			name:   "256kb",
			target: 256 << 10,
		},
		{
			name:   "1mb",
			target: 1 << 20,
		},
		{
			name:   "4mb",
			target: 4 << 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := generateJSONBody(
				tt.target,
				map[string]any{
					"customer_id": "usr_000005",
					"currency":    "USD",
					"total":       129900,
				},
			)
			if err != nil {
				t.Fatalf("generateJSONBody() error: %v", err)
			}

			if len(body) != int(tt.target) {
				t.Fatalf(
					"body size = %d, want %d",
					len(body),
					tt.target,
				)
			}

			var decoded map[string]any

			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatalf("invalid JSON: %v", err)
			}

			for _, key := range []string{"customer_id", "currency", "total"} {
				if _, ok := decoded[key]; !ok {
					t.Fatalf("request body missing %q", key)
				}
			}
		})
	}
}

func TestGenerateJSONBodyRejectsNegativeSize(t *testing.T) {
	_, err := generateJSONBody(
		-1,
		map[string]any{"name": "Demo User"},
	)

	if err == nil {
		t.Fatal("expected negative size to be rejected")
	}
}
