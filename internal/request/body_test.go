package request

import (
	"encoding/json"
	"testing"
)

func TestGenerateJSONBodyDefault(t *testing.T) {
	body, err := generateJSONBody(
		0,
		map[string]any{
			"name":  "Demo User",
			"email": "demo.user@example.com",
			"plan":  "pro",
		},
	)
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
					"name":  "Demo User",
					"email": "demo.user@example.com",
					"plan":  "pro",
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

			if _, ok := decoded["data"]; !ok {
				t.Fatal("response body missing data field")
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
