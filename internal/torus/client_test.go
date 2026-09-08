package torus

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientDo(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Test", "true")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}),
	)
	defer server.Close()

	client := NewClient(5 * time.Second)

	req, err := http.NewRequest(
		http.MethodGet,
		server.URL,
		nil,
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	result, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}

	if result.StatusCode != http.StatusOK {
		t.Fatalf(
			"status code = %d, want %d",
			result.StatusCode,
			http.StatusOK,
		)
	}

	if result.Status != "200 OK" {
		t.Fatalf(
			"status = %q, want %q",
			result.Status,
			"200 OK",
		)
	}

	if result.BodySize != int64(len(result.Body)) {
		t.Fatalf(
			"body size = %d, actual body length = %d",
			result.BodySize,
			len(result.Body),
		)
	}

	if string(result.Body) != `{"status":"ok"}` {
		t.Fatalf(
			"body = %q",
			string(result.Body),
		)
	}

	if result.Headers.Get("X-Test") != "true" {
		t.Fatal("expected X-Test response header")
	}

	if result.Duration <= 0 {
		t.Fatal("expected positive duration")
	}
}

func TestClientDoPreservesRequest(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf(
					"method = %s, want %s",
					r.Method,
					http.MethodPost,
				)
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
				return
			}

			if string(body) != `{"hello":"world"}` {
				t.Errorf("body = %q", string(body))
			}

			w.WriteHeader(http.StatusCreated)
		}),
	)
	defer server.Close()

	client := NewClient(5 * time.Second)

	req, err := http.NewRequest(
		http.MethodPost,
		server.URL,
		strings.NewReader(`{"hello":"world"}`),
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	result, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error: %v", err)
	}

	if result.StatusCode != http.StatusCreated {
		t.Fatalf(
			"status code = %d, want %d",
			result.StatusCode,
			http.StatusCreated,
		)
	}
}

func TestClientDoNilRequest(t *testing.T) {
	client := NewClient(time.Second)

	_, err := client.Do(nil)

	if err == nil {
		t.Fatal("expected nil request to return an error")
	}
}

func TestClientDoTimeout(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(250 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}),
	)
	defer server.Close()

	client := NewClient(50 * time.Millisecond)

	req, err := http.NewRequest(
		http.MethodGet,
		server.URL,
		nil,
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}

	start := time.Now()

	result, err := client.Do(req)

	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error")
	}

	if result.Duration <= 0 {
		t.Fatal("expected duration to be recorded")
	}

	if elapsed > time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}
}
