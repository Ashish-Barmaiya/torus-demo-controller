package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	unset := []string{
		"HOST",
		"PORT",
		"TORUS_BASE_URL",
		"TORUS_TIMEOUT",
		"EXECUTOR_MAX_CONCURRENT_REQUESTS",
		"RATE_LIMIT_MAX_CONCURRENT_EXECUTIONS",
		"RATE_LIMIT_MAX_REQUESTS_PER_WINDOW",
		"RATE_LIMIT_WINDOW",
	}

	for _, name := range unset {
		t.Setenv(name, "")
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Host != "0.0.0.0" {
		t.Fatalf("Host = %q, want %q", cfg.Host, "0.0.0.0")
	}
	if cfg.Port != 8081 {
		t.Fatalf("Port = %d, want 8081", cfg.Port)
	}
	if cfg.TorusBaseURL != "http://localhost:8080" {
		t.Fatalf("TorusBaseURL = %q, want default URL", cfg.TorusBaseURL)
	}
	if cfg.TorusTimeout != 10*time.Second {
		t.Fatalf("TorusTimeout = %s, want 10s", cfg.TorusTimeout)
	}
	if cfg.ExecutorMaxConcurrentRequests != 10 {
		t.Fatalf("ExecutorMaxConcurrentRequests = %d, want 10", cfg.ExecutorMaxConcurrentRequests)
	}
	if cfg.RateLimitMaxConcurrentExecutions != 10 {
		t.Fatalf("RateLimitMaxConcurrentExecutions = %d, want 10", cfg.RateLimitMaxConcurrentExecutions)
	}
	if cfg.RateLimitMaxRequestsPerWindow != 100 {
		t.Fatalf("RateLimitMaxRequestsPerWindow = %d, want 100", cfg.RateLimitMaxRequestsPerWindow)
	}
	if cfg.RateLimitWindow != time.Second {
		t.Fatalf("RateLimitWindow = %s, want 1s", cfg.RateLimitWindow)
	}
}

func TestLoadEnvironment(t *testing.T) {
	t.Setenv("HOST", "127.0.0.1")
	t.Setenv("PORT", "9090")
	t.Setenv("TORUS_BASE_URL", "http://torus:8080")
	t.Setenv("TORUS_TIMEOUT", "5s")
	t.Setenv("EXECUTOR_MAX_CONCURRENT_REQUESTS", "3")
	t.Setenv("RATE_LIMIT_MAX_CONCURRENT_EXECUTIONS", "4")
	t.Setenv("RATE_LIMIT_MAX_REQUESTS_PER_WINDOW", "20")
	t.Setenv("RATE_LIMIT_WINDOW", "2s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Host != "127.0.0.1" {
		t.Fatalf("Host = %q", cfg.Host)
	}
	if cfg.Port != 9090 {
		t.Fatalf("Port = %d", cfg.Port)
	}
	if cfg.TorusBaseURL != "http://torus:8080" {
		t.Fatalf("TorusBaseURL = %q", cfg.TorusBaseURL)
	}
	if cfg.TorusTimeout != 5*time.Second {
		t.Fatalf("TorusTimeout = %s", cfg.TorusTimeout)
	}
	if cfg.ExecutorMaxConcurrentRequests != 3 {
		t.Fatalf("ExecutorMaxConcurrentRequests = %d", cfg.ExecutorMaxConcurrentRequests)
	}
	if cfg.RateLimitMaxConcurrentExecutions != 4 {
		t.Fatalf("RateLimitMaxConcurrentExecutions = %d", cfg.RateLimitMaxConcurrentExecutions)
	}
	if cfg.RateLimitMaxRequestsPerWindow != 20 {
		t.Fatalf("RateLimitMaxRequestsPerWindow = %d", cfg.RateLimitMaxRequestsPerWindow)
	}
	if cfg.RateLimitWindow != 2*time.Second {
		t.Fatalf("RateLimitWindow = %s", cfg.RateLimitWindow)
	}
}

func TestLoadRejectsInvalidInteger(t *testing.T) {
	t.Setenv("PORT", "not-a-number")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid PORT error")
	}
}

func TestLoadRejectsInvalidDuration(t *testing.T) {
	t.Setenv("TORUS_TIMEOUT", "not-a-duration")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid TORUS_TIMEOUT error")
	}
}

func TestAddress(t *testing.T) {
	cfg := Config{
		Host: "127.0.0.1",
		Port: 8081,
	}

	if got := cfg.Address(); got != "127.0.0.1:8081" {
		t.Fatalf("Address() = %q, want %q", got, "127.0.0.1:8081")
	}
}
