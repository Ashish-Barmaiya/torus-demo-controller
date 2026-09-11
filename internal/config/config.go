package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Host string
	Port int

	TorusBaseURL string

	TorusTimeout time.Duration

	ExecutorMaxConcurrentRequests int

	RateLimitMaxConcurrentExecutions int
	RateLimitMaxRequestsPerWindow    int
	RateLimitWindow                  time.Duration
}

func Load() (Config, error) {
	port, err := loadInt("PORT", 8081)
	if err != nil {
		return Config{}, err
	}

	torusTimeout, err := loadDuration("TORUS_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	executorMaxConcurrentRequests, err := loadInt(
		"EXECUTOR_MAX_CONCURRENT_REQUESTS",
		10,
	)
	if err != nil {
		return Config{}, err
	}

	rateLimitMaxConcurrentExecutions, err := loadInt(
		"RATE_LIMIT_MAX_CONCURRENT_EXECUTIONS",
		10,
	)
	if err != nil {
		return Config{}, err
	}

	rateLimitMaxRequestsPerWindow, err := loadInt(
		"RATE_LIMIT_MAX_REQUESTS_PER_WINDOW",
		100,
	)
	if err != nil {
		return Config{}, err
	}

	rateLimitWindow, err := loadDuration(
		"RATE_LIMIT_WINDOW",
		time.Second,
	)
	if err != nil {
		return Config{}, err
	}

	torusBaseURL := os.Getenv("TORUS_BASE_URL")
	if torusBaseURL == "" {
		torusBaseURL = "http://localhost:8080"
	}

	host := os.Getenv("HOST")
	if host == "" {
		host = "0.0.0.0"
	}

	return Config{
		Host:                             host,
		Port:                             port,
		TorusBaseURL:                     torusBaseURL,
		TorusTimeout:                     torusTimeout,
		ExecutorMaxConcurrentRequests:    executorMaxConcurrentRequests,
		RateLimitMaxConcurrentExecutions: rateLimitMaxConcurrentExecutions,
		RateLimitMaxRequestsPerWindow:    rateLimitMaxRequestsPerWindow,
		RateLimitWindow:                  rateLimitWindow,
	}, nil
}

func (c Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func loadInt(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid %s %q: %w",
			name,
			value,
			err,
		)
	}

	return parsed, nil
}

func loadDuration(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid %s %q: %w",
			name,
			value,
			err,
		)
	}

	return parsed, nil
}
