package torus

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type Result struct {
	StatusCode int
	Status     string
	Headers    http.Header
	Body       []byte
	BodySize   int64
	Duration   time.Duration
}

type Client struct {
	httpClient *http.Client
}

func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *Client) Do(req *http.Request) (Result, error) {
	if req == nil {
		return Result{}, fmt.Errorf("request must not be nil")
	}

	start := time.Now()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Result{
			Duration: time.Since(start),
		}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Headers:    resp.Header.Clone(),
			Duration:   time.Since(start),
		}, fmt.Errorf("read response body: %w", err)
	}

	duration := time.Since(start)

	return Result{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Headers:    resp.Header.Clone(),
		Body:       body,
		BodySize:   int64(len(body)),
		Duration:   duration,
	}, nil
}
