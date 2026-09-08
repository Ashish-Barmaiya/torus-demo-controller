package execution

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/request"
	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/torus"
)

const DefaultMaxConcurrency = 10

type RequestResult struct {
	Index      int
	StatusCode int
	Status     string
	Body       []byte
	BodySize   int64
	Duration   time.Duration
	Error      string
}

type Result struct {
	Scenario      demo.Scenario
	Requests      []RequestResult
	TotalDuration time.Duration
}

type Executor struct {
	generator     *request.Generator
	torusClient   *torus.Client
	maxConcurrent int
}

func NewExecutor(
	generator *request.Generator,
	torusClient *torus.Client,
	maxConcurrent int,
) (*Executor, error) {
	if generator == nil {
		return nil, fmt.Errorf("request generator must not be nil")
	}

	if torusClient == nil {
		return nil, fmt.Errorf("torus client must not be nil")
	}

	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrency
	}

	return &Executor{
		generator:     generator,
		torusClient:   torusClient,
		maxConcurrent: maxConcurrent,
	}, nil
}

func (e *Executor) Execute(
	ctx context.Context,
	scenario demo.Scenario,
) (Result, error) {
	if err := scenario.Validate(); err != nil {
		return Result{}, fmt.Errorf("validate scenario: %w", err)
	}

	start := time.Now()

	results := make([]RequestResult, scenario.RequestCount)

	sem := make(chan struct{}, e.maxConcurrent)

	var wg sync.WaitGroup

	for i := 0; i < scenario.RequestCount; i++ {
		index := i

		wg.Add(1)

		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[index] = RequestResult{
					Index: index,
					Error: ctx.Err().Error(),
				}
				return
			}

			defer func() {
				<-sem
			}()

			req, err := e.generator.Build(scenario)
			if err != nil {
				results[index] = RequestResult{
					Index: index,
					Error: err.Error(),
				}
				return
			}

			req = req.WithContext(ctx)

			result, err := e.torusClient.Do(req)

			requestResult := RequestResult{
				Index:      index,
				StatusCode: result.StatusCode,
				Status:     result.Status,
				Body:       result.Body,
				BodySize:   result.BodySize,
				Duration:   result.Duration,
			}

			if err != nil {
				requestResult.Error = err.Error()
			}

			results[index] = requestResult
		}()
	}

	wg.Wait()

	return Result{
		Scenario:      scenario,
		Requests:      results,
		TotalDuration: time.Since(start),
	}, nil
}
