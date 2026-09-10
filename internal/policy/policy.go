package policy

import (
	"fmt"
	"time"

	"github.com/Ashish-Barmaiya/torus-demo-controller/internal/demo"
)

const (
	DefaultMaxRequests                     = 50
	DefaultMaxConcurrency                  = 10
	DefaultMaxRequestBodyBytes       int64 = 4 << 20
	DefaultMaxResponseBodyBytes      int64 = 4 << 20
	DefaultMaxAggregateRequestBytes  int64 = 16 << 20
	DefaultMaxAggregateResponseBytes int64 = 16 << 20
	DefaultMaxExecutionDuration            = 10 * time.Second
)

type Policy struct {
	MaxRequests               int
	MaxConcurrency            int
	MaxRequestBodyBytes       int64
	MaxResponseBodyBytes      int64
	MaxAggregateRequestBytes  int64
	MaxAggregateResponseBytes int64
	MaxExecutionDuration      time.Duration
}

func Default() Policy {
	return Policy{
		MaxRequests:               DefaultMaxRequests,
		MaxConcurrency:            DefaultMaxConcurrency,
		MaxRequestBodyBytes:       DefaultMaxRequestBodyBytes,
		MaxResponseBodyBytes:      DefaultMaxResponseBodyBytes,
		MaxAggregateRequestBytes:  DefaultMaxAggregateRequestBytes,
		MaxAggregateResponseBytes: DefaultMaxAggregateResponseBytes,
		MaxExecutionDuration:      DefaultMaxExecutionDuration,
	}
}

func (p Policy) Validate(scenario demo.Scenario) error {
	if err := scenario.Validate(); err != nil {
		return err
	}

	if scenario.RequestCount > p.MaxRequests {
		return fmt.Errorf(
			"request count %d exceeds maximum %d",
			scenario.RequestCount,
			p.MaxRequests,
		)
	}

	if p.MaxConcurrency <= 0 {
		return fmt.Errorf("max concurrency must be greater than zero")
	}

	if scenario.RequestSize.Bytes() > p.MaxRequestBodyBytes {
		return fmt.Errorf(
			"request body size %d exceeds maximum %d",
			scenario.RequestSize.Bytes(),
			p.MaxRequestBodyBytes,
		)
	}

	if scenario.ResponseSize.Bytes() > p.MaxResponseBodyBytes {
		return fmt.Errorf(
			"response body size %d exceeds maximum %d",
			scenario.ResponseSize.Bytes(),
			p.MaxResponseBodyBytes,
		)
	}

	aggregateRequestBytes :=
		int64(scenario.RequestCount) * scenario.RequestSize.Bytes()

	if aggregateRequestBytes > p.MaxAggregateRequestBytes {
		return fmt.Errorf(
			"aggregate request body size %d exceeds maximum %d",
			aggregateRequestBytes,
			p.MaxAggregateRequestBytes,
		)
	}

	aggregateResponseBytes :=
		int64(scenario.RequestCount) * scenario.ResponseSize.Bytes()

	if aggregateResponseBytes > p.MaxAggregateResponseBytes {
		return fmt.Errorf(
			"aggregate response body size %d exceeds maximum %d",
			aggregateResponseBytes,
			p.MaxAggregateResponseBytes,
		)
	}

	if p.MaxExecutionDuration <= 0 {
		return fmt.Errorf("maximum execution duration must be greater than zero")
	}

	return nil
}
