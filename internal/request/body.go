package request

import (
	"encoding/json"
	"fmt"
)

const defaultFiller = "torus-demo-request"

type requestEnvelope struct {
	Data    any    `json:"data"`
	Payload string `json:"payload,omitempty"`
}

func generateJSONBody(target int64, data any) ([]byte, error) {
	if target < 0 {
		return nil, fmt.Errorf("request size must not be negative")
	}

	base := requestEnvelope{
		Data: data,
	}

	// 0b means: return a normal valid request body without
	// requesting a specific size.
	if target == 0 {
		body, err := json.Marshal(base)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}

		return body, nil
	}

	low := 0
	high := int(target)

	for low <= high {
		mid := (low + high) / 2

		base.Payload = makeFiller(mid)

		body, err := json.Marshal(base)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}

		switch {
		case len(body) < int(target):
			low = mid + 1

		case len(body) > int(target):
			high = mid - 1

		default:
			return body, nil
		}
	}

	// Refine around the binary-search boundary.
	best := []byte(nil)
	bestDistance := int(target)

	start := max(0, high-4)
	end := min(int(target), high+4)

	for fillerLength := start; fillerLength <= end; fillerLength++ {
		base.Payload = makeFiller(fillerLength)

		body, err := json.Marshal(base)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}

		distance := abs(len(body) - int(target))

		if distance < bestDistance {
			best = body
			bestDistance = distance
		}

		if distance == 0 {
			return body, nil
		}
	}

	if best == nil {
		return nil, fmt.Errorf(
			"unable to generate request body near target size %d",
			target,
		)
	}

	return best, nil
}

func makeFiller(length int) string {
	if length <= 0 {
		return ""
	}

	pattern := []byte(defaultFiller)
	filler := make([]byte, length)

	for i := range filler {
		filler[i] = pattern[i%len(pattern)]
	}

	return string(filler)
}

func abs(value int) int {
	if value < 0 {
		return -value
	}

	return value
}
