package request

import (
	"fmt"
	"math/rand"
	"sync"
)

type UserRef struct {
	PathID string
	ID     string
}

type OrderRef struct {
	PathID string
	ID     string
}

type IDSource interface {
	User() UserRef
	Order() OrderRef
}

type RandomIDSource struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func NewRandomIDSource(seed int64) *RandomIDSource {
	return &RandomIDSource{
		rng: rand.New(rand.NewSource(seed)),
	}
}

func (s *RandomIDSource) User() UserRef {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.rng.Intn(20) + 1

	return UserRef{
		PathID: fmt.Sprintf("%d", id),
		ID:     fmt.Sprintf("usr_%06d", id),
	}
}

func (s *RandomIDSource) Order() OrderRef {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.rng.Intn(20) + 1

	return OrderRef{
		PathID: fmt.Sprintf("%d", id),
		ID:     fmt.Sprintf("ord_%06d", id),
	}
}
