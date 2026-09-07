package request

import (
	"fmt"
	"math/rand"
	"sync"
)

type IDSource interface {
	UserID() string
	OrderID() string
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

func (s *RandomIDSource) UserID() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return fmt.Sprintf("usr_%06d", s.rng.Intn(20)+1)
}

func (s *RandomIDSource) OrderID() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return fmt.Sprintf("ord_%06d", s.rng.Intn(20)+1)
}
