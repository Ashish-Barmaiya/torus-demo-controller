package ratelimit

import (
	"sync"
	"time"
)

const (
	DefaultMaxConcurrentExecutions = 10
	DefaultMaxRequestsPerWindow    = 100
	DefaultWindow                  = time.Second
)

type Limiter struct {
	mu            sync.Mutex
	maxConcurrent int
	maxRequests   int
	window        time.Duration
	clients       map[string]*clientState
}

type clientState struct {
	active   int
	requests []time.Time
}

func New(maxConcurrent int, maxRequests int, window time.Duration) *Limiter {
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrentExecutions
	}

	if maxRequests <= 0 {
		maxRequests = DefaultMaxRequestsPerWindow
	}

	if window <= 0 {
		window = DefaultWindow
	}

	return &Limiter{
		maxConcurrent: maxConcurrent,
		maxRequests:   maxRequests,
		window:        window,
		clients:       make(map[string]*clientState),
	}
}

func NewDefault() *Limiter {
	return New(
		DefaultMaxConcurrentExecutions,
		DefaultMaxRequestsPerWindow,
		DefaultWindow,
	)
}

func (l *Limiter) Allow(clientKey string) bool {
	clientKey = normalizeClientKey(clientKey)

	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.clients[clientKey]
	if !ok {
		state = &clientState{}
		l.clients[clientKey] = state
	}

	state.requests = removeExpiredRequests(
		state.requests,
		now,
		l.window,
	)

	if len(state.requests) >= l.maxRequests {
		l.cleanupClient(clientKey, state)
		return false
	}

	if state.active >= l.maxConcurrent {
		l.cleanupClient(clientKey, state)
		return false
	}

	state.requests = append(state.requests, now)
	state.active++

	return true
}

func (l *Limiter) Done(clientKey string) {
	clientKey = normalizeClientKey(clientKey)

	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.clients[clientKey]
	if !ok || state == nil {
		return
	}

	if state.active > 0 {
		state.active--
	}

	// Do not remove request timestamps here.
	// Done releases concurrency capacity only.
	l.cleanupClient(clientKey, state)
}

func normalizeClientKey(clientKey string) string {
	if clientKey == "" {
		return "unknown"
	}

	return clientKey
}

func removeExpiredRequests(
	requests []time.Time,
	now time.Time,
	window time.Duration,
) []time.Time {
	keep := requests[:0]

	for _, requestTime := range requests {
		if now.Sub(requestTime) < window {
			keep = append(keep, requestTime)
		}
	}

	return keep
}

func (l *Limiter) cleanupClient(
	clientKey string,
	state *clientState,
) {
	if state.active == 0 && len(state.requests) == 0 {
		delete(l.clients, clientKey)
	}
}
