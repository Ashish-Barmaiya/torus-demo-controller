package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimiterAllowsUpToMaxConcurrent(t *testing.T) {
	limiter := New(2, 10, time.Hour)

	if !limiter.Allow("client-a") {
		t.Fatal("first allow should succeed")
	}

	if !limiter.Allow("client-a") {
		t.Fatal("second allow should succeed")
	}

	if limiter.Allow("client-a") {
		t.Fatal("third allow should fail when concurrent slots are exhausted")
	}

	limiter.Done("client-a")

	if !limiter.Allow("client-a") {
		t.Fatal("allow after Done should succeed")
	}
}

func TestLimiterDoneDoesNotUndoRateUsage(t *testing.T) {
	limiter := New(10, 2, time.Hour)

	if !limiter.Allow("client-a") {
		t.Fatal("first allow should succeed")
	}

	if !limiter.Allow("client-a") {
		t.Fatal("second allow should succeed")
	}

	limiter.Done("client-a")

	if limiter.Allow("client-a") {
		t.Fatal("Done must not restore rate-limit capacity")
	}
}

func TestLimiterTracksRateWindowPerClient(t *testing.T) {
	limiter := New(10, 2, 50*time.Millisecond)

	if !limiter.Allow("client-a") {
		t.Fatal("first allow should succeed")
	}

	if !limiter.Allow("client-a") {
		t.Fatal("second allow should succeed")
	}

	if limiter.Allow("client-a") {
		t.Fatal("third allow should fail while rate window is active")
	}

	time.Sleep(60 * time.Millisecond)

	if !limiter.Allow("client-a") {
		t.Fatal("allow should succeed after rate window expires")
	}
}

func TestLimiterIsolatesClients(t *testing.T) {
	limiter := New(1, 1, time.Hour)

	if !limiter.Allow("client-a") {
		t.Fatal("client-a first allow should succeed")
	}

	if limiter.Allow("client-a") {
		t.Fatal("client-a second allow should fail")
	}

	if !limiter.Allow("client-b") {
		t.Fatal("client-b should be independent of client-a")
	}
}

func TestLimiterReleasesConcurrencyForSameClient(t *testing.T) {
	limiter := New(1, 10, time.Hour)

	if !limiter.Allow("client-a") {
		t.Fatal("first allow should succeed")
	}

	if limiter.Allow("client-a") {
		t.Fatal("second allow should fail while first is active")
	}

	limiter.Done("client-a")

	if !limiter.Allow("client-a") {
		t.Fatal("same client should be admitted after Done")
	}
}

func TestLimiterBlocksConcurrentAdmissionsForSameClient(t *testing.T) {
	const goroutineCount = 20

	limiter := New(1, 100, time.Hour)

	var allowed int32

	var wg sync.WaitGroup

	wg.Add(goroutineCount)

	for i := 0; i < goroutineCount; i++ {
		go func() {
			defer wg.Done()

			if limiter.Allow("client-a") {
				atomic.AddInt32(&allowed, 1)
			}
		}()
	}

	wg.Wait()

	if allowed != 1 {
		t.Fatalf(
			"allowed = %d, want exactly 1",
			allowed,
		)
	}
}

func TestLimiterAllowsDifferentClientsConcurrently(t *testing.T) {
	const goroutineCount = 20

	limiter := New(1, 100, time.Hour)

	var allowed int32

	var wg sync.WaitGroup

	wg.Add(goroutineCount)

	for i := 0; i < goroutineCount; i++ {
		client := string(rune('a' + i))

		go func(client string) {
			defer wg.Done()

			if limiter.Allow(client) {
				atomic.AddInt32(&allowed, 1)
			}
		}(client)
	}

	wg.Wait()

	if allowed != goroutineCount {
		t.Fatalf(
			"allowed = %d, want %d",
			allowed,
			goroutineCount,
		)
	}
}

func TestLimiterRemovesExpiredRequestsOnNextAllow(t *testing.T) {
	limiter := New(1, 1, 10*time.Millisecond)

	if !limiter.Allow("client-a") {
		t.Fatal("first allow should succeed")
	}

	limiter.Done("client-a")

	time.Sleep(20 * time.Millisecond)

	if !limiter.Allow("client-a") {
		t.Fatal("allow after window expires should succeed")
	}

	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	state, ok := limiter.clients["client-a"]
	if !ok {
		t.Fatal("client state should exist after successful allow")
	}

	if len(state.requests) != 1 {
		t.Fatalf(
			"stored request timestamps = %d, want 1",
			len(state.requests),
		)
	}
}

func TestLimiterNormalizesEmptyClientKey(t *testing.T) {
	limiter := New(1, 10, time.Hour)

	if !limiter.Allow("") {
		t.Fatal("empty client key should still be admitted")
	}

	if limiter.Allow("unknown") {
		t.Fatal("empty client key and unknown key should map to the same client")
	}
}

func TestLimiterDefaultsInvalidConfiguration(t *testing.T) {
	limiter := New(0, 0, 0)

	if limiter.maxConcurrent != DefaultMaxConcurrentExecutions {
		t.Fatalf(
			"maxConcurrent = %d, want %d",
			limiter.maxConcurrent,
			DefaultMaxConcurrentExecutions,
		)
	}

	if limiter.maxRequests != DefaultMaxRequestsPerWindow {
		t.Fatalf(
			"maxRequests = %d, want %d",
			limiter.maxRequests,
			DefaultMaxRequestsPerWindow,
		)
	}

	if limiter.window != DefaultWindow {
		t.Fatalf(
			"window = %s, want %s",
			limiter.window,
			DefaultWindow,
		)
	}
}
