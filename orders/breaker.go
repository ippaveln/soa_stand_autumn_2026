package main

import (
	"sync"
	"time"
)

// One breaker covers both POST and reconciliation GET requests, across all orders.
// After an outage only one probe is allowed; failed probes back off to 10 s.
type paymentBreaker struct {
	mu       sync.Mutex
	until    time.Time
	probing  bool
	delay    time.Duration
	failures int
}

func (b *paymentBreaker) allow(now time.Time) (probe bool, wait time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.until.IsZero() {
		return false, 0
	}
	if now.Before(b.until) {
		return false, b.until.Sub(now)
	}
	if b.probing {
		return false, 250 * time.Millisecond
	}
	b.probing = true
	return true, 0
}

func (b *paymentBreaker) finish(now time.Time, probe bool, networkFailure bool, serverFailure bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
		if networkFailure || serverFailure {
			b.delay = min(max(b.delay*2, 5*time.Second), 10*time.Second)
			b.until = now.Add(b.delay)
			return
		}
		b.until = time.Time{}
		b.delay, b.failures = 0, 0
		return
	}
	// Ignore older in-flight calls once another call opened the circuit.
	if !b.until.IsZero() {
		return
	}
	if !networkFailure && !serverFailure {
		b.failures = 0
		return
	}
	b.failures++
	if networkFailure || b.failures >= 5 {
		b.delay = 5 * time.Second
		b.until = now.Add(b.delay)
	}
}
