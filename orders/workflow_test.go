package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type paymentAttempt struct {
	Key  string
	Body struct {
		OrderID  string `json:"order_id"`
		Amount   int64  `json:"amount_cents"`
		Currency string `json:"currency"`
	}
}

func recordPayment(t *testing.T, r *http.Request) paymentAttempt {
	t.Helper()
	var attempt paymentAttempt
	attempt.Key = r.Header.Get("Idempotency-Key")
	if err := json.NewDecoder(r.Body).Decode(&attempt.Body); err != nil {
		t.Errorf("payment body: %v", err)
	}
	return attempt
}

func TestChargeLostResponseRecoversWithoutDoubleCharge(t *testing.T) {
	t.Parallel()
	const id = "order-lost"
	var mu sync.Mutex
	var attempts []paymentAttempt
	var captures, lookups int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPost:
			attempt := recordPayment(t, r)
			attempts = append(attempts, attempt)
			if len(attempts) == 1 || attempt.Key == "" || attempt.Key != attempts[0].Key {
				captures++
			}
			if len(attempts) == 1 {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("hijack: %v", err)
					return
				}
				if err := conn.Close(); err != nil {
					t.Errorf("close lost response: %v", err)
				}
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			lookups++
			if r.URL.Query().Get("order_id") != id {
				t.Errorf("lookup order = %q", r.URL.Query().Get("order_id"))
			}
			writeJSON(w, http.StatusOK, []map[string]any{{"order_id": id, "amount_cents": 1200, "status": "captured"}})
		}
	}))
	defer server.Close()
	a := &app{paymentURL: server.URL, client: &http.Client{Timeout: 100 * time.Millisecond}}
	var breaker paymentBreaker
	for range 2 {
		status, err := a.charge(t.Context(), id, 1200, &breaker)
		if err != nil || status != "paid" {
			t.Fatalf("charge = %q, %v; want paid", status, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if captures != 1 || lookups != 1 || len(attempts) != 2 {
		t.Fatalf("captures=%d, lookups=%d, attempts=%d", captures, lookups, len(attempts))
	}
	if attempts[0].Key != id || attempts[0].Body.OrderID != id || attempts[0].Body.Amount != 1200 || attempts[0].Body.Currency != "RUB" {
		t.Fatalf("unexpected payment: %+v", attempts[0])
	}
	if !reflect.DeepEqual(attempts[0], attempts[1]) {
		t.Fatalf("retry changed payment: %+v -> %+v", attempts[0], attempts[1])
	}
}

func TestChargeSlowSuccessIsConfirmedByLookup(t *testing.T) {
	t.Parallel()
	const id = "order-slow"
	var posts, lookups atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			attempt := recordPayment(t, r)
			if attempt.Key != id {
				t.Errorf("key = %q, want %q", attempt.Key, id)
			}
			<-r.Context().Done()
			return
		}
		lookups.Add(1)
		writeJSON(w, http.StatusOK, []map[string]any{{"order_id": id, "amount_cents": 500, "status": "captured"}})
	}))
	defer server.Close()
	a := &app{paymentURL: server.URL, client: &http.Client{Timeout: 50 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	status, err := a.charge(ctx, id, 500, &paymentBreaker{})
	if err != nil || status != "paid" || posts.Load() != 1 || lookups.Load() != 1 {
		t.Fatalf("charge=%q, err=%v, posts=%d, lookups=%d", status, err, posts.Load(), lookups.Load())
	}
}

func TestChargeDelayedDeclineUsesSamePaymentOnReplay(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var attempts []paymentAttempt
	var lookups atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			lookups.Add(1)
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		attempt := recordPayment(t, r)
		mu.Lock()
		attempts = append(attempts, attempt)
		mu.Unlock()
		select {
		case <-r.Context().Done():
			return
		case <-time.After(150 * time.Millisecond):
			w.WriteHeader(http.StatusPaymentRequired)
		}
	}))
	defer server.Close()
	a := &app{paymentURL: server.URL, client: &http.Client{Timeout: 50 * time.Millisecond}}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	status, err := a.charge(ctx, "order-declined", 250, &paymentBreaker{})
	if err != nil || status != "release" {
		t.Fatalf("charge = %q, %v; want release", status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || lookups.Load() != 1 {
		t.Fatalf("attempts=%d, lookups=%d", len(attempts), lookups.Load())
	}
	if attempts[0].Key != "order-declined" || !reflect.DeepEqual(attempts[0], attempts[1]) {
		t.Fatalf("replay changed payment: %+v", attempts)
	}
}

func TestChargeLearnsResponseWaitForSlowDecline(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var attempts []paymentAttempt
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		attempt := recordPayment(t, r)
		mu.Lock()
		attempts = append(attempts, attempt)
		mu.Unlock()
		select {
		case <-r.Context().Done():
			return
		case <-time.After(150 * time.Millisecond):
			w.WriteHeader(http.StatusPaymentRequired)
		}
	}))
	defer server.Close()
	a := &app{paymentURL: server.URL, client: &http.Client{Timeout: 50 * time.Millisecond}}
	breaker := paymentBreaker{responseWait: 100 * time.Millisecond}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if status, err := a.charge(ctx, "order-slow-decline", 250, &breaker); err == nil || status != "" {
		t.Fatalf("first charge = %q, %v; want unknown outcome", status, err)
	}
	if breaker.responseWait != 200*time.Millisecond || !breaker.retryAt.IsZero() {
		t.Fatalf("reachable slow payment should extend wait without opening circuit: %+v", breaker)
	}
	if status, err := a.charge(ctx, "order-slow-decline", 250, &breaker); err != nil || status != "release" {
		t.Fatalf("charge with learned wait = %q, %v; want release", status, err)
	}
	if breaker.responseWait != 200*time.Millisecond || breaker.failures != 0 {
		t.Fatalf("decline must retain learned response wait: %+v", breaker)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 4 {
		t.Fatalf("payment attempts = %d, want 4", len(attempts))
	}
	for _, attempt := range attempts {
		if attempt.Key != "order-slow-decline" || !reflect.DeepEqual(attempt, attempts[0]) {
			t.Fatalf("learning response wait changed payment: %+v", attempts)
		}
	}
}

func TestChargeRetriesTransientErrorWithSameKey(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var attempts []paymentAttempt
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected lookup after explicit 500")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, recordPayment(t, r))
		if len(attempts) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()
	a := &app{paymentURL: server.URL, client: server.Client()}
	var breaker paymentBreaker
	if status, err := a.charge(t.Context(), "order-retry", 100, &breaker); err == nil || status != "" {
		t.Fatalf("failed charge = %q, %v", status, err)
	}
	if breaker.failures != 1 || !breaker.retryAt.IsZero() {
		t.Fatalf("one transient error opened breaker: %+v", breaker)
	}
	if status, err := a.charge(t.Context(), "order-retry", 100, &breaker); err != nil || status != "paid" {
		t.Fatalf("retry = %q, %v", status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || attempts[0].Key != "order-retry" || !reflect.DeepEqual(attempts[0], attempts[1]) {
		t.Fatalf("payment changed: %+v", attempts)
	}
	if breaker.failures != 0 {
		t.Fatalf("successful retry retained failures: %d", breaker.failures)
	}
}

func TestChargeBreakerIsSharedAcrossOrdersUntilProbe(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var recovered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method == http.MethodPost {
			_ = recordPayment(t, r)
		}
		if recovered.Load() {
			w.WriteHeader(http.StatusCreated)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	a := &app{paymentURL: server.URL, client: &http.Client{Timeout: 50 * time.Millisecond}}
	var breaker paymentBreaker
	if _, err := a.charge(t.Context(), "order-before-outage", 100, &breaker); err == nil {
		t.Fatal("unavailable payment succeeded")
	}
	if !breaker.retryAt.After(time.Now()) || calls.Load() != 2 {
		t.Fatalf("breaker=%+v, calls=%d; want open after POST and GET", breaker, calls.Load())
	}
	for _, id := range []string{"another-order", "third-order", "order-before-outage"} {
		if _, err := a.charge(t.Context(), id, 100, &breaker); err == nil {
			t.Fatal("open breaker permitted payment")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("open breaker made calls: %d", calls.Load())
	}
	recovered.Store(true)
	breaker.retryAt = time.Now().Add(-time.Second)
	if status, err := a.charge(t.Context(), "order-after-recovery", 100, &breaker); err != nil || status != "paid" {
		t.Fatalf("recovery probe = %q, %v", status, err)
	}
	if calls.Load() != 3 || breaker.failures != 0 {
		t.Fatalf("recovery calls=%d, failures=%d", calls.Load(), breaker.failures)
	}
}
