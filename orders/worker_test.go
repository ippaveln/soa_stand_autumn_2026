package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPaymentRetriesUseSameKey(t *testing.T) {
	var mu sync.Mutex
	var key, body string
	requests, charges := 0, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requests++
		var value map[string]any
		_ = json.NewDecoder(r.Body).Decode(&value)
		b, _ := json.Marshal(value)
		if requests == 1 {
			key, body = r.Header.Get("Idempotency-Key"), string(b)
			charges++
		} else if key != r.Header.Get("Idempotency-Key") || body != string(b) {
			t.Error("retry changed payment identity")
			charges++
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL, client: srv.Client()}
	j := job{ID: "order-1", Phase: "pay", Amount: 1200}
	// Simulate a crash after the bank responded but before progress was saved.
	for range 2 {
		_, status, _, err := a.process(context.Background(), j)
		if err != nil || status != "paid" {
			t.Fatalf("status=%s err=%v", status, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if key == "" || charges != 1 || requests != 2 {
		t.Fatalf("key=%q charges=%d requests=%d", key, charges, requests)
	}
}

func TestLostResponseIsReconciled(t *testing.T) {
	var charges atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			charges.Add(1)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		if r.URL.Query().Get("order_id") != "order-2" {
			t.Error("wrong lookup ID")
		}
		fmt.Fprint(w, `[{"order_id":"order-2","amount_cents":1200,"currency":"RUB","status":"captured"}]`)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL, client: srv.Client()}
	j := job{ID: "order-2", Phase: "pay", Amount: 1200}
	phase, status, _, err := a.process(context.Background(), j)
	if err == nil || phase != "verify" || status != "pending" {
		t.Fatalf("phase=%s status=%s err=%v", phase, status, err)
	}
	// A dropped reply from a reachable bank must not delay every other order.
	if _, wait := a.breaker.allow(time.Now()); wait != 0 {
		t.Fatal("lost reply opened the outage breaker")
	}
	j.Phase = phase
	_, status, _, err = a.process(context.Background(), j)
	if err != nil || status != "paid" || charges.Load() != 1 {
		t.Fatalf("status=%s charges=%d err=%v", status, charges.Load(), err)
	}
}

func TestDeclineWaitsForRelease(t *testing.T) {
	var releases atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(402)
			return
		}
		if releases.Add(1) == 1 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL, inventoryURL: srv.URL, client: srv.Client()}
	j := job{ID: "declined", Phase: "pay", Amount: 200}
	phase, status, _, err := a.process(context.Background(), j)
	if err != nil || phase != "release" || status != "pending" {
		t.Fatalf("%s %s %v", phase, status, err)
	}
	j.Phase = phase
	_, status, _, err = a.process(context.Background(), j)
	if err == nil || status != "pending" {
		t.Fatal("failed release must not finalize rejection")
	}
	_, status, _, err = a.process(context.Background(), j)
	if err != nil || status != "rejected" {
		t.Fatalf("status=%s err=%v", status, err)
	}
}

func TestPaymentTimeoutStaysPending(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL, client: &http.Client{Timeout: 20 * time.Millisecond}}
	phase, status, _, err := a.process(context.Background(), job{ID: "slow", Phase: "pay", Amount: 100})
	if err == nil || phase != "verify" || status != "pending" {
		t.Fatalf("%s %s %v", phase, status, err)
	}
}

func TestServerErrorAndEmptyLookupNeverReject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, `[]`)
	}))
	defer srv.Close()
	a := &app{paymentURL: srv.URL, client: srv.Client()}
	j := job{ID: "retry", Phase: "pay", Amount: 100}
	phase, status, _, err := a.process(context.Background(), j)
	if err == nil || phase != "pay" || status != "pending" {
		t.Fatalf("server error: %s %s %v", phase, status, err)
	}
	j.Phase = "verify"
	phase, status, _, err = a.process(context.Background(), j)
	if err != nil || phase != "pay" || status != "pending" {
		t.Fatalf("empty lookup: %s %s %v", phase, status, err)
	}
}

func TestBreakerBoundsOutageRequests(t *testing.T) {
	var b paymentBreaker
	start := time.Now()
	requests := 0
	// Many orders share the same breaker for an entire minute of outage.
	for elapsed := time.Duration(0); elapsed < time.Minute; elapsed += 10 * time.Millisecond {
		now := start.Add(elapsed)
		for range 50 {
			probe, wait := b.allow(now)
			if wait > 0 {
				continue
			}
			requests++
			b.finish(now, probe, true, false)
		}
	}
	if requests > 10 {
		t.Fatalf("outage requests=%d", requests)
	}
	probe, wait := b.allow(start.Add(2 * time.Minute))
	if !probe || wait != 0 {
		t.Fatal("recovery probe denied")
	}
	b.finish(start.Add(2*time.Minute), probe, false, false)
	if probe, wait = b.allow(start.Add(2 * time.Minute)); probe || wait != 0 {
		t.Fatal("did not recover")
	}
}

func TestBreakerAllowsOnlyOneRecoveryProbe(t *testing.T) {
	var b paymentBreaker
	now := time.Now()
	b.finish(now.Add(-time.Minute), false, true, false)
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for range 50 {
		wg.Go(func() {
			probe, wait := b.allow(now)
			if probe && wait == 0 {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if allowed != 1 {
		t.Fatalf("concurrent probes=%d", allowed)
	}
}

func TestOrderAmountRejectsOverflow(t *testing.T) {
	for _, items := range [][]Item{
		{{SKU: "a", Qty: 2, PriceCents: math.MaxInt64}},
		{{SKU: "a", Qty: 1, PriceCents: math.MaxInt64}, {SKU: "b", Qty: 1, PriceCents: 1}},
		{{SKU: "a", Qty: -1, PriceCents: 100}},
	} {
		if _, ok := orderAmount(OrderRequest{UserID: "user", Items: items}); ok {
			t.Fatal("accepted invalid amount")
		}
	}
	got, ok := orderAmount(OrderRequest{UserID: "user", Items: []Item{{SKU: "a", Qty: 2, PriceCents: 120}, {SKU: "b", Qty: 3, PriceCents: 10}}})
	if !ok || got != 270 {
		t.Fatalf("amount=%d valid=%v", got, ok)
	}
}
