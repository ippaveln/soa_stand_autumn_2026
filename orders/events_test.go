package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Each integration test uses an isolated schema in a disposable test database.
func eventTestApp(t *testing.T) *app {
	t.Helper()
	url := os.Getenv("ORDERS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set ORDERS_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	name := "test_" + strings.ReplaceAll(newID(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = name
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		_, err := admin.Exec(ctx, "DROP SCHEMA "+name+" CASCADE")
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	if _, err = db.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	return &app{db: db}
}

func seedPending(t *testing.T, a *app, id string) {
	t.Helper()
	_, err := a.db.Exec(context.Background(), `INSERT INTO orders (id,user_id,status,amount_cents,items)
	    VALUES ($1,'buyer','pending',100,'[]')`, id)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPaidAndOutboxCommitTogether(t *testing.T) {
	a := eventTestApp(t)
	ctx := context.Background()
	seedPending(t, a, "paid")
	seedPending(t, a, "declined")
	if err := a.saveProgress(ctx, "paid", "done", "paid", 0, 0); err != nil {
		t.Fatal(err)
	}
	// Repeating completion after an ambiguous DB acknowledgement adds no event.
	if err := a.saveProgress(ctx, "paid", "done", "paid", 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := a.saveProgress(ctx, "declined", "done", "rejected", 0, 0); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := a.db.QueryRow(ctx, `SELECT count(*) FROM paid_outbox`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("events=%d err=%v", count, err)
	}
	seedPending(t, a, "rollback")
	// Force a failure between the two writes; paid must not escape alone.
	if _, err := a.db.Exec(ctx, `ALTER TABLE paid_outbox ADD CHECK (order_id <> 'rollback')`); err != nil {
		t.Fatal(err)
	}
	if err := a.saveProgress(ctx, "rollback", "done", "paid", 0, 0); err == nil {
		t.Fatal("expected outbox failure")
	}
	var status string
	if err := a.db.QueryRow(ctx, `SELECT status FROM orders WHERE id='rollback'`).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("status=%s err=%v", status, err)
	}
}

func TestOutboxRetryAfterLostAcknowledgement(t *testing.T) {
	a := eventTestApp(t)
	ctx := context.Background()
	seedPending(t, a, "recovered")
	if err := a.saveProgress(ctx, "recovered", "done", "paid", 0, 0); err != nil {
		t.Fatal(err)
	}
	var events []paidEvent
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event paidEvent
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
		}
		mu.Lock()
		events = append(events, event)
		firstRequest := len(events) == 1
		mu.Unlock()
		if firstRequest {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close() // accepted by the bus, acknowledgement lost
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	a.eventsURL, a.client = srv.URL, srv.Client()
	first, _, err := a.claimEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.publish(ctx, first); err == nil {
		t.Fatal("expected lost response")
	}
	// Simulate a killed publisher: no progress saved, then its lease expires.
	if _, err = a.db.Exec(ctx, `UPDATE paid_outbox SET next_attempt_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	restarted := &app{db: a.db, eventsURL: srv.URL, client: srv.Client()}
	second, _, err := restarted.claimEvent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.publish(ctx, second); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0] != events[1] || events[0].ID == "" {
		t.Fatalf("event identity changed: %+v", events)
	}
}

func TestConcurrentDeliveryCopiesAndRestart(t *testing.T) {
	a := eventTestApp(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 20 {
		for _, event := range []deliveryEvent{
			{ID: "a", UserID: "buyer", Points: 10},
			{ID: "b", UserID: "buyer", Points: 20},
			{ID: "c", UserID: "other", Points: 40},
		} {
			wg.Go(func() {
				if err := a.creditDelivery(ctx, event); err != nil {
					t.Error(err)
				}
			})
		}
	}
	wg.Wait()
	// A new process has no memory of previously processed deliveries.
	restarted := &app{db: a.db}
	if err := restarted.creditDelivery(ctx, deliveryEvent{ID: "a", UserID: "buyer", Points: 10}); err != nil {
		t.Fatal(err)
	}
	for user, want := range map[string]int64{"buyer": 30, "other": 40, "unknown": 0} {
		resp := httptest.NewRecorder()
		restarted.routes().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/users/"+user+"/bonus", nil))
		var balance struct{ Points int64 }
		if err := json.Unmarshal(resp.Body.Bytes(), &balance); err != nil || resp.Code != 200 || balance.Points != want {
			t.Fatalf("%s: code=%d body=%s err=%v", user, resp.Code, resp.Body, err)
		}
	}
}

func TestFailedCreditRollsBackInbox(t *testing.T) {
	a := eventTestApp(t)
	ctx := context.Background()
	if _, err := a.db.Exec(ctx, `INSERT INTO bonus_balances VALUES ('full',$1)`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	event := deliveryEvent{ID: "retry", UserID: "full", Points: 1}
	if err := a.creditDelivery(ctx, event); err == nil {
		t.Fatal("expected overflow")
	}
	var count int
	if err := a.db.QueryRow(ctx, `SELECT count(*) FROM delivery_inbox`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed credit was deduplicated: count=%d err=%v", count, err)
	}
	if _, err := a.db.Exec(ctx, `UPDATE bonus_balances SET points=0`); err != nil {
		t.Fatal(err)
	}
	if err := a.creditDelivery(ctx, event); err != nil {
		t.Fatal(err)
	}
}

func TestDeliveryRejectsInvalidInput(t *testing.T) {
	a := &app{}
	for _, body := range []string{
		`{}`, `{"id":"a","type":"delivery.confirmed","user_id":"u","bonus_points":0}`,
		`{"id":"a","type":"other","user_id":"u","bonus_points":1}`,
		`{"id":"a","type":"delivery.confirmed","user_id":"u","bonus_points":1} {}`,
	} {
		resp := httptest.NewRecorder()
		a.delivery(resp, httptest.NewRequest(http.MethodPost, "/webhooks/delivery", strings.NewReader(body)))
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("body=%s code=%d", body, resp.Code)
		}
	}
}
