package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestDeliveryCopiesCountOnce(t *testing.T) {
	db := testDatabase(t)
	a := &app{db: db}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			a.routes().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/webhooks/delivery", strings.NewReader(fmt.Sprintf(
				`{"id":"delivery-1","type":"delivery.confirmed","user_id":"u-1","bonus_points":40,"occurred_at":"copy-%d"}`, i))))
			if w.Code != http.StatusNoContent {
				t.Errorf("delivery status = %d: %s", w.Code, w.Body)
			}
		}()
	}
	wg.Wait()
	// Повтор после перезапуска и новое событие для того же покупателя.
	a = &app{db: db}
	for _, id := range []string{"delivery-1", "delivery-2"} {
		w := httptest.NewRecorder()
		a.routes().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/webhooks/delivery", strings.NewReader(fmt.Sprintf(
			`{"id":%q,"type":"delivery.confirmed","user_id":"u-1","bonus_points":40}`, id))))
		if w.Code != http.StatusNoContent {
			t.Fatalf("replay status = %d: %s", w.Code, w.Body)
		}
	}
	for user, want := range map[string]int64{"u-1": 80, "u-0": 0} {
		w := httptest.NewRecorder()
		a.routes().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/users/"+user+"/bonus", nil))
		var balance struct{ Points int64 }
		if err := json.Unmarshal(w.Body.Bytes(), &balance); err != nil || w.Code != http.StatusOK || balance.Points != want {
			t.Errorf("%s balance = %s, want %d, err = %v", user, w.Body, want, err)
		}
	}
}

func TestInvalidDelivery(t *testing.T) {
	a := &app{}
	for _, body := range []string{
		`{}`, `{"id":"d","type":"other","user_id":"u","bonus_points":1}`,
		`{"id":"d","type":"delivery.confirmed","user_id":"u","bonus_points":0}`,
		`{"id":"d","type":"delivery.confirmed","user_id":"u","bonus_points":1} {}`,
	} {
		w := httptest.NewRecorder()
		a.deliveryWebhook(w, httptest.NewRequest(http.MethodPost, "/webhooks/delivery", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest {
			t.Errorf("body = %s, status = %d", body, w.Code)
		}
	}
}

func TestOutboxReplaysSameEventAfterLostReply(t *testing.T) {
	db := testDatabase(t)
	seedOrder(t, db, "paid-1", "paid", "pay")
	seedOrder(t, db, "pending-1", "pending", "pay")
	seedOrder(t, db, "rejected-1", "rejected", "release")
	var mu sync.Mutex
	var events []map[string]string
	bus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event map[string]string
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		events = append(events, event)
		first := len(events) == 1
		mu.Unlock()
		if first {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer bus.Close()
	a := &app{db: db, eventsURL: bus.URL, client: bus.Client()}
	if err := a.publishEvents(context.Background()); err == nil {
		t.Fatal("lost event response must be retried")
	}
	// Новый процесс восстанавливает outbox из БД, без памяти предыдущего.
	a = &app{db: db, eventsURL: bus.URL, client: bus.Client()}
	if err := a.publishEvents(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.publishEvents(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0]["id"] == "" || events[0]["id"] != events[1]["id"] {
		t.Fatalf("events = %+v", events)
	}
	for _, event := range events {
		if event["order_id"] != "paid-1" || event["type"] != "order.paid" {
			t.Fatalf("published event for an unpaid order: %+v", event)
		}
	}
	var sent bool
	if err := db.QueryRow(context.Background(), `SELECT event_sent FROM orders WHERE id = 'paid-1'`).Scan(&sent); err != nil || !sent {
		t.Fatalf("event_sent = %v, err = %v", sent, err)
	}
}
