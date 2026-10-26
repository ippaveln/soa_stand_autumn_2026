package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "test_" + strings.ReplaceAll(newID(), "-", "")
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = name
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if _, err := db.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedOrder(t *testing.T, db *pgxpool.Pool, id, status, stage string) {
	t.Helper()
	_, err := db.Exec(context.Background(), `
		INSERT INTO orders (id, user_id, status, stage, amount_cents, items)
		VALUES ($1, 'u-1', $2, $3, 100, '[{"sku":"sku-1","qty":1,"price_cents":100}]')`, id, status, stage)
	if err != nil {
		t.Fatal(err)
	}
}

func TestInvalidOrder(t *testing.T) {
	a := &app{}
	for _, body := range []string{
		`{`, `{}`, `{"user_id":"u","items":[]}`,
		`{"user_id":"u","items":[{"sku":"s","qty":0,"price_cents":1}]}`,
		`{"user_id":"u","items":[{"sku":"s","qty":1}]}`,
		`{"user_id":"u","items":[{"sku":"s","qty":1,"price_cents":null}]}`,
		`{"user_id":"u","items":[{"sku":"s","qty":1,"price_cents":-1}]}`,
		`{"user_id":"u","items":[{"sku":"s","qty":2,"price_cents":9223372036854775807}]}`,
		`{"user_id":"u","items":[{"sku":"s","qty":1,"price_cents":1}]} {}`,
	} {
		t.Run(body, func(t *testing.T) {
			w := httptest.NewRecorder()
			a.createOrder(w, httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body)))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body)
			}
		})
	}
	if valid(OrderRequest{UserID: "u", Items: []Item{{SKU: "s", Qty: 1, PriceCents: math.MaxInt64}, {SKU: "s", Qty: 1, PriceCents: 1}}}) {
		t.Fatal("accepted overflowing total")
	}
	if !valid(OrderRequest{UserID: "u", Items: []Item{{SKU: "s", Qty: 1, PriceCents: math.MaxInt64}}}) {
		t.Fatal("rejected valid int64 total")
	}
}

func TestOrderIsDurableBeforeNeighbors(t *testing.T) {
	db := testDatabase(t)
	var calls atomic.Int32
	neighbor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Idempotency-Key") == "" && r.URL.Path == "/payments" {
			t.Error("missing payment idempotency key")
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer neighbor.Close()
	a := &app{db: db, paymentURL: neighbor.URL, inventoryURL: neighbor.URL, client: neighbor.Client()}
	w := httptest.NewRecorder()
	start := time.Now()
	a.routes().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(
		`{"user_id":"u","items":[{"sku":"s","qty":3,"price_cents":350}]}`)))
	if w.Code != http.StatusCreated || time.Since(start) >= time.Second {
		t.Fatalf("create status = %d, elapsed = %s, body = %s", w.Code, time.Since(start), w.Body)
	}
	var created struct{ ID, Status string }
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	orders, err := a.pendingOrders(context.Background())
	if err != nil || len(orders) != 1 || orders[0].ID != created.ID || orders[0].Amount != 1050 || created.Status != "pending" || calls.Load() != 0 {
		t.Fatalf("pending = %+v, response = %+v, calls = %d, err = %v", orders, created, calls.Load(), err)
	}
	if err := a.processOrder(context.Background(), orders[0], &paymentBreaker{}); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	a.routes().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/orders/"+created.ID, nil))
	var result struct {
		Status string `json:"status"`
		Amount int64  `json:"amount_cents"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != http.StatusOK || result.Status != "paid" || result.Amount != 1050 {
		t.Fatalf("order = %s, status = %d, err = %v", w.Body, w.Code, err)
	}
}

func TestRestartResumesPaymentAndRelease(t *testing.T) {
	for _, stage := range []string{"pay", "release"} {
		t.Run(stage, func(t *testing.T) {
			db := testDatabase(t)
			seedOrder(t, db, "order-1", "pending", stage)
			var calls atomic.Int32
			neighbor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if stage == "pay" && r.Method == http.MethodPost && r.URL.Path == "/payments" && r.Header.Get("Idempotency-Key") == "order-1" {
					// Банк уже списал деньги до падения; повтор возвращает прежний результат.
					w.WriteHeader(http.StatusOK)
					return
				}
				if stage == "release" && r.Method == http.MethodDelete && r.URL.Path == "/reservations/order-1" {
					if calls.Load() == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
					} else {
						w.WriteHeader(http.StatusNoContent)
					}
					return
				}
				t.Errorf("unexpected neighbor request: %s %s", r.Method, r.URL)
				w.WriteHeader(http.StatusBadRequest)
			}))
			defer neighbor.Close()
			attempt := func() error {
				// Новый app не содержит состояния предыдущего процесса.
				a := &app{db: db, paymentURL: neighbor.URL, inventoryURL: neighbor.URL, client: neighbor.Client()}
				orders, err := a.pendingOrders(context.Background())
				if err != nil || len(orders) != 1 {
					t.Fatalf("pending = %+v, err = %v", orders, err)
				}
				return a.processOrder(context.Background(), orders[0], &paymentBreaker{})
			}
			if stage == "release" && attempt() == nil {
				t.Fatal("release failure was ignored")
			}
			if err := attempt(); err != nil {
				t.Fatal(err)
			}
			var status string
			if err := db.QueryRow(context.Background(), `SELECT status FROM orders WHERE id = 'order-1'`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := "paid"
			if stage == "release" {
				want = "rejected"
			}
			if status != want {
				t.Fatalf("status = %s, want %s", status, want)
			}
		})
	}
}
