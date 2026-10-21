// Orders persists accepted orders before making any external calls.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'rejected')),
    amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
    items JSONB NOT NULL,
    phase TEXT NOT NULL DEFAULT 'reserve',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_pending_due
    ON orders (next_attempt_at, created_at) WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS paid_outbox (
    order_id TEXT PRIMARY KEY REFERENCES orders(id),
    event_id TEXT NOT NULL UNIQUE,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent BOOLEAN NOT NULL DEFAULT false,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS paid_outbox_due
    ON paid_outbox (next_attempt_at) WHERE NOT sent;
CREATE TABLE IF NOT EXISTS delivery_inbox (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    bonus_points BIGINT NOT NULL CHECK (bonus_points > 0)
);
CREATE TABLE IF NOT EXISTS bonus_balances (
    user_id TEXT PRIMARY KEY,
    points BIGINT NOT NULL CHECK (points >= 0)
);
`

type Item struct {
	SKU        string `json:"sku"`
	Qty        int64  `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type OrderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

type app struct {
	db           *pgxpool.Pool
	paymentURL   string
	inventoryURL string
	eventsURL    string
	client       *http.Client
	breaker      paymentBreaker
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := connect(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("database", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	a := &app{
		db:           db,
		paymentURL:   strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		inventoryURL: strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		eventsURL:    strings.TrimRight(mustEnv("EVENTS_URL"), "/"),
		client:       &http.Client{Timeout: 4 * time.Second},
	}
	for range 8 {
		go a.worker(ctx)
	}
	for range 4 {
		go a.eventWorker(ctx)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port, Handler: a.routes(), ReadHeaderTimeout: 2 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	slog.Info("orders listening", "port", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	// Workers never hold a database connection during network calls.
	config.MaxConns = 16
	config.ConnConfig.ConnectTimeout = 2 * time.Second
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	for range 60 {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err = db.Exec(attempt, schema)
		cancel()
		if err == nil {
			return db, nil
		}
		if !pause(ctx, time.Second) {
			break
		}
	}
	db.Close()
	return nil, err
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := a.db.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /webhooks/delivery", a.delivery)
	mux.HandleFunc("GET /users/{id}/bonus", a.bonus)
	return mux
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	amount, valid := orderAmount(req)
	// A second JSON value or trailing garbage is not a single order.
	if !valid || dec.Decode(new(any)) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	id := newID()
	items, _ := json.Marshal(req.Items)
	ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
	defer cancel()
	_, err := a.db.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES ($1,$2,'pending',$3,$4)`,
		id, req.UserID, amount, items)
	if err != nil {
		// A database outage cannot be acknowledged as a durable acceptance.
		// The stand keeps Postgres available; neighbour failures never reach here.
		slog.Error("accept order", "id", id, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": "pending"})
}

func (a *app) getOrder(w http.ResponseWriter, r *http.Request) {
	var id, status string
	var amount int64
	ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
	defer cancel()
	err := a.db.QueryRow(ctx, `SELECT id, status, amount_cents FROM orders WHERE id=$1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

func orderAmount(r OrderRequest) (int64, bool) {
	if strings.TrimSpace(r.UserID) == "" || len(r.Items) == 0 {
		return 0, false
	}
	var amount int64
	for _, it := range r.Items {
		if strings.TrimSpace(it.SKU) == "" || it.Qty < 1 || it.PriceCents < 0 {
			return 0, false
		}
		if it.PriceCents > (math.MaxInt64-amount)/it.Qty {
			return 0, false
		}
		amount += it.Qty * it.PriceCents
	}
	return amount, true
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("missing environment variable", "name", key)
		os.Exit(1)
	}
	return v
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
