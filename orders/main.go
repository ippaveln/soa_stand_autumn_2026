package main

import (
	"bytes"
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
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT 'reserve';
ALTER TABLE orders ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE orders ADD COLUMN IF NOT EXISTS event_sent BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX IF NOT EXISTS orders_pending ON orders (next_attempt_at, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS orders_outbox ON orders (created_at) WHERE status = 'paid' AND NOT event_sent;
CREATE TABLE IF NOT EXISTS delivery_events (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    bonus_points BIGINT NOT NULL CHECK (bonus_points > 0)
);
CREATE INDEX IF NOT EXISTS delivery_events_user ON delivery_events (user_id);
`

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

func (item *Item) UnmarshalJSON(data []byte) error {
	type plain Item
	// Нулевая цена допустима, отсутствующая или null — нет.
	value := plain{PriceCents: -1}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*item = Item(value)
	return nil
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
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	a := &app{
		paymentURL:   strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		inventoryURL: strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		eventsURL:    strings.TrimRight(mustEnv("EVENTS_URL"), "/"),
		client:       &http.Client{Timeout: 400 * time.Millisecond},
	}
	db, err := connect(mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("база", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	a.db = db
	go a.runOrders(ctx)
	go a.runOutbox(ctx)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{Addr: ":" + port, Handler: a.routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			slog.Error("остановка сервера", "err", err)
		}
	}()
	slog.Info("orders: слушаю", "port", port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
		defer cancel()
		if err := a.db.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /webhooks/delivery", a.deliveryWebhook)
	mux.HandleFunc("GET /users/{id}/bonus", a.getBonus)
	return mux
}

// connect ждёт базу: она может подниматься дольше сервиса.
func connect(url string) (*pgxpool.Pool, error) {
	var lastErr error
	for range 60 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		db, err := pgxpool.New(ctx, url)
		if err == nil {
			_, err = db.Exec(ctx, schema)
		}
		cancel()
		if err == nil {
			return db, nil
		}
		if db != nil {
			db.Close()
		}
		lastErr = err
		slog.Info("база недоступна, жду", "err", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
	defer cancel()
	var req OrderRequest
	if err := decodeJSON(w, r, &req); err != nil || !valid(req) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	id := newID()
	var amount int64
	for _, it := range req.Items {
		amount += int64(it.Qty) * it.PriceCents
	}
	items, err := json.Marshal(req.Items)
	if err != nil {
		a.fail(w, id, err)
		return
	}
	_, err = a.db.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES ($1, $2, 'pending', $3, $4)`,
		id, req.UserID, amount, items)
	if err != nil {
		a.fail(w, id, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "status": "pending"})
}

func (a *app) fail(w http.ResponseWriter, id string, err error) {
	slog.Error("заказ", "id", id, "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
}

func (a *app) getOrder(w http.ResponseWriter, r *http.Request) {
	var (
		id, status string
		amount     int64
	)
	err := a.db.QueryRow(r.Context(), `SELECT id, status, amount_cents FROM orders WHERE id = $1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		a.fail(w, r.PathValue("id"), err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

func (a *app) call(ctx context.Context, method, url string, body any, key string, result any) (int, error) {
	return callWithClient(ctx, a.client, method, url, body, key, result)
}

func callWithClient(ctx context.Context, client *http.Client, method, url string, body any, key string, result any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(result)
	} else {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	}
	return resp.StatusCode, err
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("ожидался один JSON документ")
	}
	return nil
}

func valid(r OrderRequest) bool {
	if r.UserID == "" || len(r.Items) == 0 {
		return false
	}
	var amount int64
	for _, it := range r.Items {
		if it.SKU == "" || it.Qty < 1 || it.PriceCents < 0 || it.PriceCents > (math.MaxInt64-amount)/int64(it.Qty) {
			return false
		}
		amount += int64(it.Qty) * it.PriceCents
	}
	return true
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("ответ клиенту", "err", err)
	}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("не задана переменная окружения", "name", k)
		os.Exit(1)
	}
	return v
}
