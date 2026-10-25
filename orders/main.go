// Orders — сервис оформления заказов.
//
// Задание 1: деньги не списываются дважды. Заказ сначала записывается в базу
// (pending), затем Orders резервирует товар и списывает деньги с
// Idempotency-Key = id заказа: повтор запроса к Payment не создаёт второго
// списания. Если исход не выяснился за бюджет ответа, покупатель получает
// pending, а фоновый воркер доводит заказ до paid или rejected. Пока Payment
// недоступен, circuit breaker не пускает к нему запросы (кроме редких проб).
//
// Задание 2: заказ не теряется. Событие order.paid пишется в outbox в той же
// транзакции, что и статус paid; отдельный публикатор отправляет его в шину,
// пока шина не примет (id события хранится в базе, у копий он один).
// Входящие delivery.confirmed дедуплицируются по id события в базе.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,              -- pending | paid | rejected
    stage           TEXT NOT NULL,              -- new | reserved | declined | done
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_pending ON orders (next_attempt_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,           -- id события order.paid, один у всех копий
    order_id        TEXT NOT NULL UNIQUE,
    payload         JSONB NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (next_attempt_at) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS bonus_events (
    event_id     TEXT PRIMARY KEY,              -- id delivery.confirmed: начисление ровно одно
    user_id      TEXT NOT NULL,
    points       BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bonus_events_user ON bonus_events (user_id);
`

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type OrderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

func main() {
	a := newApp(
		strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		strings.TrimRight(os.Getenv("EVENTS_URL"), "/"),
	)
	db, err := connect(mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("база", "err", err)
		os.Exit(1)
	}
	a.db = db

	// После перезапуска фоновые циклы подхватывают из базы всё незавершённое:
	// pending-заказы и неопубликованные события.
	go a.orderWorker()
	if a.eventsURL != "" {
		go a.publisher()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.createOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("POST /webhooks/delivery", a.deliveryConfirmed)
	mux.HandleFunc("GET /users/{id}/bonus", a.getBonus)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	slog.Info("orders: слушаю", "port", port)
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

// connect ждёт базу: она может подниматься дольше сервиса.
func connect(url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 32
	var lastErr error
	for range 60 {
		db, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err == nil {
			if _, err = db.Exec(context.Background(), schema); err == nil {
				return db, nil
			}
			db.Close()
		}
		lastErr = err
		slog.Info("база недоступна, жду", "err", err)
		time.Sleep(time.Second)
	}
	return nil, lastErr
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || !valid(req) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	o := &order{ID: newID(), Items: req.Items, Stage: stageNew}
	for _, it := range req.Items {
		o.Amount += int64(it.Qty) * it.PriceCents
	}

	// Сначала заказ в базе, потом внешние вызовы: если Orders упадёт посреди
	// оплаты, воркер после перезапуска найдёт заказ и доведёт его.
	// next_attempt_at в будущем — воркер не возьмёт заказ, пока с ним
	// работает этот обработчик.
	items, _ := json.Marshal(req.Items)
	ctx, cancel := dbCtx()
	_, err := a.db.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, stage, amount_cents, items, next_attempt_at)
		 VALUES ($1, $2, 'pending', $3, $4, $5, now() + interval '5 seconds')`,
		o.ID, req.UserID, stageNew, o.Amount, items)
	cancel()
	if err != nil {
		slog.Error("POST /orders: запись заказа", "id", o.ID, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable"})
		return
	}

	status := statusPending
	if a.lock(o.ID) {
		ctx, cancel := context.WithTimeout(context.Background(), inlineBudget)
		status = a.advance(ctx, o, false)
		cancel()
		a.unlock(o.ID)
	}
	slog.Info("order", "id", o.ID, "status", status)
	writeJSON(w, http.StatusCreated, map[string]string{"id": o.ID, "status": status})
}

func (a *app) getOrder(w http.ResponseWriter, r *http.Request) {
	var (
		id, status string
		amount     int64
	)
	err := a.db.QueryRow(r.Context(), `SELECT id, status, amount_cents FROM orders WHERE id = $1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

func valid(r OrderRequest) bool {
	if r.UserID == "" || len(r.Items) == 0 {
		return false
	}
	for _, it := range r.Items {
		if it.SKU == "" || it.Qty < 1 || it.PriceCents < 0 {
			return false
		}
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

// dbCtx — контекст для запроса в базу, не зависящий от бюджета внешних вызовов.
func dbCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("не задана переменная окружения", "name", k)
		os.Exit(1)
	}
	return v
}
