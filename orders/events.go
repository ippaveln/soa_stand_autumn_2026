package main

// Задание 2: исходящие события (outbox → шина) и входящие delivery.confirmed.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const (
	publishTimeout  = 2 * time.Second
	publishTick     = 250 * time.Millisecond
	publishBatch    = 200
	publishRetryMax = 5 * time.Second
)

func (a *app) wakePublisher() {
	select {
	case a.kick <- struct{}{}:
	default:
	}
}

// publisher отправляет в шину события из outbox, пока шина их не примет.
// Событие отмечается опубликованным только после 202. Если Orders упадёт
// между 202 и отметкой, событие уйдёт ещё раз с тем же id — подписчики
// отличат копию. Получается доставка «как минимум один раз».
func (a *app) publisher() {
	t := time.NewTicker(publishTick)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-a.kick:
		}
		for _, ev := range a.dueEvents() {
			if _, loaded := a.pubBusy.LoadOrStore(ev.id, struct{}{}); loaded {
				continue
			}
			go func() {
				a.pubSem <- struct{}{}
				defer func() { <-a.pubSem }()
				defer a.pubBusy.Delete(ev.id)
				a.publish(ev)
			}()
		}
	}
}

type outboxEvent struct {
	id       string
	payload  []byte
	attempts int
}

func (a *app) dueEvents() []outboxEvent {
	ctx, cancel := dbCtx()
	defer cancel()
	rows, err := a.db.Query(ctx,
		`SELECT id, payload, attempts FROM outbox
		 WHERE published_at IS NULL AND next_attempt_at <= now()
		 ORDER BY next_attempt_at LIMIT $1`, publishBatch)
	if err != nil {
		slog.Error("outbox: выборка", "err", err)
		return nil
	}
	defer rows.Close()
	var out []outboxEvent
	for rows.Next() {
		var ev outboxEvent
		if err := rows.Scan(&ev.id, &ev.payload, &ev.attempts); err != nil {
			slog.Error("outbox: чтение", "err", err)
			return out
		}
		out = append(out, ev)
	}
	return out
}

func (a *app) publish(ev outboxEvent) {
	var body json.RawMessage = ev.payload
	code, err := a.call(context.Background(), publishTimeout, http.MethodPost, a.eventsURL+"/events", body, nil)
	ctx, cancel := dbCtx()
	defer cancel()
	if err == nil && code == http.StatusAccepted {
		if _, err := a.db.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, ev.id); err != nil {
			slog.Error("outbox: отметка", "id", ev.id, "err", err)
		}
		return
	}
	if err == nil {
		err = fmt.Errorf("шина: %d", code)
	}
	delay := min(backoff(ev.attempts), publishRetryMax)
	slog.Warn("событие не опубликовано, повторю", "id", ev.id, "err", err, "через", delay)
	if _, err := a.db.Exec(ctx,
		`UPDATE outbox SET attempts = attempts + 1, next_attempt_at = now() + $2::bigint * interval '1 millisecond' WHERE id = $1`,
		ev.id, delay.Milliseconds()); err != nil {
		slog.Error("outbox: планирование повтора", "id", ev.id, "err", err)
	}
}

// deliveryConfirmed — POST /webhooks/delivery: начислить бонусы за доставку.
//
// Начисление — строка bonus_events с первичным ключом по id события. Копия
// (в том числе одновременная) упирается в ключ и ничего не добавляет: вторая
// вставка ждёт, пока первая транзакция завершится, и дальше ничего не
// делает. Вставка — одна операция: если Orders упадёт до коммита, начисления
// нет, и шина пришлёт событие снова.
func (a *app) deliveryConfirmed(w http.ResponseWriter, r *http.Request) {
	var ev struct {
		ID          string `json:"id"`
		UserID      string `json:"user_id"`
		BonusPoints int64  `json:"bonus_points"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&ev); err != nil ||
		ev.ID == "" || ev.UserID == "" || ev.BonusPoints <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := a.db.Exec(ctx,
		`INSERT INTO bonus_events (event_id, user_id, points) VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING`,
		ev.ID, ev.UserID, ev.BonusPoints)
	if err != nil {
		// Не подтверждаем: шина повторит доставку.
		slog.Error("бонусы", "event", ev.ID, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// getBonus — баланс: сумма начислений по разным событиям покупателя.
func (a *app) getBonus(w http.ResponseWriter, r *http.Request) {
	var points int64
	err := a.db.QueryRow(r.Context(),
		`SELECT COALESCE(SUM(points), 0)::bigint FROM bonus_events WHERE user_id = $1`,
		r.PathValue("id")).Scan(&points)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}
