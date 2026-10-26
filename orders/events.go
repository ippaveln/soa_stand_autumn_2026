package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type deliveryEvent struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	UserID      string `json:"user_id"`
	BonusPoints int64  `json:"bonus_points"`
}

func (a *app) deliveryWebhook(w http.ResponseWriter, r *http.Request) {
	var event deliveryEvent
	if err := decodeJSON(w, r, &event); err != nil || event.ID == "" ||
		event.Type != "delivery.confirmed" || event.UserID == "" || event.BonusPoints <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	// Сама запись — начисление: уникальный id защищает и от параллельных копий.
	_, err := a.db.Exec(r.Context(), `
		INSERT INTO delivery_events (id, user_id, bonus_points) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING`, event.ID, event.UserID, event.BonusPoints)
	if err != nil {
		slog.Error("delivery", "id", event.ID, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *app) getBonus(w http.ResponseWriter, r *http.Request) {
	var points int64
	err := a.db.QueryRow(r.Context(), `
		SELECT COALESCE(SUM(bonus_points), 0)::bigint FROM delivery_events WHERE user_id = $1`,
		r.PathValue("id")).Scan(&points)
	if err != nil {
		slog.Error("bonus", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}

func (a *app) runOutbox(ctx context.Context) {
	if a.eventsURL == "" {
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := a.publishEvents(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("outbox", "err", err)
			// Не проходим всю очередь запросами к недоступной шине.
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *app) publishEvents(ctx context.Context) error {
	// Статус paid сам хранит обязанность отправить событие, без второй записи.
	rows, err := a.db.Query(ctx, `
		SELECT id FROM orders WHERE status = 'paid' AND NOT event_sent ORDER BY created_at, id LIMIT 100`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		code, err := a.call(ctx, http.MethodPost, a.eventsURL+"/events", map[string]string{
			"id":       "order.paid:" + id,
			"type":     "order.paid",
			"order_id": id,
		}, "", nil)
		if err != nil {
			return err
		}
		if code != http.StatusAccepted {
			return fmt.Errorf("events: %d", code)
		}
		// При падении до UPDATE отправим ту же копию с прежним id.
		if _, err := a.db.Exec(ctx, `UPDATE orders SET event_sent = true WHERE id = $1`, id); err != nil {
			return err
		}
	}
	return nil
}
