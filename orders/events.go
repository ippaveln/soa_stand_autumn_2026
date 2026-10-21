package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type paidEvent struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	OrderID    string    `json:"order_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (a *app) eventWorker(ctx context.Context) {
	for ctx.Err() == nil {
		queryCtx, cancel := context.WithTimeout(ctx, time.Second)
		event, attempts, err := a.claimEvent(queryCtx)
		cancel()
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
				slog.Error("claim event", "err", err)
			}
			if !pause(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		err = a.publish(ctx, event)
		sent, delay := err == nil, time.Duration(0)
		if err != nil {
			attempts = min(attempts+1, 16)
			delay = retryDelay(attempts)
			slog.Warn("event retry", "id", event.ID, "err", err)
		}
		updateCtx, finish := context.WithTimeout(ctx, time.Second)
		_, err = a.db.Exec(updateCtx, `UPDATE paid_outbox SET sent=$2,attempts=$3,
		    next_attempt_at=now()+($4 * interval '1 millisecond') WHERE event_id=$1`,
			event.ID, sent, attempts, delay.Milliseconds())
		finish()
		if err != nil && ctx.Err() == nil {
			// The bus may already have accepted it: retry the identical event ID.
			slog.Error("save event", "id", event.ID, "err", err)
		}
	}
}

func (a *app) claimEvent(ctx context.Context) (paidEvent, int, error) {
	event := paidEvent{Type: "order.paid"}
	var attempts int
	err := a.db.QueryRow(ctx, `WITH picked AS (
	    SELECT order_id FROM paid_outbox WHERE NOT sent AND next_attempt_at <= now()
	    ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1
	)
	UPDATE paid_outbox e SET next_attempt_at=now()+interval '15 seconds'
	FROM picked WHERE e.order_id=picked.order_id
	RETURNING e.event_id,e.order_id,e.occurred_at,e.attempts`).Scan(
		&event.ID, &event.OrderID, &event.OccurredAt, &attempts)
	return event, attempts, err
}

func (a *app) publish(ctx context.Context, event paidEvent) error {
	code, _, err := a.call(ctx, http.MethodPost, a.eventsURL+"/events", "", event)
	if err != nil {
		return err
	}
	if code != http.StatusAccepted {
		return fmt.Errorf("publish: HTTP %d", code)
	}
	return nil
}

type deliveryEvent struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	UserID string `json:"user_id"`
	Points int64  `json:"bonus_points"`
}

func (a *app) delivery(w http.ResponseWriter, r *http.Request) {
	var event deliveryEvent
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&event); err != nil || dec.Decode(new(any)) != io.EOF ||
		strings.TrimSpace(event.ID) == "" || event.Type != "delivery.confirmed" ||
		strings.TrimSpace(event.UserID) == "" || event.Points < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_event"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
	defer cancel()
	if err := a.creditDelivery(ctx, event); err != nil {
		slog.Error("credit delivery", "id", event.ID, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
		return
	}
	// Acknowledge only after commit: a lost acknowledgement is safe to retry.
	w.WriteHeader(http.StatusNoContent)
}

func (a *app) creditDelivery(ctx context.Context, event deliveryEvent) error {
	tx, err := a.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	inserted, err := tx.Exec(ctx, `INSERT INTO delivery_inbox (event_id,user_id,bonus_points)
	    VALUES ($1,$2,$3) ON CONFLICT (event_id) DO NOTHING`, event.ID, event.UserID, event.Points)
	if err != nil {
		return err
	}
	if inserted.RowsAffected() == 1 {
		// The unique inbox key serializes simultaneous copies. The balance
		// increment and deduplication record either both commit or both roll back.
		if _, err = tx.Exec(ctx, `INSERT INTO bonus_balances (user_id,points) VALUES ($1,$2)
		    ON CONFLICT (user_id) DO UPDATE SET points=bonus_balances.points+EXCLUDED.points`,
			event.UserID, event.Points); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (a *app) bonus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 700*time.Millisecond)
	defer cancel()
	var points int64
	err := a.db.QueryRow(ctx, `SELECT COALESCE((SELECT points FROM bonus_balances WHERE user_id=$1),0)`,
		r.PathValue("id")).Scan(&points)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}
