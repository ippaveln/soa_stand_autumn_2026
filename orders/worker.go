package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
)

type job struct {
	ID       string
	Phase    string
	Amount   int64
	Items    []byte
	Attempts int
}

// Claim commits before any HTTP call. A lease recovers work after a crash.
// Its duration exceeds the entire bounded processing step (HTTP + DB update).
func (a *app) claim(ctx context.Context) (job, error) {
	var j job
	err := a.db.QueryRow(ctx, `
		WITH picked AS (
		    SELECT id FROM orders
		    WHERE status='pending' AND next_attempt_at <= now()
		    ORDER BY next_attempt_at, created_at
		    FOR UPDATE SKIP LOCKED LIMIT 1
		)
		UPDATE orders o SET next_attempt_at=now()+interval '15 seconds'
		FROM picked WHERE o.id=picked.id
		RETURNING o.id,o.phase,o.amount_cents,o.items,o.attempts`).Scan(
		&j.ID, &j.Phase, &j.Amount, &j.Items, &j.Attempts)
	return j, err
}

func (a *app) worker(ctx context.Context) {
	for ctx.Err() == nil {
		queryCtx, cancel := context.WithTimeout(ctx, time.Second)
		j, err := a.claim(queryCtx)
		cancel()
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
				slog.Error("claim", "err", err)
			}
			if !pause(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		phase, status, delay, err := a.process(ctx, j)
		attempts := 0
		if err != nil {
			attempts = min(j.Attempts+1, 16)
			delay = retryDelay(attempts)
			slog.Warn("order retry", "id", j.ID, "phase", phase, "err", err)
		}
		updateCtx, finish := context.WithTimeout(ctx, time.Second)
		updateErr := a.saveProgress(updateCtx, j.ID, phase, status, attempts, delay)
		finish()
		if updateErr != nil && ctx.Err() == nil {
			// Leave the lease: retrying the old phase is safe, including payment.
			slog.Error("save progress", "id", j.ID, "err", updateErr)
		}
	}
}

// Committing paid and its outgoing event together closes the crash window
// between recording a successful payment and scheduling its notification.
func (a *app) saveProgress(ctx context.Context, id, phase, status string, attempts int, delay time.Duration) error {
	tx, err := a.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE orders SET phase=$2,status=$3,attempts=$4,
	    next_attempt_at=now()+($5 * interval '1 millisecond') WHERE id=$1`,
		id, phase, status, attempts, delay.Milliseconds()); err != nil {
		return err
	}
	if status == "paid" {
		if _, err = tx.Exec(ctx, `INSERT INTO paid_outbox (order_id,event_id)
		    SELECT id,'order.paid:' || id FROM orders WHERE id=$1 AND status='paid'
		    ON CONFLICT (order_id) DO NOTHING`, id); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func retryDelay(attempt int) time.Duration {
	base := min(200*time.Millisecond*time.Duration(1<<min(attempt-1, 5)), 5*time.Second)
	return base + time.Duration(rand.Int64N(int64(100*time.Millisecond)))
}

func (a *app) process(ctx context.Context, j job) (phase, status string, delay time.Duration, err error) {
	phase, status = j.Phase, "pending"
	switch j.Phase {
	case "reserve":
		var items []Item
		if err = json.Unmarshal(j.Items, &items); err != nil {
			return
		}
		type reservationItem struct {
			SKU string `json:"sku"`
			Qty int64  `json:"qty"`
		}
		reservation := make([]reservationItem, len(items))
		for i, it := range items {
			reservation[i] = reservationItem{it.SKU, it.Qty}
		}
		var code int
		code, _, err = a.call(ctx, http.MethodPost, a.inventoryURL+"/reservations", "",
			map[string]any{"order_id": j.ID, "items": reservation})
		if err == nil && code != 200 && code != 201 {
			err = fmt.Errorf("reserve: HTTP %d", code)
		}
		if err == nil {
			phase = "pay"
			// Payment accepts strictly positive charges; a free order needs none.
			if j.Amount == 0 {
				phase, status = "done", "paid"
			}
		}
	case "pay", "verify":
		probe, wait := a.breaker.allow(time.Now())
		if wait > 0 {
			delay = wait + 50*time.Millisecond
			return
		}
		var code int
		var body []byte
		if j.Phase == "pay" {
			// The key and payment body are stable across retries and restarts.
			code, body, err = a.call(ctx, http.MethodPost, a.paymentURL+"/payments", "payment:"+j.ID,
				map[string]any{"order_id": j.ID, "amount_cents": j.Amount, "currency": "RUB"})
		} else {
			code, body, err = a.call(ctx, http.MethodGet, a.paymentURL+"/payments?order_id="+url.QueryEscape(j.ID), "", nil)
		}
		// EOF means the peer closed an established connection: reconcile the
		// uncertain payment promptly. A lost reply alone is not an outage.
		// Timeouts/connect failures do open the shared circuit.
		lostReply := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		a.breaker.finish(time.Now(), probe, err != nil && !lostReply, code >= 500 || code == 429)
		if err != nil {
			// Transport errors are ambiguous, never a reason to reject an order.
			phase = "verify"
			return
		}
		if j.Phase == "pay" && code == http.StatusPaymentRequired {
			phase = "release"
			return
		}
		if j.Phase == "pay" && (code == 200 || code == 201) {
			phase, status = "done", "paid"
			return
		}
		if j.Phase == "verify" && code == 200 {
			var charges []struct {
				OrderID  string `json:"order_id"`
				Amount   int64  `json:"amount_cents"`
				Currency string `json:"currency"`
				Status   string `json:"status"`
			}
			if err = json.Unmarshal(body, &charges); err != nil {
				return
			}
			if len(charges) == 0 {
				phase = "pay"
				return
			}
			if len(charges) == 1 && charges[0].OrderID == j.ID && charges[0].Amount == j.Amount &&
				charges[0].Currency == "RUB" && charges[0].Status == "captured" {
				phase, status = "done", "paid"
				return
			}
			err = fmt.Errorf("unexpected payment facts for %s", j.ID)
			return
		}
		err = fmt.Errorf("payment %s: HTTP %d", j.Phase, code)
	case "release":
		var code int
		code, _, err = a.call(ctx, http.MethodDelete, a.inventoryURL+"/reservations/"+url.PathEscape(j.ID), "", nil)
		if err == nil && code != 204 {
			err = fmt.Errorf("release: HTTP %d", code)
		}
		if err == nil {
			phase, status = "done", "rejected"
		}
	default:
		err = fmt.Errorf("unknown phase %q", j.Phase)
	}
	return
}

func (a *app) call(ctx context.Context, method, target, key string, value any) (int, []byte, error) {
	var reader io.Reader
	if value != nil {
		body, err := json.Marshal(value)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, err
}
