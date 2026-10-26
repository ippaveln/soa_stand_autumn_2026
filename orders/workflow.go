package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

type pendingOrder struct {
	ID     string
	Stage  string
	Amount int64
	Items  []Item
}

// Only the order worker uses the breaker, so no synchronization is needed.
type paymentBreaker struct {
	failures     int
	retryAt      time.Time
	responseWait time.Duration
}

func (a *app) runOrders(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var breaker paymentBreaker
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if time.Now().Before(breaker.retryAt) {
			continue
		}
		orders, err := a.pendingOrders(ctx)
		if err != nil {
			slog.Error("очередь заказов", "err", err)
			continue
		}
		for _, order := range orders {
			if ctx.Err() != nil {
				return
			}
			attemptTimeout := 7 * time.Second
			if breaker.responseWait > 0 {
				attemptTimeout = breaker.responseWait + 2*time.Second
			}
			attempt, cancel := context.WithTimeout(ctx, attemptTimeout)
			err := a.processOrder(attempt, order, &breaker)
			cancel()
			if err != nil {
				slog.Warn("повтор заказа", "id", order.ID, "err", err)
				if _, retryErr := a.db.Exec(ctx, `UPDATE orders SET next_attempt_at = now() + interval '1 second' WHERE id = $1 AND status = 'pending'`, order.ID); retryErr != nil {
					slog.Error("отложить заказ", "id", order.ID, "err", retryErr)
				}
			}
			if time.Now().Before(breaker.retryAt) {
				break
			}
		}
	}
}

func (a *app) pendingOrders(ctx context.Context) ([]pendingOrder, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	rows, err := a.db.Query(ctx, `SELECT id, stage, amount_cents, items FROM orders WHERE status = 'pending' AND next_attempt_at <= now() ORDER BY next_attempt_at, created_at LIMIT 64`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orders []pendingOrder
	for rows.Next() {
		var order pendingOrder
		var items []byte
		if err := rows.Scan(&order.ID, &order.Stage, &order.Amount, &items); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(items, &order.Items); err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	return orders, rows.Err()
}

func (a *app) processOrder(ctx context.Context, order pendingOrder, breaker *paymentBreaker) error {
	if order.Stage == "reserve" {
		type reservationItem struct {
			SKU string `json:"sku"`
			Qty int    `json:"qty"`
		}
		items := make([]reservationItem, 0, len(order.Items))
		for _, item := range order.Items {
			items = append(items, reservationItem{SKU: item.SKU, Qty: item.Qty})
		}
		code, err := a.call(ctx, http.MethodPost, a.inventoryURL+"/reservations", map[string]any{"order_id": order.ID, "items": items}, "", nil)
		if err != nil {
			return err
		}
		if code != http.StatusOK && code != http.StatusCreated {
			return fmt.Errorf("inventory reserve: %d", code)
		}
		if _, err := a.db.Exec(ctx, `UPDATE orders SET stage = 'pay' WHERE id = $1`, order.ID); err != nil {
			return err
		}
		order.Stage = "pay"
	}
	if order.Stage == "pay" {
		status, err := a.charge(ctx, order.ID, order.Amount, breaker)
		if err != nil {
			return err
		}
		if status == "paid" {
			// This row is also the outbox: paid and the obligation to publish commit together.
			_, err := a.db.Exec(ctx, `UPDATE orders SET status = 'paid' WHERE id = $1`, order.ID)
			return err
		}
		if _, err := a.db.Exec(ctx, `UPDATE orders SET stage = 'release' WHERE id = $1`, order.ID); err != nil {
			return err
		}
		order.Stage = "release"
	}
	if order.Stage == "release" {
		code, err := a.call(ctx, http.MethodDelete, a.inventoryURL+"/reservations/"+url.PathEscape(order.ID), nil, "", nil)
		if err != nil {
			return err
		}
		if code != http.StatusNoContent {
			return fmt.Errorf("inventory release: %d", code)
		}
		_, err = a.db.Exec(ctx, `UPDATE orders SET status = 'rejected' WHERE id = $1`, order.ID)
		return err
	}
	return fmt.Errorf("unknown order stage %q", order.Stage)
}

func (a *app) charge(ctx context.Context, id string, amount int64, breaker *paymentBreaker) (string, error) {
	if amount == 0 {
		return "paid", nil
	}
	if time.Now().Before(breaker.retryAt) {
		return "", fmt.Errorf("payment circuit open")
	}
	body := map[string]any{"order_id": id, "amount_cents": amount, "currency": "RUB"}
	code, err := a.call(ctx, http.MethodPost, a.paymentURL+"/payments", body, id, nil)
	if err != nil {
		// A timeout or lost reply does not tell us whether the bank already charged.
		var charges []struct {
			OrderID string `json:"order_id"`
			Amount  int64  `json:"amount_cents"`
			Status  string `json:"status"`
		}
		lookupCode, lookupErr := a.call(ctx, http.MethodGet, a.paymentURL+"/payments?order_id="+url.QueryEscape(id), nil, "", &charges)
		if lookupErr == nil && lookupCode == http.StatusOK {
			breaker.failures = 0
			for _, charge := range charges {
				if charge.OrderID == id && charge.Amount == amount && charge.Status == "captured" {
					return "paid", nil
				}
			}
			// A delayed bank rejection has no charge to look up; await an idempotent replay.
			if breaker.responseWait == 0 {
				breaker.responseWait = 5 * time.Second
			}
			client := *a.client
			client.Timeout = breaker.responseWait
			code, err = callWithClient(ctx, &client, http.MethodPost, a.paymentURL+"/payments", body, id, nil)
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				// The lookup worked: learn the bank's response time rather than treating it as down.
				breaker.responseWait *= 2
				return "", fmt.Errorf("payment response timed out: %w", err)
			}
		}
		if err != nil || lookupErr != nil || lookupCode != http.StatusOK {
			breaker.retryAt = time.Now().Add(5 * time.Second)
			return "", fmt.Errorf("payment outcome unknown: %w", err)
		}
	}
	switch code {
	case http.StatusOK, http.StatusCreated:
		breaker.failures = 0
		return "paid", nil
	case http.StatusPaymentRequired:
		breaker.failures = 0
		return "release", nil
	default:
		breaker.failures++
		if breaker.failures >= 3 {
			breaker.retryAt = time.Now().Add(5 * time.Second)
		}
		return "", fmt.Errorf("payment: %d", code)
	}
}
