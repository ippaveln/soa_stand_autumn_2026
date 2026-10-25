package main

// Вызовы соседей: Inventory, Payment (через circuit breaker), шина событий.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

func (a *app) reserve(ctx context.Context, timeout time.Duration, id string, items []Item) error {
	type resvItem struct {
		SKU string `json:"sku"`
		Qty int    `json:"qty"`
	}
	ri := make([]resvItem, 0, len(items))
	for _, it := range items {
		ri = append(ri, resvItem{it.SKU, it.Qty})
	}
	code, err := a.call(ctx, timeout, http.MethodPost, a.inventoryURL+"/reservations",
		map[string]any{"order_id": id, "items": ri}, nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK && code != http.StatusCreated {
		return fmt.Errorf("inventory: %d", code)
	}
	return nil
}

func (a *app) release(ctx context.Context, timeout time.Duration, id string) error {
	code, err := a.call(ctx, timeout, http.MethodDelete, a.inventoryURL+"/reservations/"+id, nil, nil)
	if err != nil {
		return err
	}
	if code != http.StatusNoContent && code != http.StatusOK {
		return fmt.Errorf("inventory: %d", code)
	}
	return nil
}

var errBreakerOpen = errors.New("payment: circuit breaker открыт")

// charge списывает деньги; declined — банк отказал (402). Ключ
// идемпотентности — id заказа, тело запроса для заказа всегда одно и то же:
// любой повтор вернёт результат первой успешной операции, второго списания
// не будет. err != nil — исход неизвестен, нужно повторить.
func (a *app) charge(ctx context.Context, timeout time.Duration, id string, amount int64, canProbe bool) (declined bool, err error) {
	if !a.pay.allow(canProbe) {
		return false, errBreakerOpen
	}
	code, err := a.call(ctx, timeout, http.MethodPost, a.paymentURL+"/payments",
		map[string]any{"order_id": id, "amount_cents": amount, "currency": "RUB"},
		map[string]string{"Idempotency-Key": id})
	// Сбой Payment — нет ответа или 5xx; 402 и прочие ответы значат, что
	// Payment жив.
	a.pay.record(err != nil || code >= 500)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK, http.StatusCreated:
		return false, nil
	case http.StatusPaymentRequired:
		return true, nil
	}
	return false, fmt.Errorf("payment: %d", code)
}

// call — один HTTP-запрос с таймаутом; тело ответа не нужно.
func (a *app) call(ctx context.Context, timeout time.Duration, method, url string, body any, headers map[string]string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
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
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, nil
}

// ---------- circuit breaker ----------

// breaker защищает Payment, пока тот недоступен. closed — запросы идут;
// после breakerThreshold сбоев подряд — open: запросов нет cooldown; потом
// half-open — проходит один пробный запрос из фона. Проба удалась — closed,
// нет — снова open, cooldown удваивается до breakerMaxCooldown.
type breaker struct {
	mu        sync.Mutex
	state     int
	fails     int
	cooldown  time.Duration
	openUntil time.Time
}

const (
	breakerClosed = iota
	breakerOpen
	breakerHalfOpen

	breakerThreshold   = 3
	breakerMinCooldown = time.Second
	breakerMaxCooldown = 5 * time.Second
)

func newBreaker() *breaker { return &breaker{cooldown: breakerMinCooldown} }

// allow — можно ли сейчас обратиться к Payment. canProbe — вызывающий готов
// быть пробой (фоновый проход с долгим таймаутом).
func (b *breaker) allow(canProbe bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if !canProbe || time.Now().Before(b.openUntil) {
			return false
		}
		b.state = breakerHalfOpen
		return true
	}
	return false // half-open: проба уже идёт
}

// record — исход запроса, пропущенного allow.
func (b *breaker) record(failed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !failed {
		b.state, b.fails, b.cooldown = breakerClosed, 0, breakerMinCooldown
		return
	}
	switch b.state {
	case breakerClosed:
		b.fails++
		if b.fails >= breakerThreshold {
			b.state, b.openUntil = breakerOpen, time.Now().Add(b.cooldown)
		}
	case breakerHalfOpen:
		b.cooldown = min(2*b.cooldown, breakerMaxCooldown)
		b.state, b.openUntil = breakerOpen, time.Now().Add(b.cooldown)
	}
	// open: запросы, начатые до открытия, ничего не меняют.
}
