package main

// Жизненный цикл заказа: шаги new → reserved → (done | declined → done).
// Каждый шаг идемпотентен — его можно повторить сколько угодно раз:
//   - резерв в Inventory один на order_id, повтор возвращает тот же;
//   - списание в Payment с Idempotency-Key = order_id, повтор возвращает
//     результат той же операции (тот же charge_id или снова 402);
//   - снятие резерва повторно тоже отвечает 204.
// Поэтому после любого сбоя (таймаут, обрыв, падение Orders) заказ просто
// проходит шаги заново с того, на котором остановился.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	statusPending  = "pending"
	statusPaid     = "paid"
	statusRejected = "rejected"

	stageNew      = "new"      // нужно зарезервировать товар
	stageReserved = "reserved" // товар в резерве, нужно списать деньги
	stageDeclined = "declined" // банк отказал, нужно снять резерв
	stageDone     = "done"
)

const (
	// inlineBudget — сколько POST /orders может ждать соседей: ответ
	// покупателю не позже 1 с, остальное — запас на базу и сеть.
	inlineBudget = 700 * time.Millisecond
	// bgBudget — на один проход заказа в фоне; bgCallTimeout — на один
	// запрос к соседу в фоне: дольше обычной задержки Payment, чтобы
	// медленный, но живой Payment успел ответить.
	bgBudget      = 12 * time.Second
	bgCallTimeout = 5 * time.Second

	workerTick    = 300 * time.Millisecond
	workerBatch   = 200
	workerWorkers = 32

	retryBase = 250 * time.Millisecond
	retryMax  = 5 * time.Second
	// breakerWait — когда вернуться к заказу, если breaker не пустил к Payment.
	breakerWait = time.Second
)

type app struct {
	db           *pgxpool.Pool
	paymentURL   string
	inventoryURL string
	eventsURL    string // шина событий; пусто — события не публикуются

	client *http.Client
	pay    *breaker

	// busy — заказы, с которыми сейчас работает этот процесс (обработчик
	// POST или воркер): один заказ не обрабатывается двумя горутинами сразу.
	busy sync.Map
	sem  chan struct{}

	pubBusy sync.Map
	pubSem  chan struct{}
	kick    chan struct{} // разбудить публикатор: появилось новое событие
}

func newApp(paymentURL, inventoryURL, eventsURL string) *app {
	return &app{
		paymentURL:   paymentURL,
		inventoryURL: inventoryURL,
		eventsURL:    eventsURL,
		client: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     30 * time.Second,
		}},
		pay:    newBreaker(),
		sem:    make(chan struct{}, workerWorkers),
		pubSem: make(chan struct{}, workerWorkers),
		kick:   make(chan struct{}, 1),
	}
}

type order struct {
	ID       string
	Amount   int64
	Items    []Item
	Stage    string
	Attempts int
}

func (a *app) lock(id string) bool {
	_, loaded := a.busy.LoadOrStore(id, struct{}{})
	return !loaded
}

func (a *app) unlock(id string) { a.busy.Delete(id) }

// advance проводит заказ по шагам, сколько успеет за ctx, и возвращает его
// статус. background — фоновый проход: ему можно быть пробой breaker'а и
// ждать соседей дольше.
func (a *app) advance(ctx context.Context, o *order, background bool) string {
	callTimeout := bgCallTimeout
	if !background {
		callTimeout = inlineBudget
	}

	if o.Stage == stageNew {
		if err := a.reserve(ctx, callTimeout, o.ID, o.Items); err != nil {
			slog.Warn("резерв не удался", "id", o.ID, "err", err)
			a.retryLater(o, 0)
			return statusPending
		}
		if !a.setStage(o, stageReserved) {
			return statusPending
		}
	}

	if o.Stage == stageReserved {
		if o.Amount == 0 {
			// Списывать нечего: Payment принимает только amount_cents ≥ 1.
			return a.markPaid(o)
		}
		declined, err := a.charge(ctx, callTimeout, o.ID, o.Amount, background)
		switch {
		case errors.Is(err, errBreakerOpen):
			a.retryLater(o, breakerWait)
			return statusPending
		case err != nil:
			// Исход неизвестен (таймаут, обрыв, 5xx): деньги могли списаться.
			// Повтор с тем же ключом это выяснит, не списав второй раз.
			slog.Warn("оплата: исход неизвестен", "id", o.ID, "err", err)
			a.retryLater(o, 0)
			return statusPending
		case declined:
			if !a.setStage(o, stageDeclined) {
				return statusPending
			}
		default:
			return a.markPaid(o)
		}
	}

	if o.Stage == stageDeclined {
		if err := a.release(ctx, callTimeout, o.ID); err != nil {
			slog.Warn("снятие резерва не удалось", "id", o.ID, "err", err)
			a.retryLater(o, 0)
			return statusPending
		}
		return a.markRejected(o)
	}
	return statusPending
}

func (a *app) setStage(o *order, stage string) bool {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := a.db.Exec(ctx,
		`UPDATE orders SET stage = $2, attempts = 0, updated_at = now() WHERE id = $1 AND status = 'pending'`,
		o.ID, stage)
	if err != nil {
		slog.Error("заказ: смена шага", "id", o.ID, "stage", stage, "err", err)
		return false
	}
	o.Stage, o.Attempts = stage, 0
	return true
}

// markPaid — статус paid и событие order.paid в outbox одной транзакцией:
// либо есть оба, либо ни одного. Событие не появится у неоплаченного заказа
// и не потеряется у оплаченного.
func (a *app) markPaid(o *order) string {
	ctx, cancel := dbCtx()
	defer cancel()
	err := pgx.BeginFunc(ctx, a.db, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE orders SET status = 'paid', stage = 'done', updated_at = now() WHERE id = $1 AND status = 'pending'`,
			o.ID)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		eventID := newID()
		payload, _ := json.Marshal(map[string]string{
			"id":          eventID,
			"type":        "order.paid",
			"order_id":    o.ID,
			"occurred_at": time.Now().UTC().Format(time.RFC3339),
		})
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (id, order_id, payload) VALUES ($1, $2, $3) ON CONFLICT (order_id) DO NOTHING`,
			eventID, o.ID, payload)
		return err
	})
	if err != nil {
		// Деньги списаны, а записать не вышло: заказ остаётся pending, воркер
		// повторит списание с тем же ключом (Payment вернёт ту же операцию) и
		// запишет снова.
		slog.Error("заказ: запись paid", "id", o.ID, "err", err)
		a.retryLater(o, 0)
		return statusPending
	}
	o.Stage = stageDone
	a.wakePublisher()
	return statusPaid
}

func (a *app) markRejected(o *order) string {
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := a.db.Exec(ctx,
		`UPDATE orders SET status = 'rejected', stage = 'done', updated_at = now() WHERE id = $1 AND status = 'pending'`,
		o.ID)
	if err != nil {
		slog.Error("заказ: запись rejected", "id", o.ID, "err", err)
		a.retryLater(o, 0)
		return statusPending
	}
	o.Stage = stageDone
	return statusRejected
}

// retryLater назначает следующий проход заказа: через delay или, если delay
// нулевой, с экспоненциальной паузой по числу неудачных попыток.
func (a *app) retryLater(o *order, delay time.Duration) {
	inc := 0
	if delay == 0 {
		delay = backoff(o.Attempts)
		inc = 1
	}
	ctx, cancel := dbCtx()
	defer cancel()
	_, err := a.db.Exec(ctx,
		`UPDATE orders SET attempts = attempts + $2, next_attempt_at = now() + $3::bigint * interval '1 millisecond'
		 WHERE id = $1 AND status = 'pending'`,
		o.ID, inc, delay.Milliseconds())
	if err != nil {
		slog.Error("заказ: планирование повтора", "id", o.ID, "err", err)
	}
	o.Attempts += inc
}

func backoff(attempts int) time.Duration {
	d := retryBase
	for range attempts {
		d *= 2
		if d >= retryMax {
			return retryMax
		}
	}
	return d
}

// orderWorker — фоновый цикл: берёт из базы pending-заказы, которым пора, и
// доводит их до конечного статуса. Источник правды — база, поэтому после
// перезапуска Orders цикл продолжает с того же места.
func (a *app) orderWorker() {
	t := time.NewTicker(workerTick)
	defer t.Stop()
	for range t.C {
		for _, o := range a.dueOrders() {
			if !a.lock(o.ID) {
				continue
			}
			go func() {
				a.sem <- struct{}{}
				defer func() { <-a.sem }()
				defer a.unlock(o.ID)
				ctx, cancel := context.WithTimeout(context.Background(), bgBudget)
				defer cancel()
				if st := a.advance(ctx, o, true); st != statusPending {
					slog.Info("order (фон)", "id", o.ID, "status", st)
				}
			}()
		}
	}
}

func (a *app) dueOrders() []*order {
	ctx, cancel := dbCtx()
	defer cancel()
	rows, err := a.db.Query(ctx,
		`SELECT id, amount_cents, items, stage, attempts FROM orders
		 WHERE status = 'pending' AND next_attempt_at <= now()
		 ORDER BY next_attempt_at LIMIT $1`, workerBatch)
	if err != nil {
		slog.Error("воркер: выборка заказов", "err", err)
		return nil
	}
	defer rows.Close()
	var out []*order
	for rows.Next() {
		o := &order{}
		var items []byte
		if err := rows.Scan(&o.ID, &o.Amount, &items, &o.Stage, &o.Attempts); err != nil {
			slog.Error("воркер: чтение заказа", "err", err)
			return out
		}
		_ = json.Unmarshal(items, &o.Items)
		out = append(out, o)
	}
	return out
}
