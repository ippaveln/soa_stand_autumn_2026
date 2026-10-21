import { setTimeout as delay } from "node:timers/promises";
import type { Pool } from "pg";
import { CircuitBreaker } from "./circuit-breaker.js";
import { markPaid } from "./database.js";
import { call, retryDelay } from "./http.js";
import type { Order } from "./types.js";

export interface Services {
  payment: string;
  inventory: string;
  events: string;
}

export function startWorkers(
  db: Pool,
  services: Services,
): () => Promise<void> {
  let stopped = false;
  const payment = new CircuitBreaker();

  async function processOrder(order: Order): Promise<void> {
    try {
      if (order.stage === "reserve") {
        const response = await call(
          `${services.inventory}/reservations`,
          "POST",
          {
            order_id: order.id,
            items: order.items.map(({ sku, qty }) => ({ sku, qty })),
          },
        );
        if (response.status !== 200 && response.status !== 201) {
          throw new Error(`Inventory: ${response.status}`);
        }
        await db.query("UPDATE orders SET stage = 'payment' WHERE id = $1", [
          order.id,
        ]);
        order.stage = "payment";
      }

      if (order.stage === "payment") {
        if (order.amount_cents === "0") {
          await markPaid(db, order.id);
          return;
        }
        if (!payment.allow()) {
          await db.query(
            "UPDATE orders SET next_attempt = now() + interval '1 second' WHERE id = $1",
            [order.id],
          );
          return;
        }

        let status: number;
        try {
          const response = await call(
            `${services.payment}/payments`,
            "POST",
            {
              order_id: order.id,
              amount_cents: Number(order.amount_cents),
              currency: "RUB",
            },
            order.id,
          );
          status = response.status;
        } catch {
          // Потерянный ответ не означает, что деньги не списались.
          try {
            const lookup = await call(
              `${services.payment}/payments?order_id=${encodeURIComponent(order.id)}`,
            );
            status =
              lookup.status === 200 &&
              Array.isArray(lookup.body) &&
              lookup.body.length > 0
                ? 200
                : 503;
          } catch {
            status = 503;
          }
        }

        if (status !== 200 && status !== 201 && status !== 402) {
          payment.failure();
          throw new Error(`Payment: ${status}`);
        }
        payment.success();
        if (status !== 402) {
          await markPaid(db, order.id);
          return;
        }
        await db.query("UPDATE orders SET stage = 'release' WHERE id = $1", [
          order.id,
        ]);
        order.stage = "release";
      }

      if (order.stage === "release") {
        const response = await call(
          `${services.inventory}/reservations/${encodeURIComponent(order.id)}`,
          "DELETE",
        );
        if (response.status !== 204)
          throw new Error(`Release: ${response.status}`);
        await db.query("UPDATE orders SET status = 'rejected' WHERE id = $1", [
          order.id,
        ]);
      }
    } catch (error) {
      console.warn(
        "Order retry:",
        order.id,
        error instanceof Error ? error.message : error,
      );
      await db.query(
        `UPDATE orders SET attempts = attempts + 1,
         next_attempt = now() + $2 * interval '1 millisecond' WHERE id = $1`,
        [order.id, retryDelay(order.attempts)],
      );
    }
  }

  async function ordersBatch(): Promise<void> {
    const { rows } = await db.query<Order>(
      `SELECT id, user_id, items, amount_cents, stage, attempts FROM orders
       WHERE status = 'pending' AND next_attempt <= now()
       ORDER BY next_attempt, created_at LIMIT 4`,
    );
    await Promise.allSettled(rows.map(processOrder));
  }

  async function eventsBatch(): Promise<void> {
    const { rows } = await db.query<{
      id: string;
      order_id: string;
      created_at: Date;
      attempts: number;
    }>(
      `SELECT id, order_id, created_at, attempts FROM outbox
       WHERE sent_at IS NULL AND next_attempt <= now()
       ORDER BY next_attempt LIMIT 16`,
    );
    await Promise.allSettled(
      rows.map(async (event) => {
        try {
          const response = await call(`${services.events}/events`, "POST", {
            id: event.id,
            type: "order.paid",
            order_id: event.order_id,
            occurred_at: event.created_at.toISOString(),
          });
          if (response.status !== 202)
            throw new Error(`Events: ${response.status}`);
          await db.query("UPDATE outbox SET sent_at = now() WHERE id = $1", [
            event.id,
          ]);
        } catch (error) {
          console.warn(
            "Event retry:",
            event.id,
            error instanceof Error ? error.message : error,
          );
          await db.query(
            `UPDATE outbox SET attempts = attempts + 1,
           next_attempt = now() + $2 * interval '1 millisecond' WHERE id = $1`,
            [event.id, retryDelay(event.attempts)],
          );
        }
      }),
    );
  }

  async function loop(batch: () => Promise<void>): Promise<void> {
    while (!stopped) {
      try {
        await batch();
      } catch (error) {
        console.error("Worker:", error);
      }
      await delay(100);
    }
  }

  const running = [loop(ordersBatch), loop(eventsBatch)];
  return async () => {
    stopped = true;
    await Promise.all(running);
  };
}
