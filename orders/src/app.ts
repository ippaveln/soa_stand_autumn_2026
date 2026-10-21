import { randomUUID } from "node:crypto";
import Fastify from "fastify";
import type { Pool } from "pg";
import { creditBonus } from "./database.js";
import type { Delivery, OrderInput } from "./types.js";

const nonempty = { type: "string", minLength: 1 };
const integer = { type: "integer", maximum: Number.MAX_SAFE_INTEGER };

export function createApp(db: Pool) {
  const app = Fastify({
    logger: { level: "warn" },
    ajv: { customOptions: { coerceTypes: false } },
  });

  app.get("/healthz", async () => {
    await db.query("SELECT 1");
    return { status: "ok" };
  });

  app.post<{ Body: OrderInput }>(
    "/orders",
    {
      schema: {
        body: {
          type: "object",
          required: ["user_id", "items"],
          properties: {
            user_id: nonempty,
            items: {
              type: "array",
              minItems: 1,
              items: {
                type: "object",
                required: ["sku", "qty", "price_cents"],
                properties: {
                  sku: nonempty,
                  qty: { ...integer, minimum: 1 },
                  price_cents: { ...integer, minimum: 0 },
                },
              },
            },
          },
        },
      },
    },
    async (request, reply) => {
      const { user_id, items } = request.body;
      const amount = items.reduce(
        (sum, item) => sum + BigInt(item.qty) * BigInt(item.price_cents),
        0n,
      );
      if (amount > BigInt(Number.MAX_SAFE_INTEGER)) {
        return reply.code(400).send({ error: "amount_too_large" });
      }
      const id = randomUUID();
      await db.query(
        "INSERT INTO orders (id, user_id, items, amount_cents) VALUES ($1, $2, $3, $4)",
        [id, user_id, JSON.stringify(items), amount.toString()],
      );
      return reply.code(201).send({ id, status: "pending" });
    },
  );

  app.get<{ Params: { id: string } }>("/orders/:id", async (request, reply) => {
    const { rows } = await db.query<{
      id: string;
      status: string;
      amount_cents: string;
    }>("SELECT id, status, amount_cents FROM orders WHERE id = $1", [
      request.params.id,
    ]);
    const order = rows[0];
    if (!order) return reply.code(404).send({ error: "not_found" });
    return { ...order, amount_cents: Number(order.amount_cents) };
  });

  app.post<{ Body: Delivery }>(
    "/webhooks/delivery",
    {
      schema: {
        body: {
          type: "object",
          required: ["id", "type", "user_id", "bonus_points"],
          properties: {
            id: nonempty,
            type: { const: "delivery.confirmed", type: "string" },
            user_id: nonempty,
            bonus_points: { ...integer, minimum: 1 },
          },
        },
      },
    },
    async (request, reply) => {
      await creditBonus(db, request.body);
      return reply.code(204).send();
    },
  );

  app.get<{ Params: { id: string } }>("/users/:id/bonus", async (request) => {
    const { rows } = await db.query<{ points: string }>(
      "SELECT points FROM bonuses WHERE user_id = $1",
      [request.params.id],
    );
    return { points: Number(rows[0]?.points ?? 0) };
  });

  return app;
}
