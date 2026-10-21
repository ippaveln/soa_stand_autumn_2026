import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import { createApp } from "../dist/app.js";
import { connectDatabase, creditBonus, markPaid } from "../dist/database.js";
import { startWorkers } from "../dist/workers.js";

const url = process.env.TEST_DATABASE_URL;
if (!url || new URL(url).pathname !== "/orders_test") {
  throw new Error(
    "TEST_DATABASE_URL must point to the separate orders_test database",
  );
}

test("PostgreSQL: валидация, транзакции и восстановление обработки", async (t) => {
  const db = await connectDatabase(url);
  const app = createApp(db);
  t.after(async () => {
    await app.close();
    await db.end();
  });
  await db.query("TRUNCATE orders, outbox, inbox, bonuses CASCADE");

  await t.test("некорректные заказы не записываются", async () => {
    const invalid = [
      {},
      { user_id: "u", items: [] },
      { user_id: "u", items: [{ sku: "a", qty: -1, price_cents: 10 }] },
      { user_id: "u", items: [{ sku: "a", qty: "1", price_cents: 10 }] },
      {
        user_id: "u",
        items: [{ sku: "a", qty: 2, price_cents: Number.MAX_SAFE_INTEGER }],
      },
    ];
    for (const payload of invalid) {
      const response = await app.inject({
        method: "POST",
        url: "/orders",
        payload,
      });
      assert.equal(response.statusCode, 400);
    }
    const { rows } = await db.query(
      "SELECT count(*)::int AS count FROM orders",
    );
    assert.equal(rows[0].count, 0);
    assert.equal((await app.inject("/orders/missing")).statusCode, 404);
    assert.deepEqual((await app.inject("/users/missing/bonus")).json(), {
      points: 0,
    });
  });

  await t.test("параллельные дубликаты начисляют бонусы один раз", async () => {
    const duplicate = {
      id: "same",
      type: "delivery.confirmed",
      user_id: "u",
      bonus_points: 7,
    };
    const events = [
      ...Array.from({ length: 20 }, () => duplicate),
      ...Array.from({ length: 20 }, (_, i) => ({
        ...duplicate,
        id: `unique-${i}`,
        bonus_points: 3,
      })),
    ];
    const responses = await Promise.all(
      events.map((payload) =>
        app.inject({
          method: "POST",
          url: "/webhooks/delivery",
          payload,
        }),
      ),
    );
    assert.ok(responses.every((response) => response.statusCode === 204));
    assert.deepEqual((await app.inject("/users/u/bonus")).json(), {
      points: 67,
    });
    await creditBonus(db, duplicate);
    assert.deepEqual((await app.inject("/users/u/bonus")).json(), {
      points: 67,
    });
  });

  await t.test(
    "ошибка начисления откатывает отметку входящего события",
    async () => {
      const event = {
        id: "retry-credit",
        type: "delivery.confirmed",
        user_id: "rollback",
        bonus_points: 101,
      };
      await db.query(
        "ALTER TABLE bonuses ADD CONSTRAINT test_limit CHECK (points <= 100)",
      );
      try {
        await assert.rejects(creditBonus(db, event));
        const { rowCount } = await db.query(
          "SELECT id FROM inbox WHERE id = $1",
          [event.id],
        );
        assert.equal(rowCount, 0);
      } finally {
        await db.query("ALTER TABLE bonuses DROP CONSTRAINT test_limit");
      }
      await creditBonus(db, event);
      assert.deepEqual((await app.inject("/users/rollback/bonus")).json(), {
        points: 101,
      });
    },
  );

  await t.test("paid не фиксируется без записи события", async () => {
    await db.query(
      "INSERT INTO orders (id, user_id, items, amount_cents) VALUES ('atomic', 'u', '[]', 10)",
    );
    await db.query(
      "ALTER TABLE outbox ADD CONSTRAINT test_outbox CHECK (order_id <> 'atomic')",
    );
    try {
      await assert.rejects(markPaid(db, "atomic"));
      const { rows } = await db.query(
        "SELECT status FROM orders WHERE id = 'atomic'",
      );
      assert.equal(rows[0].status, "pending");
    } finally {
      await db.query("ALTER TABLE outbox DROP CONSTRAINT test_outbox");
    }
    await markPaid(db, "atomic");
    await markPaid(db, "atomic");
    const { rows } = await db.query(
      "SELECT count(*)::int AS count FROM outbox WHERE order_id = 'atomic'",
    );
    assert.equal(rows[0].count, 1);
    await db.query("TRUNCATE orders, outbox CASCADE");
  });

  await t.test(
    "сохранённый заказ завершается после потери ответа банка",
    async () => {
      let charges = 0;
      const eventIds = [];
      const server = createServer(async (request, response) => {
        let body = "";
        for await (const chunk of request) body += chunk;
        if (request.url === "/payments" && request.method === "POST") {
          assert.ok(request.headers["idempotency-key"]);
          charges++;
          request.socket.destroy();
          return;
        }
        if (request.url.startsWith("/payments?")) {
          response.end(JSON.stringify([{ charge_id: "charge-1" }]));
          return;
        }
        if (request.url === "/events") {
          eventIds.push(JSON.parse(body).id);
          response.statusCode = eventIds.length === 1 ? 503 : 202;
          response.end("{}");
          return;
        }
        response.statusCode = 201;
        response.end("{}");
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const address = `http://127.0.0.1:${server.address().port}`;
      const created = await app.inject({
        method: "POST",
        url: "/orders",
        payload: {
          user_id: "recovery",
          items: [{ sku: "a", qty: 2, price_cents: 50 }],
        },
      });
      assert.equal(created.statusCode, 201);
      assert.equal(created.json().status, "pending");
      const stop = startWorkers(db, {
        payment: address,
        inventory: address,
        events: address,
      });
      try {
        const deadline = Date.now() + 10000;
        while (Date.now() < deadline) {
          const { rows } = await db.query(
            "SELECT sent_at FROM outbox WHERE order_id = $1",
            [created.json().id],
          );
          if (rows[0]?.sent_at) break;
          await delay(100);
        }
        assert.equal(charges, 1);
        assert.equal(eventIds.length, 2);
        assert.equal(eventIds[0], eventIds[1]);
        const order = (await app.inject(`/orders/${created.json().id}`)).json();
        assert.equal(order.status, "paid");
        assert.equal(order.amount_cents, 100);
      } finally {
        await stop();
        server.closeAllConnections();
        await new Promise((resolve) => server.close(resolve));
      }
    },
  );
});
