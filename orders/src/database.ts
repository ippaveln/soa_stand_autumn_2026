import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { Pool } from "pg";
import type { Delivery } from "./types.js";

export async function connectDatabase(url: string): Promise<Pool> {
  const db = new Pool({ connectionString: url, max: 12 });
  db.on("error", (error) =>
    console.error("Database connection:", error.message),
  );
  const schema = await readFile(
    new URL("../schema.sql", import.meta.url),
    "utf8",
  );
  for (let attempt = 0; ; attempt++) {
    try {
      await db.query(schema);
      return db;
    } catch (error) {
      if (attempt >= 59) throw error;
      await delay(1000);
    }
  }
}

export async function markPaid(db: Pool, orderId: string): Promise<void> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query("UPDATE orders SET status = 'paid' WHERE id = $1", [
      orderId,
    ]);
    await client.query(
      `INSERT INTO outbox (id, order_id) VALUES ($1, $2)
       ON CONFLICT (order_id) DO NOTHING`,
      [randomUUID(), orderId],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function creditBonus(db: Pool, event: Delivery): Promise<void> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const inserted = await client.query(
      `INSERT INTO inbox (id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`,
      [event.id],
    );
    if (inserted.rowCount === 1) {
      await client.query(
        `INSERT INTO bonuses (user_id, points) VALUES ($1, $2)
         ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points`,
        [event.user_id, event.bonus_points],
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
