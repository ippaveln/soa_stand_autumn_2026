import { createApp } from "./app.js";
import { connectDatabase } from "./database.js";
import { startWorkers } from "./workers.js";

function env(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing ${name}`);
  return value;
}

const services = {
  payment: env("PAYMENT_URL").replace(/\/$/, ""),
  inventory: env("INVENTORY_URL").replace(/\/$/, ""),
  events: env("EVENTS_URL").replace(/\/$/, ""),
};
const db = await connectDatabase(env("DATABASE_URL"));
const app = createApp(db);
const stopWorkers = startWorkers(db, services);
await app.listen({ port: Number(process.env.PORT ?? 8080), host: "0.0.0.0" });

async function shutdown() {
  await app.close();
  await stopWorkers();
  await db.end();
}
process.once("SIGTERM", () => void shutdown());
process.once("SIGINT", () => void shutdown());
