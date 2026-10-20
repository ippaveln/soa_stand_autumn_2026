import json
import logging
import os
import queue
import re
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

WORKERS = 4
POST_WAIT = 0.6
PAYMENT_TIMEOUT = 6.0
INVENTORY_TIMEOUT = 3.0
EVENTS_TIMEOUT = 3.0
BREAKER_THRESHOLD = 4
BREAKER_BASE = 2.0
BREAKER_CAP = 8.0
OUTBOX_PARALLEL = 8
OUTBOX_KEEP_SECONDS = 3600
INBOX_KEEP_SECONDS = 30 * 86400

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    reserved        BOOLEAN NOT NULL DEFAULT FALSE,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_pending_idx ON orders (next_attempt_at) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS outbox (
    id              BIGSERIAL PRIMARY KEY,
    event_id        TEXT NOT NULL UNIQUE,
    order_id        TEXT NOT NULL,
    payload         JSONB NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox (next_attempt_at) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS inbox (
    event_id   TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    points     BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
)
"""

pool: ConnectionPool = None


def init_db():
    for _ in range(90):
        try:
            with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
                conn.execute("SELECT pg_advisory_lock(42)")
                conn.execute(SCHEMA)
                conn.execute("SELECT pg_advisory_unlock(42)")
            return
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("не дождался базы")


class NetError(Exception):
    """Сеть подвела: соединение оборвалось / отказано."""


class NetTimeout(NetError):
    """Не дождались ответа."""


def http(method, url, body=None, headers=None, timeout=5.0):
    """Возвращает (код, json-или-None). На сетевых проблемах бросает NetError."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                code, raw = resp.status, resp.read()
        except urllib.error.HTTPError as e:
            code = e.code
            try:
                raw = e.read()
            except Exception:
                raw = b""
    except (socket.timeout, TimeoutError) as e:
        raise NetTimeout(str(e))
    except urllib.error.URLError as e:
        if isinstance(e.reason, (socket.timeout, TimeoutError)):
            raise NetTimeout(str(e))
        raise NetError(str(e))
    except Exception as e:
        raise NetError(f"{type(e).__name__}: {e}")
    try:
        parsed = json.loads(raw) if raw else None
    except ValueError:
        parsed = None
    return code, parsed


class Breaker:
    """closed -> (N неудач подряд) -> open -> (пауза) -> half-open: один пробный запрос.

    Пробный успешен -> closed. Пробный провален -> снова open, пауза x2 (до CAP).
    """

    def __init__(self, threshold, base, cap):
        self.threshold, self.base, self.cap = threshold, base, cap
        self.lock = threading.Lock()
        self.state = "closed"
        self.fails = 0
        self.cooldown = base
        self.open_until = 0.0

    def blocked(self):
        """Дёшево: «сейчас в Payment ходить нельзя?» (не меняет состояние)."""
        with self.lock:
            if self.state == "closed":
                return False
            if self.state == "open":
                return time.monotonic() < self.open_until
            return True

    def allow(self):
        """Можно ли отправить запрос прямо сейчас (и занять место пробного)."""
        with self.lock:
            if self.state == "closed":
                return True
            if self.state == "open" and time.monotonic() >= self.open_until:
                self.state = "half"
                log.info("breaker: half-open, пробный запрос в Payment")
                return True
            return False

    def success(self):
        with self.lock:
            if self.state != "closed":
                log.info("breaker: closed, Payment снова отвечает")
            self.state, self.fails, self.cooldown = "closed", 0, self.base

    def failure(self):
        with self.lock:
            if self.state == "half":
                self.cooldown = min(self.cooldown * 2, self.cap)
                self._open()
            elif self.state == "closed":
                self.fails += 1
                if self.fails >= self.threshold:
                    self.cooldown = self.base
                    self._open()

    def _open(self):
        self.state = "open"
        self.open_until = time.monotonic() + self.cooldown
        log.warning("breaker: open на %.1f с", self.cooldown)


breaker = Breaker(BREAKER_THRESHOLD, BREAKER_BASE, BREAKER_CAP)


def reserve(order_id, items):
    try:
        code, _ = http("POST", f"{INVENTORY_URL}/reservations", {
            "order_id": order_id,
            "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
        }, timeout=INVENTORY_TIMEOUT)
    except NetError as e:
        log.warning("order %s: inventory reserve: %s", order_id, e)
        return False
    return code in (200, 201)


def release(order_id):
    try:
        code, _ = http("DELETE", f"{INVENTORY_URL}/reservations/{order_id}", timeout=INVENTORY_TIMEOUT)
    except NetError as e:
        log.warning("order %s: inventory release: %s", order_id, e)
        return False
    return code in (200, 204)


def charge(order_id, amount):
    """Возвращает 'paid' | 'declined' | 'retry' | 'blocked'."""
    if not breaker.allow():
        return "blocked"
    try:
        try:
            code, _ = http(
                "POST", f"{PAYMENT_URL}/payments",
                {"order_id": order_id, "amount_cents": amount, "currency": "RUB"},
                headers={"Idempotency-Key": order_id},
                timeout=PAYMENT_TIMEOUT,
            )
        except NetTimeout:
            breaker.failure()
            return "retry"
        except NetError as e:
            log.warning("order %s: payment: %s — проверяю списание", order_id, e)
            return check_charge(order_id)
        if code in (200, 201):
            breaker.success()
            return "paid"
        if code == 402:
            breaker.success()
            return "declined"
        if code >= 500:
            breaker.failure()
        else:
            breaker.success()
        log.warning("order %s: payment вернул %s", order_id, code)
        return "retry"
    except Exception:
        breaker.failure()
        log.exception("order %s: charge", order_id)
        return "retry"


def check_charge(order_id):
    try:
        code, body = http(
            "GET", f"{PAYMENT_URL}/payments?order_id={urllib.parse.quote(order_id)}", timeout=3.0)
    except NetError:
        breaker.failure()
        return "retry"
    if code == 200 and isinstance(body, list) and any(c.get("status") == "captured" for c in body):
        breaker.success()
        return "paid"
    breaker.failure()
    return "retry"


tasks = queue.Queue()
inflight = set()
waiters = {}
state_lock = threading.Lock()


def enqueue(order_id):
    with state_lock:
        if order_id in inflight:
            return False
        inflight.add(order_id)
    tasks.put(order_id)
    return True


def schedule_retry(order_id):
    """Следующая попытка позже: 0.5, 1, 2, 4, 5, 5... секунд (+ немного случайности)."""
    with pool.connection() as conn:
        conn.execute(
            "UPDATE orders SET attempts = attempts + 1, "
            "next_attempt_at = now() + (LEAST(0.5 * power(2, LEAST(attempts, 10)), 5.0) + random() * 0.5) "
            "* interval '1 second' WHERE id = %s AND status = 'pending'",
            (order_id,),
        )


def finish_paid(order_id):
    """paid + событие в outbox — ОДНА транзакция (всё или ничего)."""
    with pool.connection() as conn:
        cur = conn.execute(
            "UPDATE orders SET status = 'paid' WHERE id = %s AND status = 'pending'", (order_id,))
        if cur.rowcount == 1:
            event_id = str(uuid.uuid4())
            payload = {
                "id": event_id,
                "type": "order.paid",
                "order_id": order_id,
                "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            conn.execute(
                "INSERT INTO outbox (event_id, order_id, payload) VALUES (%s, %s, %s)",
                (event_id, order_id, Jsonb(payload)),
            )
    log.info("order %s: paid", order_id)


def finish_rejected(order_id):
    with pool.connection() as conn:
        conn.execute("UPDATE orders SET status = 'rejected' WHERE id = %s AND status = 'pending'", (order_id,))
    log.info("order %s: rejected", order_id)


def process_order(order_id):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT status, amount_cents, items, reserved FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None or row[0] != "pending":
        return
    _, amount, items, reserved = row

    if amount == 0:
        finish_paid(order_id)
        return

    if breaker.blocked():
        return

    if not reserved:
        if not reserve(order_id, items):
            schedule_retry(order_id)
            return
        with pool.connection() as conn:
            conn.execute("UPDATE orders SET reserved = TRUE WHERE id = %s", (order_id,))

    result = charge(order_id, amount)
    if result == "paid":
        finish_paid(order_id)
    elif result == "declined":
        if release(order_id):
            finish_rejected(order_id)
        else:
            schedule_retry(order_id)
    elif result == "retry":
        schedule_retry(order_id)


def worker():
    while True:
        order_id = tasks.get()
        try:
            process_order(order_id)
        except Exception:
            log.exception("order %s: ошибка обработки", order_id)
            try:
                schedule_retry(order_id)
            except Exception:
                pass
        finally:
            with state_lock:
                inflight.discard(order_id)
                ev = waiters.get(order_id)
            if ev:
                ev.set()


def scanner():
    """Каждые полсекунды берёт из БД pending-заказы, которым пора на попытку.

    Он же подбирает заказы после перезапуска сервиса и ретраи после ошибок.
    """
    while True:
        try:
            if not breaker.blocked():
                with pool.connection() as conn:
                    rows = conn.execute(
                        "SELECT id FROM orders WHERE status = 'pending' AND next_attempt_at <= now() "
                        "ORDER BY created_at LIMIT 200").fetchall()
                for (oid,) in rows:
                    enqueue(oid)
        except Exception:
            log.exception("scanner")
        time.sleep(0.5)


def publish_one(row_id, payload):
    try:
        code, _ = http("POST", f"{EVENTS_URL}/events", payload, timeout=EVENTS_TIMEOUT)
        ok = code == 202
    except NetError:
        ok = False
    with pool.connection() as conn:
        if ok:
            conn.execute("UPDATE outbox SET sent_at = now() WHERE id = %s", (row_id,))
        else:
            conn.execute(
                "UPDATE outbox SET attempts = attempts + 1, "
                "next_attempt_at = now() + (LEAST(0.5 * power(2, LEAST(attempts, 10)), 4.0) + random() * 0.5) "
                "* interval '1 second' WHERE id = %s", (row_id,))
    return ok


def publisher():
    ex = ThreadPoolExecutor(OUTBOX_PARALLEL)
    while True:
        try:
            with pool.connection() as conn:
                rows = conn.execute(
                    "SELECT id, payload FROM outbox "
                    "WHERE sent_at IS NULL AND next_attempt_at <= now() ORDER BY id LIMIT 50").fetchall()
            if not rows:
                time.sleep(0.3)
                continue
            futures = [ex.submit(publish_one, r[0], r[1]) for r in rows]
            wait(futures)
            if not any(f.result() for f in futures):
                time.sleep(0.5)
        except Exception:
            log.exception("publisher")
            time.sleep(1)


def cleaner():
    """Чтобы таблицы не росли бесконечно."""
    while True:
        time.sleep(60)
        try:
            with pool.connection() as conn:
                conn.execute("DELETE FROM outbox WHERE sent_at IS NOT NULL "
                             "AND sent_at < now() - %s * interval '1 second'", (OUTBOX_KEEP_SECONDS,))
                conn.execute("DELETE FROM inbox WHERE created_at < now() - %s * interval '1 second'",
                             (INBOX_KEEP_SECONDS,))
        except Exception:
            log.exception("cleaner")


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    return True


def get_status(order_id):
    with pool.connection() as conn:
        row = conn.execute("SELECT status FROM orders WHERE id = %s", (order_id,)).fetchone()
    return row[0] if row else "pending"


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])

    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES (%s, %s, 'pending', %s, %s)",
            (order_id, body["user_id"], amount, Jsonb(body["items"])),
        )

    if not breaker.blocked():
        ev = threading.Event()
        with state_lock:
            waiters[order_id] = ev
        try:
            if enqueue(order_id):
                ev.wait(POST_WAIT)
        finally:
            with state_lock:
                waiters.pop(order_id, None)
    return {"id": order_id, "status": get_status(order_id)}


def get_order(order_id):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


def delivery_confirmed(event):
    """Бонусы за доставку — ровно один раз на событие.

    Запись в inbox и начисление — одна транзакция. PRIMARY KEY (event_id) не даёт
    двум копиям (даже одновременным) пройти дальше: вторая ждёт первую и получает
    «уже есть» (ON CONFLICT DO NOTHING -> rowcount 0).
    """
    with pool.connection() as conn:
        cur = conn.execute(
            "INSERT INTO inbox (event_id, user_id, points) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (event["id"], event["user_id"], event["bonus_points"]),
        )
        if cur.rowcount == 1:
            conn.execute(
                "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points",
                (event["user_id"], event["bonus_points"]),
            )


def get_bonus(user_id):
    with pool.connection() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return {"points": row[0] if row else 0}


ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


class Server(ThreadingHTTPServer):
    request_queue_size = 256
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        m = ORDER_PATH.match(path)
        if m:
            order = get_order(urllib.parse.unquote(m.group(1)))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        m = BONUS_PATH.match(path)
        if m:
            return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if self.path == "/webhooks/delivery":
            return self.webhook(body)
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if (not isinstance(body, dict)
                or not isinstance(body.get("id"), str) or not body["id"]
                or not isinstance(body.get("user_id"), str) or not body["user_id"]
                or not isinstance(body.get("bonus_points"), int) or isinstance(body.get("bonus_points"), bool)
                or body["bonus_points"] <= 0):
            return self.send_json(400, {"error": "bad_request"})
        try:
            delivery_confirmed(body)
        except Exception as e:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


def main():
    global pool
    init_db()
    pool = ConnectionPool(DATABASE_URL, min_size=4, max_size=40, timeout=5, open=False)
    pool.open(wait=True, timeout=60)

    with pool.connection() as conn:
        conn.execute("UPDATE orders SET next_attempt_at = now() WHERE status = 'pending'")
        conn.execute("UPDATE outbox SET next_attempt_at = now() WHERE sent_at IS NULL")

    for _ in range(WORKERS):
        threading.Thread(target=worker, daemon=True).start()
    threading.Thread(target=scanner, daemon=True).start()
    if EVENTS_URL:
        threading.Thread(target=publisher, daemon=True).start()
    threading.Thread(target=cleaner, daemon=True).start()

    server = Server(("0.0.0.0", PORT), Handler)
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()


if __name__ == "__main__":
    main()
