import json
import logging
import os
import re
import threading
import time
import http.client
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg


PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
EVENTS_URL = os.environ["EVENTS_URL"].rstrip("/")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items JSONB NOT NULL,
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
    order_id TEXT PRIMARY KEY REFERENCES orders(id),
    event_id TEXT NOT NULL UNIQUE,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published BOOLEAN NOT NULL DEFAULT FALSE,
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS processed_delivery (
    event_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points BIGINT NOT NULL
);
"""

ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


def db():
    return psycopg.connect(DATABASE_URL)


def init_db():
    for _ in range(60):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as exc:
            log.info("Ожидаю PostgreSQL: %s", exc)
            time.sleep(1)
    raise SystemExit("PostgreSQL недоступен")


def call(method, url, body=None, timeout=3.5, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for name, value in (headers or {}).items():
        req.add_header(name, value)

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            response.read()
            return response.status
    except urllib.error.HTTPError as exc:
        code = exc.code
        exc.close()
        return code


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False

    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False

    for item in items:
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("sku"), str) or not item["sku"]:
            return False
        for name, minimum in (("qty", 1), ("price_cents", 0)):
            value = item.get(name)
            if isinstance(value, bool) or not isinstance(value, int):
                return False
            if value < minimum:
                return False

    return sum(i["qty"] * i["price_cents"] for i in items) > 0


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])

    with db() as conn:
        conn.execute(
            """
            INSERT INTO orders
                (id, user_id, status, phase, amount_cents, items)
            VALUES (%s, %s, 'pending', 'reserve', %s, %s)
            """,
            (order_id, body["user_id"], amount, json.dumps(body["items"])),
        )

    return {"id": order_id, "status": "pending"}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()

    if row is None:
        return None
    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


def set_phase(order_id, phase):
    with db() as conn:
        conn.execute(
            """
            UPDATE orders
            SET phase = %s, next_attempt = now()
            WHERE id = %s AND status = 'pending'
            """,
            (phase, order_id),
        )


def retry_later(order_id, seconds):
    with db() as conn:
        conn.execute(
            """
            UPDATE orders
            SET next_attempt = now() + (%s * interval '1 second')
            WHERE id = %s AND status = 'pending'
            """,
            (seconds, order_id),
        )


def finish_order(order_id, status):
    with db() as conn:
        changed = conn.execute(
            """
            UPDATE orders
            SET status = %s, phase = 'done'
            WHERE id = %s AND status = 'pending'
            RETURNING id
            """,
            (status, order_id),
        ).fetchone()

        if changed is not None and status == "paid":
            conn.execute(
                """
                INSERT INTO outbox (order_id, event_id)
                VALUES (%s, %s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (order_id, str(uuid.uuid4())),
            )


class PaymentGate:
    """Ограничивает обращения к Payment при его недоступности."""

    def __init__(self):
        self.lock = threading.Lock()
        self.failures = 0
        self.open_until = 0.0
        self.probe_in_flight = False

    def permit(self):
        with self.lock:
            now = time.monotonic()
            if now < self.open_until:
                return False
            if self.failures >= 2:
                if self.probe_in_flight:
                    return False
                self.probe_in_flight = True
            return True

    def success(self):
        with self.lock:
            self.failures = 0
            self.open_until = 0.0
            self.probe_in_flight = False

    def failure(self):
        with self.lock:
            if time.monotonic() < self.open_until:
                return
            self.failures += 1
            if self.failures >= 2:
                self.open_until = time.monotonic() + 5
                self.probe_in_flight = False


payment_gate = PaymentGate()
in_progress = set()
in_progress_lock = threading.Lock()
worker_limit = threading.Semaphore(16)


def process_order(row):
    order_id, phase, amount, items = row

    try:
        if phase == "reserve":
            code = call(
                "POST",
                f"{INVENTORY_URL}/reservations",
                {
                    "order_id": order_id,
                    "items": [
                        {"sku": item["sku"], "qty": item["qty"]}
                        for item in items
                    ],
                },
            )
            if code not in (200, 201):
                retry_later(order_id, 0.5)
                return
            set_phase(order_id, "payment")
            phase = "payment"

        if phase == "payment":
            if not payment_gate.permit():
                retry_later(order_id, 1)
                return

            try:
                code = call(
                    "POST",
                    f"{PAYMENT_URL}/payments",
                    {
                        "order_id": order_id,
                        "amount_cents": amount,
                        "currency": "RUB",
                    },
                    headers={"Idempotency-Key": order_id},
                )
            except OSError as exc:
                log.warning("Оплата %s: нет ответа: %s", order_id, exc)

                if isinstance(exc, (http.client.RemoteDisconnected, ConnectionResetError)):
                    payment_gate.success()
                    retry_later(order_id, 0.1)
                else:
                    payment_gate.failure()
                    retry_later(order_id, 0.5)
                return

            if code in (200, 201):
                payment_gate.success()
                finish_order(order_id, "paid")
                return

            if code == 402:
                payment_gate.success()
                set_phase(order_id, "release")
                phase = "release"
            else:
                log.warning("Оплата %s: HTTP %s", order_id, code)
                payment_gate.success()
                retry_later(order_id, 1)
                return

        if phase == "release":
            code = call(
                "DELETE",
                f"{INVENTORY_URL}/reservations/{order_id}",
            )
            if code != 204:
                retry_later(order_id, 0.5)
                return
            finish_order(order_id, "rejected")

    except Exception:
        log.exception("Не удалось обработать заказ %s", order_id)
        try:
            retry_later(order_id, 1)
        except Exception:
            log.exception("Не удалось отложить заказ %s", order_id)
    finally:
        with in_progress_lock:
            in_progress.discard(order_id)
        worker_limit.release()


def work_loop():
    while True:
        try:
            with db() as conn:
                rows = conn.execute(
                    """
                    SELECT id, phase, amount_cents, items
                    FROM orders
                    WHERE status = 'pending' AND next_attempt <= now()
                    ORDER BY next_attempt, created_at
                    LIMIT 100
                    """
                ).fetchall()

            for row in rows:
                order_id = row[0]
                if not worker_limit.acquire(blocking=False):
                    break
                with in_progress_lock:
                    if order_id in in_progress:
                        worker_limit.release()
                        continue
                    in_progress.add(order_id)
                threading.Thread(
                    target=process_order,
                    args=(row,),
                    daemon=True,
                ).start()

        except Exception:
            log.exception("Ошибка фонового обработчика")

        time.sleep(0.1)


event_in_progress = set()
event_lock = threading.Lock()
event_limit = threading.Semaphore(8)


def publish_event(row):
    order_id, event_id, occurred_at = row

    try:
        code = call(
            "POST",
            f"{EVENTS_URL}/events",
            {
                "id": event_id,
                "type": "order.paid",
                "order_id": order_id,
                "occurred_at": occurred_at.isoformat(),
            },
            timeout=1.5,
        )

        if code == 202:
            with db() as conn:
                conn.execute(
                    "UPDATE outbox SET published = TRUE WHERE order_id = %s",
                    (order_id,),
                )
        else:
            log.warning("Событие заказа %s: HTTP %s", order_id, code)
            postpone_event(order_id)

    except Exception:
        log.exception("Не удалось отправить событие заказа %s", order_id)
        try:
            postpone_event(order_id)
        except Exception:
            log.exception("Не удалось отложить событие заказа %s", order_id)
    finally:
        with event_lock:
            event_in_progress.discard(order_id)
        event_limit.release()


def postpone_event(order_id):
    with db() as conn:
        conn.execute(
            """
            UPDATE outbox
            SET next_attempt = now() + interval '1 second'
            WHERE order_id = %s AND published = FALSE
            """,
            (order_id,),
        )


def publish_loop():
    while True:
        try:
            with db() as conn:
                rows = conn.execute(
                    """
                    SELECT order_id, event_id, occurred_at
                    FROM outbox
                    WHERE published = FALSE AND next_attempt <= now()
                    ORDER BY next_attempt, occurred_at
                    LIMIT 100
                    """
                ).fetchall()

            for row in rows:
                order_id = row[0]
                if not event_limit.acquire(blocking=False):
                    break
                with event_lock:
                    if order_id in event_in_progress:
                        event_limit.release()
                        continue
                    event_in_progress.add(order_id)
                threading.Thread(
                    target=publish_event,
                    args=(row,),
                    daemon=True,
                ).start()

        except Exception:
            log.exception("Ошибка обработчика исходящих событий")

        time.sleep(0.1)


def valid_delivery(event):
    if not isinstance(event, dict):
        return False
    if not isinstance(event.get("id"), str) or not event["id"]:
        return False
    if event.get("type") != "delivery.confirmed":
        return False
    if not isinstance(event.get("user_id"), str) or not event["user_id"]:
        return False
    points = event.get("bonus_points")
    return isinstance(points, int) and not isinstance(points, bool) and points > 0


def delivery_confirmed(event):
    with db() as conn:
        inserted = conn.execute(
            """
            INSERT INTO processed_delivery (event_id)
            VALUES (%s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (event["id"],),
        ).fetchone()

        if inserted is not None:
            conn.execute(
                """
                INSERT INTO bonuses (user_id, points)
                VALUES (%s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET points = bonuses.points + EXCLUDED.points
                """,
                (event["user_id"], event["bonus_points"]),
            )


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute(
            "SELECT points FROM bonuses WHERE user_id = %s",
            (user_id,),
        ).fetchone()
    return {"points": row[0] if row is not None else 0}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            self.send_json(200, {"status": "ok"})
            return

        match = ORDER_PATH.match(self.path)
        if match:
            order = get_order(match.group(1))
            if order is None:
                self.send_json(404, {"error": "not_found"})
            else:
                self.send_json(200, order)
            return

        match = BONUS_PATH.match(self.path)
        if match:
            user_id = urllib.parse.unquote(match.group(1))
            self.send_json(200, get_bonus(user_id))
            return

        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            self.send_json(404, {"error": "not_found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length) or b"null")
        except (ValueError, OverflowError):
            body = None

        if self.path == "/webhooks/delivery":
            if not valid_delivery(body):
                self.send_json(400, {"error": "bad_request"})
                return
            try:
                delivery_confirmed(body)
                self.send_json(200, {"status": "ok"})
            except Exception:
                log.exception("POST /webhooks/delivery")
                self.send_json(500, {"error": "internal"})
            return

        if not valid(body):
            self.send_json(400, {"error": "bad_request"})
            return

        try:
            result = create_order(body)
            self.send_json(201, result)
        except Exception:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    threading.Thread(target=work_loop, daemon=True).start()
    threading.Thread(target=publish_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log.info("Orders слушает порт %s", PORT)
    server.serve_forever()
