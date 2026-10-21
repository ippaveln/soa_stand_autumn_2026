CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    items JSONB NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid', 'rejected')),
    stage TEXT NOT NULL DEFAULT 'reserve' CHECK (stage IN ('reserve', 'payment', 'release')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pending_orders ON orders (next_attempt) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES orders(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS unsent_events ON outbox (next_attempt) WHERE sent_at IS NULL;

CREATE TABLE IF NOT EXISTS inbox (
    id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points BIGINT NOT NULL CHECK (points >= 0)
);
