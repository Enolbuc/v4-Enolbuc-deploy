CREATE TABLE idempotency_keys (
    buyer_id UUID NOT NULL,
    key TEXT NOT NULL,
    order_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (buyer_id, key)
);
