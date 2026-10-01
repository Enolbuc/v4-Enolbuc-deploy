CREATE TABLE orders (
    id UUID PRIMARY KEY,
    buyer_id UUID NOT NULL,
    listing_id UUID NOT NULL,
    qty BIGINT NOT NULL CHECK (qty > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_buyer_created ON orders (buyer_id, created_at DESC);
