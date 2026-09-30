CREATE TABLE listings (
    id UUID PRIMARY KEY,
    seller_id UUID NOT NULL,
    title TEXT NOT NULL,
    price NUMERIC(12,2) NOT NULL CHECK (price > 0),
    stock BIGINT NOT NULL CHECK (stock >= 0),
    sold BIGINT NOT NULL DEFAULT 0 CHECK (sold >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_listings_created_at ON listings (created_at);
