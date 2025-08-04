CREATE TABLE orders (
                        id UUID PRIMARY KEY,
                        total NUMERIC
);

CREATE TABLE order_items (
                             id SERIAL PRIMARY KEY,
                             order_id UUID REFERENCES orders(id),
                             product TEXT,
                             quantity INTEGER,
                             price NUMERIC
);

CREATE TABLE outbox (
                        id SERIAL PRIMARY KEY,
                        order_id UUID,
                        total NUMERIC
);
