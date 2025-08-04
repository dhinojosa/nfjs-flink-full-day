CREATE TABLE stock_count
(
    stock_symbol VARCHAR(10) NOT NULL PRIMARY KEY,
    trade_count  BIGINT      NOT NULL,
    last_updated TIMESTAMP   NOT NULL DEFAULT NOW()
);
