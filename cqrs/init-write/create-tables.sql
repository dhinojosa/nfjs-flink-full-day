CREATE TABLE stock_trade
(
    id              SERIAL PRIMARY KEY,
    stock_symbol    VARCHAR(10)                                      NOT NULL,
    trade_timestamp TIMESTAMP                                        NOT NULL,
    trade_type      VARCHAR(4) CHECK (trade_type IN ('buy', 'sell')) NOT NULL,
    amount          NUMERIC(10, 2)                                   NOT NULL
);
