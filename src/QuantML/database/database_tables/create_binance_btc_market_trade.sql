CREATE TABLE BINANCE_BTC_MARKET_TRADE(
    DEFAULT_ID                  BIGSERIAL    PRIMARY KEY,
    EVENT_TIME                  TIMESTAMP    NOT NULL,
    TRADE_TIME                  TIMESTAMP    NOT NULL,
    SYMBOL                      VARCHAR(15)  NOT NULL,
    PRICE                       NUMERIC,
    QUANTITY                    NUMERIC,
    IS_BUYER_MAKER              BOOLEAN
);

CREATE INDEX idx_binance_btc_market_trade_1 ON BINANCE_BTC_MARKET_DEPTH (TRADE_TIME);
CREATE INDEX idx_binance_btc_market_trade_2 ON BINANCE_BTC_MARKET_DEPTH (SYMBOL);