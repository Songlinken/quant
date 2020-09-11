CREATE TABLE HUOBI_BTC_MARKET_TRADE(
    DEFAULT_ID                  BIGSERIAL    PRIMARY KEY,
    TRADE_ID                    BIGINT       NOT NULL,
    EVENT_DATE                  DATE         NOT NULL,
    EVENT_TIME                  TIMESTAMP    NOT NULL,
    SYMBOL                      VARCHAR(15)  NOT NULL,
    SIDE                        VARCHAR(10)  NOT NULL,
    PRICE                       NUMERIC,
    QUANTITY                    NUMERIC
);

CREATE INDEX idx_huobi_btc_market_trade_1 ON HUOBI_BTC_MARKET_TRADE (EVENT_DATE);
CREATE INDEX idx_huobi_btc_market_trade_2 ON HUOBI_BTC_MARKET_TRADE (SYMBOL);