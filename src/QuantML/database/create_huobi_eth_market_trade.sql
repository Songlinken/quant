CREATE TABLE HUOBI_ETH_MARKET_TRADE(
    TRADE_ID                    BIGINT    PRIMARY KEY,
    EVENT_DATE                  DATE         NOT NULL,
    EVENT_TIME                  TIMESTAMP    NOT NULL,
    SYMBOL                      VARCHAR(15)  NOT NULL,
    SIDE                        VARCHAR(10)  NOT NULL,
    PRICE                       NUMERIC,
    QUANTITY                    NUMERIC
);

CREATE INDEX idx_huobi_eth_market_trade_1 ON HUOBI_ETH_MARKET_TRADE (EVENT_DATE);
CREATE INDEX idx_huobi_eth_market_trade_2 ON HUOBI_ETH_MARKET_TRADE (SYMBOL);