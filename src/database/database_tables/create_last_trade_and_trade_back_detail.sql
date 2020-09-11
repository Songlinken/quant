CREATE TABLE LAST_TRADE_AND_TRADE_BACK_DETAIL (
    STORED_TYPE                 CHAR(1)     NOT NULL,
    RELEASE_VERSION             INT         NOT NULL,
    ACCOUNT_ID                  INT         NOT NULL,
    EVENT_ID                    BIGINT      NOT NULL,
    EVENT_DATE                  DATE        NOT NULL,
    EVENT_TYPE                  VARCHAR(10) NOT NULL,
    DATE_TIME                   TIMESTAMP   NOT NULL,
    SYMBOL                      CHAR(6)     NOT NULL,
    SIDE                        VARCHAR(4)  NOT NULL,
    LAST_TRADED_PRICE           NUMERIC,
    PRICE_CHANGE_PCT            NUMERIC,
    TIME_FROM_LAST_TRADE        INTERVAL,
    TRADE_VALUE                 NUMERIC,
    COUNTER_PARTY_ACCOUNT_ID    NUMERIC,
    TRADE_BACK_TIME             TIMESTAMP,
    TRADE_BACK_PRICE            NUMERIC,
    TRADE_TIME_RANGE            INTERVAL,
    MONEY_PASS                  NUMERIC,
    PRIMARY KEY (STORED_TYPE, RELEASE_VERSION, ACCOUNT_ID, EVENT_ID, EVENT_DATE)
);

CREATE INDEX idx_last_trade_and_trade_back_detail_1 ON LAST_TRADE_AND_TRADE_BACK_DETAIL (SYMBOL);
CREATE INDEX idx_last_trade_and_trade_back_detail_2 ON LAST_TRADE_AND_TRADE_BACK_DETAIL (EVENT_DATE);
CREATE INDEX idx_last_trade_and_trade_back_detail_3 ON LAST_TRADE_AND_TRADE_BACK_DETAIL (ACCOUNT_ID);