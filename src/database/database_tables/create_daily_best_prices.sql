CREATE TABLE DAILY_BEST_PRICES (
    ACCOUNT_ID                  INT         NOT NULL,
    EVENT_ID                    BIGINT      NOT NULL,
    EVENT_DATE                  DATE        NOT NULL,
    EVENT_TYPE                  VARCHAR(10) NOT NULL,
    DATE_TIME                   TIMESTAMP   NOT NULL,
    SYMBOL                      CHAR(6)     NOT NULL,
    SIDE                        VARCHAR(4)  NOT NULL,
    BEST_PRICE_1                NUMERIC,
    BEST_PRICE_2                NUMERIC,
    BEST_PRICE_3                NUMERIC,
    BEST_VOLUME_1               NUMERIC,
    BEST_VOLUME_2               NUMERIC,
    BEST_VOLUME_3               NUMERIC,
    PRIMARY KEY (ACCOUNT_ID, EVENT_ID, EVENT_DATE)
);

CREATE INDEX idx_account_daily_best_prices_1 ON DAILY_BEST_PRICES (SYMBOL);
CREATE INDEX idx_account_daily_best_prices_2 ON DAILY_BEST_PRICES (EVENT_DATE);