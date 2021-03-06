CREATE TABLE DAILY_BEST_PRICES_SPLIT_BY_ACCOUNT (
    ACCOUNT_ID                          INT         NOT NULL,
    EVENT_ID                            BIGINT      NOT NULL,
    EVENT_DATE                          DATE        NOT NULL,
    EVENT_TYPE                          VARCHAR(10) NOT NULL,
    DATE_TIME                           TIMESTAMP   NOT NULL,
    SYMBOL                              CHAR(6)     NOT NULL,
    SIDE                                VARCHAR(4)  NOT NULL,
    BEST_PRICE_1                        NUMERIC,
    BEST_PRICE_2                        NUMERIC,
    BEST_PRICE_3                        NUMERIC,
    BEST_PRICE_4                        NUMERIC,
    BEST_PRICE_5                        NUMERIC,
    BEST_PRICE_6                        NUMERIC,
    BEST_PRICE_7                        NUMERIC,
    BEST_PRICE_8                        NUMERIC,
    BEST_PRICE_9                        NUMERIC,
    BEST_PRICE_10                       NUMERIC,
    BEST_ACCOUNT_VOLUME_1               VARCHAR,
    BEST_ACCOUNT_VOLUME_2               VARCHAR,
    BEST_ACCOUNT_VOLUME_3               VARCHAR,
    BEST_ACCOUNT_VOLUME_4               VARCHAR,
    BEST_ACCOUNT_VOLUME_5               VARCHAR,
    BEST_ACCOUNT_VOLUME_6               VARCHAR,
    BEST_ACCOUNT_VOLUME_7               VARCHAR,
    BEST_ACCOUNT_VOLUME_8               VARCHAR,
    BEST_ACCOUNT_VOLUME_9               VARCHAR,
    BEST_ACCOUNT_VOLUME_10              VARCHAR,
    BEST_PRICE_1_OTHER_SIDE             NUMERIC,
    BEST_PRICE_2_OTHER_SIDE             NUMERIC,
    BEST_PRICE_3_OTHER_SIDE             NUMERIC,
    BEST_PRICE_4_OTHER_SIDE             NUMERIC,
    BEST_PRICE_5_OTHER_SIDE             NUMERIC,
    BEST_PRICE_6_OTHER_SIDE             NUMERIC,
    BEST_PRICE_7_OTHER_SIDE             NUMERIC,
    BEST_PRICE_8_OTHER_SIDE             NUMERIC,
    BEST_PRICE_9_OTHER_SIDE             NUMERIC,
    BEST_PRICE_10_OTHER_SIDE            NUMERIC,
    BEST_ACCOUNT_VOLUME_1_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_2_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_3_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_4_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_5_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_6_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_7_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_8_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_9_OTHER_SIDE    VARCHAR,
    BEST_ACCOUNT_VOLUME_10_OTHER_SIDE   VARCHAR,
    PRIMARY KEY (ACCOUNT_ID, EVENT_ID, EVENT_DATE)
);

CREATE INDEX idx_account_daily_best_prices_split_by_account_1 ON DAILY_BEST_PRICES_SPLIT_BY_ACCOUNT (SYMBOL);
CREATE INDEX idx_account_daily_best_prices_split_by_account_2 ON DAILY_BEST_PRICES_SPLIT_BY_ACCOUNT (EVENT_DATE);