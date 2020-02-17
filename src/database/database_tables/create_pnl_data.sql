CREATE TABLE PNL_DATA (
    ACCOUNT_ID                  INT         NOT NULL,
    EVENT_ID                    BIGINT      NOT NULL,
    EVENT_DATE                  DATE        NOT NULL,
    CREATED                     TIMESTAMP   NOT NULL,
    TRADING_PAIR                VARCHAR(6)  NOT NULL,
    SIDE                        VARCHAR(30) NOT NULL,
    ORDER_BOOK                  VARCHAR(6)  NOT NULL,
    QUANTITY                    NUMERIC,
    PRICE                       NUMERIC,
    PNL                         NUMERIC,
    UNPNL                       NUMERIC,
    QUEUES                      TEXT,
    PRIMARY KEY (ACCOUNT_ID, EVENT_ID, ORDER_BOOK)
);

CREATE INDEX idx_pnl_data_1 ON PNL_DATA (ORDER_BOOK);
CREATE INDEX idx_pnl_data_2 ON PNL_DATA (EVENT_DATE);