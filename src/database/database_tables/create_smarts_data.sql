CREATE TABLE SMARTS_DATA (
    ACCOUNT_ID                  INT         NOT NULL,
    EVENT_ID                    BIGINT      NOT NULL,
    DATA_FROM_DATE              DATE        NOT NULL,
    EVENT_DATE                  DATE        NOT NULL,
    DATE_TIME                   TIMESTAMP   NOT NULL,
    EVENT_TIME                  TIME        NOT NULL,
    EVENT_MILLIS                INTERVAL    NOT NULL,
    EVENT_TYPE                  VARCHAR(10) NOT NULL,
    SIDE                        VARCHAR(4)  NOT NULL,
    SYMBOL                      CHAR(6)     NOT NULL,
    AUCTION_ID                  INT,
    EXECUTION_OPTIONS           VARCHAR(30),
    ORDER_ID                    BIGINT,
    ORDER_TYPE                  VARCHAR(10),
    LIMIT_PRICE                 NUMERIC,
    ORIGINAL_QUANTITY           NUMERIC,
    GROSS_NOTIONAL_VALUE        NUMERIC,
    FILL_PRICE                  NUMERIC,
    FILL_QUANTITY               NUMERIC,
    TOTAL_EXEC_QUANTITY         NUMERIC,
    REMAINING_QUANTITY          NUMERIC,
    AVG_PRICE                   NUMERIC,
    FEES                        NUMERIC,
    IOI_ID                      BIGINT,
    ORDER_CANCEL_REASON         VARCHAR(50),
    PRIMARY KEY (ACCOUNT_ID, EVENT_ID, DATA_FROM_DATE)
);

CREATE INDEX idx_smarts_data_1 ON SMARTS_DATA (SYMBOL);
CREATE INDEX idx_smarts_data_2 ON SMARTS_DATA (DATA_FROM_DATE);