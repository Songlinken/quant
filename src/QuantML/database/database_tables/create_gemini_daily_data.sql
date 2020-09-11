CREATE TABLE GEMINI_DAILY_DATA (
    EVENT_DATE                  DATE         NOT NULL,
    SYMBOL                      VARCHAR(10)  NOT NULL,
    OPEN_PRICE                  NUMERIC      NOT NULL,
    CLOSE_PRICE                 NUMERIC      NOT NULL,
    HIGH_PRICE                  NUMERIC      NOT NULL,
    LOW_PRICE                   NUMERIC      NOT NULL,
    VOLUME_FROM                 NUMERIC      NOT NULL,
    VOLUME_TO                   NUMERIC      NOT NULL,
    PRIMARY KEY (EVENT_DATE, SYMBOL)
);

CREATE INDEX idx_gemini_daily_data ON GEMINI_DAILY_DATA (SYMBOL);