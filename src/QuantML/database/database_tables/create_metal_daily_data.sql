CREATE TABLE METAL_DAILY_DATA (
    EVENT_DATE                  DATE         NOT NULL,
    METAL                       VARCHAR(10)  NOT NULL,
    UNIT                        VARCHAR(10)  NOT NULL,
    CLOSE_PRICE                 NUMERIC      NOT NULL,
    PRIMARY KEY (EVENT_DATE, METAL, UNIT)
);

CREATE INDEX idx_metal_daily_data ON METAL_DAILY_DATA (METAL);