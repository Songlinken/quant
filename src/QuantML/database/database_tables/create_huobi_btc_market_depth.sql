CREATE TABLE HUOBI_BTC_MARKET_DEPTH(
    DEFAULT_ID                  BIGSERIAL    PRIMARY KEY,
    EVENT_DATE                  DATE         NOT NULL,
    EVENT_TIME                  TIMESTAMP    NOT NULL,
    SYMBOL                      VARCHAR(15)  NOT NULL,
    ASK_PRICE_1                 NUMERIC,
    ASK_PRICE_2                 NUMERIC,
    ASK_PRICE_3                 NUMERIC,
    ASK_PRICE_4                 NUMERIC,
    ASK_PRICE_5                 NUMERIC,
    ASK_PRICE_6                 NUMERIC,
    ASK_PRICE_7                 NUMERIC,
    ASK_PRICE_8                 NUMERIC,
    ASK_PRICE_9                 NUMERIC,
    ASK_PRICE_10                NUMERIC,
    ASK_PRICE_11                NUMERIC,
    ASK_PRICE_12                NUMERIC,
    ASK_PRICE_13                NUMERIC,
    ASK_PRICE_14                NUMERIC,
    ASK_PRICE_15                NUMERIC,
    ASK_PRICE_16                NUMERIC,
    ASK_PRICE_17                NUMERIC,
    ASK_PRICE_18                NUMERIC,
    ASK_PRICE_19                NUMERIC,
    ASK_PRICE_20                NUMERIC,
    ASK_VOLUME_1                NUMERIC,
    ASK_VOLUME_2                NUMERIC,
    ASK_VOLUME_3                NUMERIC,
    ASK_VOLUME_4                NUMERIC,
    ASK_VOLUME_5                NUMERIC,
    ASK_VOLUME_6                NUMERIC,
    ASK_VOLUME_7                NUMERIC,
    ASK_VOLUME_8                NUMERIC,
    ASK_VOLUME_9                NUMERIC,
    ASK_VOLUME_10               NUMERIC,
    ASK_VOLUME_11               NUMERIC,
    ASK_VOLUME_12               NUMERIC,
    ASK_VOLUME_13               NUMERIC,
    ASK_VOLUME_14               NUMERIC,
    ASK_VOLUME_15               NUMERIC,
    ASK_VOLUME_16               NUMERIC,
    ASK_VOLUME_17               NUMERIC,
    ASK_VOLUME_18               NUMERIC,
    ASK_VOLUME_19               NUMERIC,
    ASK_VOLUME_20               NUMERIC,
    BID_PRICE_1                 NUMERIC,
    BID_PRICE_2                 NUMERIC,
    BID_PRICE_3                 NUMERIC,
    BID_PRICE_4                 NUMERIC,
    BID_PRICE_5                 NUMERIC,
    BID_PRICE_6                 NUMERIC,
    BID_PRICE_7                 NUMERIC,
    BID_PRICE_8                 NUMERIC,
    BID_PRICE_9                 NUMERIC,
    BID_PRICE_10                NUMERIC,
    BID_PRICE_11                NUMERIC,
    BID_PRICE_12                NUMERIC,
    BID_PRICE_13                NUMERIC,
    BID_PRICE_14                NUMERIC,
    BID_PRICE_15                NUMERIC,
    BID_PRICE_16                NUMERIC,
    BID_PRICE_17                NUMERIC,
    BID_PRICE_18                NUMERIC,
    BID_PRICE_19                NUMERIC,
    BID_PRICE_20                NUMERIC,
    BID_VOLUME_1                NUMERIC,
    BID_VOLUME_2                NUMERIC,
    BID_VOLUME_3                NUMERIC,
    BID_VOLUME_4                NUMERIC,
    BID_VOLUME_5                NUMERIC,
    BID_VOLUME_6                NUMERIC,
    BID_VOLUME_7                NUMERIC,
    BID_VOLUME_8                NUMERIC,
    BID_VOLUME_9                NUMERIC,
    BID_VOLUME_10               NUMERIC,
    BID_VOLUME_11               NUMERIC,
    BID_VOLUME_12               NUMERIC,
    BID_VOLUME_13               NUMERIC,
    BID_VOLUME_14               NUMERIC,
    BID_VOLUME_15               NUMERIC,
    BID_VOLUME_16               NUMERIC,
    BID_VOLUME_17               NUMERIC,
    BID_VOLUME_18               NUMERIC,
    BID_VOLUME_19               NUMERIC,
    BID_VOLUME_20               NUMERIC
);

CREATE INDEX idx_huobi_btc_market_depth_1 ON HUOBI_BTC_MARKET_DEPTH (EVENT_DATE);
CREATE INDEX idx_huobi_btc_market_depth_2 ON HUOBI_BTC_MARKET_DEPTH (SYMBOL);