CREATE TABLE daily_crypto_price (
    Price_id          INT NOT NULL AUTO_INCREMENT,
    Instrument        Char(10) NOT NULL,
    Price_date        DATE NOT NULL,
    Open_price        NUMERIC NULL,
    High_price        NUMERIC NULL,
    Low_price         NUMERIC NULL,
    Close_price       NUMERIC NULL,
    Market_price_cap  NUMERIC NULL,
    Volume            NUMERIC NULL,
    Timezone          Char(5) NULL,
    PRIMARY KEY (Price_id)
)   ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE INDEX Price_date ON daily_crypto_price(Price_date);