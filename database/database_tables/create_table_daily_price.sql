CREATE TABLE daily_crypto_price (
    Price_id          INT NOT NULL AUTO_INCREMENT,
    Instrument        Char(10) NOT NULL,
    Price_date        DATE NOT NULL,
    Open_price        Decimal(20, 8) NULL,
    High_price        Decimal(20, 8) NULL,
    Low_price         Decimal(20, 8) NULL,
    Close_price       Decimal(20, 8) NULL,
    Market_price_cap  Decimal(20, 8) NULL,
    Volume            Decimal(20, 8) NULL,
    Timezone          Char(5) NULL,
    PRIMARY KEY (Price_id)
)   ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;


CREATE INDEX Price_date ON daily_crypto_price(Price_date);