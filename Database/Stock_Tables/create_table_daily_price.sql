CREATE TABLE daily_price (
    ID                INT NOT NULL AUTO_INCREMENT,
    Data_vendor_id    INT NOT NULL,
    Symbol_id         INT NOT NULL,
    Price_date        DATETIME NOT NULL,
    Created_date      DATETIME NOT NULL,
    Last_updated_date DATETIME NOT NULL,
    Open_price        NUMERIC NULL,
    High_price        NUMERIC NULL,
    Low_price         NUMERIC NULL,
    Close_price       NUMERIC NULL,
    Adj_close_price   NUMERIC NULL,
    Volume            BIGINT  NULL,
    PRIMARY KEY (ID),
    KEY (Data_vendor_id),
    KEY (Symbol_id)
)   ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;