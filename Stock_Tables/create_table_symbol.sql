CREATE TABLE symbol (
    ID                INT NOT NULL AUTO_INCREMENT,
    Exchange_id       INT NULL,
    Ticker            VARCHAR(32) NOT NULL,
    Instrument        VARCHAR(64) NOT NULL,
    Name              VARCHAR(255) NOT NULL,
    Sector            VARCHAR(255) NULL,
    Currency          VARCHAR(32) NULL,
    Created_date      DATETIME NOT NULL,
    Last_updated_date DATETIME NOT NULL,
    PRIMARY KEY (ID),
    KEY (Exchange_id)
)   ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;