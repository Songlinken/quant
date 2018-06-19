CREATE TABLE exchange (
    ID                INT NOT NULL AUTO_INCREMENT,
    Abbrev            VARCHAR(32) NOT NULL,
    Name              VARCHAR(255) NOT NULL,
    City              VARCHAR(255) NULL,
    Country           VARCHAR(255) NULL,
    Currency          VARCHAR(64) NULL,
    Timezone_offset   TIME NULL,
    Created_date      DATETIME NOT NULL,
    Last_updated_date DATETIME NOT NULL,
    PRIMARY KEY (ID)
)   ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;