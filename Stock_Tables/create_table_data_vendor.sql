CREATE TABLE data_vendor (
    ID                INT NOT NULL AUTO_INCREMENT,
    Name              VARCHAR(64) NOT NULL,
    Website_url       VARCHAR(255) NULL,
    Support_email     VARCHAR(255) NULL,
    Created_date      DATETIME NOT NULL,
    Last_updated_date DATETIME NOT NULL,
    PRIMARY KEY (ID)
)   ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;