USE data_engineering;
DROP TABLE IF EXISTS contacts;
CREATE TABLE contacts (
  account_number_external_id varchar(36),
  first_name varchar(50) NOT NULL,
  last_name varchar(50) NOT NULL,
  email varchar(50) NOT NULL,
  title varchar(50) NOT NULL,
  department varchar(50) NOT NULL,
  languages varchar(50) NOT NULL,
  PRIMARY KEY (first_name, last_name)
);
