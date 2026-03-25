USE data_engineering;
DROP TABLE IF EXISTS contacts;
CREATE TABLE contacts (
  account_number_external_id varchar(36),
  first_name varchar(200) NOT NULL,
  last_name varchar(200) NOT NULL,
  email varchar(200) NOT NULL,
  title varchar(200) NOT NULL,
  department varchar(200) NOT NULL,
  languages varchar(200) NOT NULL,
  PRIMARY KEY (first_name, last_name)
);
