USE data_engineering;
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
  phone varchar(12) NOT NULL,
  company_name varchar(100) NOT NULL,
  industry varchar(50) NOT NULL,
  annual_revenue decimal(16,2) NOT NULL,
  account_number_external_id varchar(36) PRIMARY KEY,
  number_of_locations INT NOT NULL,
  number_of_employees INT NOT NULL,
  sla varchar(20) NOT NULL,
  sla_serial_number INT NOT NULL
);
