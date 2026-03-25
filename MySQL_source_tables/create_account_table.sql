USE data_engineering;
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
  phone varchar(12) NULL,
  company_name varchar(200) NULL,
  industry varchar(200) NULL,
  annual_revenue decimal(16,2) NULL,
  account_number_external_id varchar(36) PRIMARY KEY,
  number_of_locations INT,
  number_of_employees INT,
  sla varchar(20) NULL,
  sla_serial_number INT 
);
