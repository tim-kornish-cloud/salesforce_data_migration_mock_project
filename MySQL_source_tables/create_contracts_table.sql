USE data_engineering;
DROP TABLE IF EXISTS contracts;
CREATE TABLE contracts (
  account_number_external_id varchar(36) NOT NULL,
  contract_number varchar(36) PRIMARY KEY,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL
);
