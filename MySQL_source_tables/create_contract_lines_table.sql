USE data_engineering;
DROP TABLE IF EXISTS contract_lines;
CREATE TABLE contract_lines (
  contract_line_number varchar(36) PRIMARY KEY,
  contract_number varchar(36) NOT NULL,
  product varchar(50) NOT NULL,
  quantity INT NOT NULL
);
