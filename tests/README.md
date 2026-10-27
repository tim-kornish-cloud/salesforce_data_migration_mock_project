# The data validation tests are held here

## test_stage_source_data.py
This script holds test functions to validate the data migrated from the source MySQL Database into the staging MSSQL database.
These tests verify the following:
 - the data type of each column
 - the data value in each column per record
 - The number of records staged per record type

The script holds 4 functions for verifying:
- Accounts  
  - test_account_tables
- Contacts
  - test_contact_tables
- Contracts
  - test_contract_tables
- Contract Lines
  - test_contract_lines_tables

## test_migrated_records_into_sf.py
This script holds test functions to validate the data migrated from the staging MSSQL into Salesforce Objects.
These tests verify the following:
 - the data type of each field
 - the data value in each field per object type
 - The number of records migrated into each object type

The script holds 4 functions for verifying:
- Accounts  
  - test_migrated_accounts
- Contacts
  - test_migrated_contacts
- Opportunities
  - test_migrated_opportunities
- SBQQ__Quote__c
  - test_migrated_quotes_before_update
  - test_migrated_quotes_after_update
- SBQQ__QuoteLine__c
  - test_migrated_quote_lines_before_update
  - test_migrated_quote_lines_after_update
- Order
  - test_migrated_orders_after_update
- OrderItem
    - test_migrated_order_lines_after_update
- Contracts
  - test_migrated_contracts_after_update
- SBQQ__Subscription__c
  - test_migrated_contract_lines_after_update


### pytest commands
  - python -m pytest tests/test_stage_source_data.py -v
  - python -m pytest tests/test_stage_source_data.py -v -p no:warnings
  - python -m pytest tests/test_migrated_records_into_sf.py::test_migrated_accounts -v -p no:warning
  - python -m pytest tests/test_migrated_records_into_sf.py::test_migrated_accounts -v -p no:warning -rP
  - python -m pytest tests/ -v
  - python -m pytest tests/
  - python -m pytest tests/test_stage_source_data.py::test_account_tables -v
