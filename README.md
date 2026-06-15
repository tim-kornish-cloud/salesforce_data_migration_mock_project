# Salesforce Data Migration using fake data from Mocharoo.com

## Table of Contents
* [File Structure](#File-Structure)
  * [Source Data And Source Data Setup](#Source-Data-And-Source-Data-Setup)
  * [Source Database](#Source-Database)
  * [Staging Database](#Staging-Database)
  * [Migration Execution Scripts](#Migration-Execution-Scripts)
  * [Migration Backout Delete Scripts](#Migration-Backout-Delete-Scripts)
  * [Output and Reporting](#Output-and-Reporting)
  * [Database Utilities Class](#Database-Utilities-Class)
  * [Install Dependencies](#Install-Dependencies)
* [Create mock data from Mockaroo](#Create-mock-data-from-Mockaroo)
    * [Following Objects](#Following-Objects)
    * [Create relationships for](#Create-relationships-for)
* [Create Source MySQL Database](#Create-Source-MySQL-Database)
* [Set up intermediary staging database on MSSQL tables](#Set-up-intermediary-staging-database-on-MSSQL-tables)
* [Set up Salesforce Environment with Salesforce CPQ](#Set-up-Salesforce-Environment-with-Salesforce-CPQ)
* [Add custom fields to objects in Salesforce to track migrated records](#Add-custom-fields-to-objects-in-Salesforce-to-track-migrated-records)

## File Structure
### Source Data And Source Data Setup
  - MockData/
    - holds raw csv data downloaded from Mockaroo.com
  - source_data_setup/
    - holds three python scripts to create relationships in the raw data before loading into MySQL.
      - align_contacts_to_accounts.py
      - align_contracts_to_accounts.py
      - generate_contract_lines.py
### Source Database
  - MySQL_source_tables/
    - holds four SQL scripts each to create a single table to hold source data.
    - create_account_table.SQL
    - create_contacts_table.SQL
    - create_contract_lines_table.SQL
    - create_contracts_table.SQL
  - Execute four scripts to load csv data into source DB.
  1. load_mock_account_data_to_source_mysql.py
  2. load_mock_contact_data_to_source_mysql.py
  3. load_mock_contract_data_to_source_mysql.py
  4. load_mock_contract_line_data_to_source_mysql.py
### Staging Database
  - MSSQL_staging_tables/
    - holds four SQL scripts that mirror the source tables to set up in MSSQL tables
    - create_source_account_staging.SQL
    - create_source_contact_staging.SQL
    - create_source_contract_staging.SQL
    - create_source_contract_lines_staging.SQL
  - Execute four scripts to migrate source data from MySQL to stagin MSSQL DB.
  1. migrate_source_account_data_to_staging_mssql.py
  2. migrate_source_contact_data_to_staging_mssql.py
  3. migrate_source_contract_data_to_staging_mssql.py
  4. migrate_source_contract_line_data_to_staging_mssql.py
### Migration Execution Scripts
  1. 101_insert_sf_accounts.py
  2. 102_insert_sf_contacts.py
  3. 103_insert_sf_opportunities.py
  - prior to executing script 104_insert_sf_quotes.py, turn off CPQ triggers
  4. 104_insert_sf_quotes.py
  5. 105_insert_sf_quote_lines.py
  - prior to executing script 106_update_sf_quotes.py, turn on CPQ triggers
  6. 106_update_sf_quotes.py
  7. 107_update_sf_orders_activate_order.py
  8. 108_update_sf_orders_contract_order.py
### Migration Backout-Delete Scripts
  - Scripts must be executed in descending order, 206_delete_sf_orders.py is deprecated, redundant after running 207_delete_sf_order_lines.py.
  1. 209_delete_sf_subscriptions.py
  2. 208_delete_sf_contracts.py
  3. 207_delete_sf_order_lines.py
  - ~~206_delete_sf_orders.py~~
  4. 205_delete_sf_quote_lines.py
  5. 204_delete_sf_quotes.py
  6. 203_delete_sf_opportunities.py
  6. 202_delete_sf_contacts.py
  7. 201_delete_sf_accounts.py
### Output and Reporting
  - Each folder holds success and fallout files for the stated DML operations
    - DELETE/
    - INESRT/
    - UPDATE/
### Database Utilities Class
- custom_db_utilities.py
  - Holds functions for pulling data from several Database as well as perform INSERT, DELETE, and UPDATE calls. Used to load data from CSV to MySQL, from MySQL to MSSQL, and from MSSQL to Salesforce.

### Install Dependencies
- requirements.txt
  - Enter below command to install all dependecies:
  ```bash
  pip install -r requirements.txt
  ```

## Create mock data from Mockaroo
### Following Objects
- Account
- Contact
- Contract
- Contract line.
### Create relationships for
- Account->Contact
- Account->Contract
- Contract->Contract line

## Create Source MySQL Database
- Create tables for accounts, Contacts, Contracts, Contract Lines.
- Load newly associated csv mock data into source MySQL database tables

## Set up intermediary staging database on MSSQL tables
- Migrate source data from MySQL to staging MSSQL tables that are 1-1 reflections of source tables in MySQL.

## Set up Salesforce Environment with Salesforce CPQ
- Enable SOAP API Login() in User Interface in setup
- Configure CPQ package settings as desired.

## Add custom fields to objects in Salesforce to track migrated records
- Accounts
  - Migrated_Record__c
  - Account_Number_External_ID__c
- Contacts
  - Migrated_Record__c
  - Contact_External_ID__c
- Opportunity2
  - Migrated_Record__c
  - Opportunity_External_ID__c
- SBQQ__Quote__c
  - Migrated_Record__c
  - Quote_External_ID__c
- SBQQ__QuoteLine__c
  - Migrated_Record__c
  - QuoteLine_External_ID__c
