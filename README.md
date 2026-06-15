# Salesforce Data Migration using fake data from Mocharoo.com

## Table of Contents
* [File Structure](#File-Structure)
  * [Source Data And Source Data Setup](#Source-Data-And-Source-Data-Setup)
  * [Source Database](#Source-Database)
  * [Staging Database](#Staging-Database)
  * [Migration Execution Scripts](#Migration-Execution-Scripts)
  * [Migration Backout Delete Scripts](#Migration-Backout-Delete-Scripts)
  * [Output and Reporting](#Output-and-Reporting)
* [Create mock data from Mockaroo](#Create-mock-data-from-Mockaroo)
    * [Following Objects](#Following-Objects)
    * [Create relationships for](#Create-relationships-for)
* [Create Source MySQL Database](#Create-Source-MySQL-Database)
* [Set up intermediary staging database on MSSQL tables](#Set-up-intermediary-staging-database-on-MSSQL-tables)
* [Set up Salesforce Environment with Salesforce CPQ](#Set-up-Salesforce-Environment-with-Salesforce-CPQ)
* [Add custom fields to objects in Salesforce to track migrated records](#Add-custom-fields-to-objects-in-Salesforce-to-track-migrated-records)
* [Script Execution Order](#Script-Execution-Order)

## File Structure
### Source Data And Source Data Setup
  - MockData/
    - holds raw csv data downloaded from Mockaroo.com
  - source_data_setup/
    - holds three python scripts to create relationships in the raw data before loading into MySQL.
### Source Database
  - MySQL_source_tables/
    - holds four SQL scripts each to create a single table to hold source data.
### Staging Database
  - MSSQL_staging_tables/
    - holds four SQL scripts that mirror the source tables to set up in MSSQL tables
### Migration Execution Scripts
  - 101_insert_sf_accounts.py
  - 102_insert_sf_contacts.py
  - 103_insert_sf_opportunities.py
  - 104_insert_sf_quotes.py
  - 105_insert_sf_quote_lines.py
  - 106_update_sf_quotes.py
  - 107_update_sf_orders_activate_order.py
  - 108_update_sf_orders_contract_order.py
### Migration Backout-Delete Scripts
  - Scripts must be executed in descending order, 206_delete_sf_orders.py is deprecated.
  - 209_delete_sf_subscriptions.py
  - 208_delete_sf_contracts.py
  - 207_delete_sf_order_lines.py
  - 206_delete_sf_orders.py
  - 205_delete_sf_quote_lines.py
  - 204_delete_sf_quotes.py
  - 203_delete_sf_opportunities.py
  - 202_delete_sf_contacts.py
  - 201_delete_sf_accounts.py
### Output and Reporting
  - Each fold holds success and fallout files for the stated DML operations
    - DELETE/
    - INESRT/
    - UPDATE/

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

## Script Execution Order
