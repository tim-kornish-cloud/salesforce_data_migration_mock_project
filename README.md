# Salesforce Data Migration using fake data from Mocharoo.com

## Table of Contents
* [File Structure](#File-Structure)
* [Create mock data from Mockaroo](#Create-mock-data-from-Mockaroo)
    * [Following Objects:](#Following Objects:)
    * [Create relationships from:](Create relationships from:)
* [Create Source MySQL Database](#Create-Source-MySQL-Database)
* [Set up intermediary staging database on MSSQL tables](#Set-up-intermediary-staging-database-on-MSSQL-tables)
* [Set up Salesforce Environment with Salesforce CPQ](#Set up Salesforce Environment with Salesforce CPQ)
* [Add custom fields to objects in Salesforce to track migrated records](# Add-custom-fields-to-objects-in-Salesforce-to-track-migrated-records)

## File Structure

## Create mock data from Mockaroo
### Following Objects:
- Account
- Contact
- Contract
- Contract line.
### Create relationships from:
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
