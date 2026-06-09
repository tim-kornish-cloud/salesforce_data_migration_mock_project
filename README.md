# Salesforce Data Migration using fake data from Mocharoo.com

## create mock data from Mockaroo
###Following Objects:
- accounts
- contacts
- contracts
- contract lines.
### Create relationships from:
- Account->Contacts
- Account->Contract
- Contract->Contract lines

## Create MySQL Database with tables for accounts, Contacts, Contracts, Contract Lines.
- Load newly associated csv mock data into source MySQL database tables

## Set up intermediary staging database on MSSQL tables.
- Migrate source data from MySQL to staging MSSQL tables that are 1-1 reflections of source tables in MySQL.

## Set up Salesforce Environment with Salesforce CPQ.
- Enable SOAP API Login() in User Interface in setup
- Configure CPQ package settings as desired.

## Add custom fields to objects in Salesforce to track migrated records
- Accounts
  - Migrated_Record__c
  - Account_Number_External_ID__c
- Contacts
- Opportunity2
- SBQQ__Quote__c
- SBQQ__QuoteLine__c
