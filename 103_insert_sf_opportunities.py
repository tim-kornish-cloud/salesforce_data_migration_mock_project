"""
Author: Timothy Kornish
CreatedDate: June - 11 - 2026
Description: Log into mssql server, query contract staging table records.
             Log into salesforce and query existing accounts.
             Merge SF.accounts.Account_Number_External_ID__c = MSSQL.Contract.Account_Number_External_ID__c
             from both systems to determine what related records exist
             in both systems, which records are net new, and which are missing prior attempted accounts, or other issues.
             Insert net new records into salesforce contact table.
"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MSSQL_Utilities, Salesforce_Utilities, Custom_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# Set option to display all columns
pd.set_option('display.max_columns', None)

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'
database = 'mssql'
object = 'Opportunity'
#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + "_" + object + "_" + database + ".csv"

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """ SELECT [account_number_external_id]
      ,[contract_number]
      ,[start_date]
      ,[end_date]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]"""

# accounts in the mssql table shown in the query above
stg_contract_df = MSSQL_Utils.query_mssql_return_dataframe(select_query, cursor)

# get credentials for salesforce login
# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
sf_environment = 'Dev'
# set database to Salesforce
sf_database = "Salesforce"

# get username from credentials
sf_username = Cred.get_username(sf_database, sf_environment)
# get password from credentials
sf_password = Cred.get_password(sf_database, sf_environment)
# get login token from credentials
sf_token = Cred.get_token(sf_database, sf_environment)

# create a instance of simple_salesforce to query and perform operations against salesforce with
sf = SF_Utils.login_to_salesForce(sf_username, sf_password, sf_token)

# query existing accounts from salesforce
# query string to select records from salesforce
account_query = "SELECT Id, Account_Number_External_ID__c FROM Account WHERE Migrated_Record__c = True"
# query salesforce and return the accounts to be deleted
account_query_results = SF_Utils.query_salesforce(sf, account_query)

# convert query results to a dataframe
sf_accounts_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
# encode the dataframe before uploading to delete
sf_accounts_df = Utils.encode_df(sf_accounts_df)

# perform merge of staging accounts and salesforce accounts
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_accounts_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV accounts
    contracts_with_accounts_df, sf_only_accounts, mssql_contracts_only_df = Utils.get_df_diffs(sf_accounts_df, stg_contract_df, left_on = ['Account_Number_External_ID__c'], right_on = ['account_number_external_id'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # remove unneccessary column for insert
    contracts_with_accounts_df.drop(["account_number_external_id",  "Account_Number_External_ID__c", "_merge"], axis = 1, inplace = True)

# query Standard Pricebook from salesforce
# query string to select records from salesforce
Pricebook2_query = "SELECT Id, IsStandard, Name FROM Pricebook2 WHERE IsStandard = True"
# query salesforce and return the accounts to be deleted
Pricebook2_query_results = SF_Utils.query_salesforce(sf, Pricebook2_query)

# convert query results to a dataframe
sf_Pricebook2_df = SF_Utils.load_query_with_lookups_into_dataframe(Pricebook2_query_results)
# encode the dataframe before uploading to delete
sf_Pricebook2_df = Utils.encode_df(sf_Pricebook2_df)

contracts_with_accounts_df["Pricebook2Id"] = sf_Pricebook2_df.iloc[0,0]

# query existing Contacts from salesforce
# query string to select records from salesforce
opportunity_query = "SELECT Id, Opportunity_External_ID__c FROM Opportunity WHERE Migrated_Record__c = True"
# query salesforce and return the opportunitys to be deleted
opportunity_query_results = SF_Utils.query_salesforce(sf, opportunity_query)

# convert query results to a dataframe
sf_opportunity_df = SF_Utils.load_query_with_lookups_into_dataframe(opportunity_query_results)
# encode the dataframe before uploading to delete
sf_opportunity_df = Utils.encode_df(sf_opportunity_df)


# print(sf_opportunity_df.head())
# print(contracts_with_accounts_df.head())

# perform merge of staging contracts and salesforce opportunitys
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_opportunity_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV Contracts
    both_df, sf_opportunity_only_df, opportunity_to_insert_df = Utils.get_df_diffs(sf_opportunity_df, contracts_with_accounts_df, left_on = ['Opportunity_External_ID__c'], right_on = ['contract_number'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # drop id columns and _merge column
    # print(both_df.head())
    # print(sf_opportunity_only_df.head())
    # print(opportunity_to_insert_df.head())

else:
    # if there are no matching contracts in SF, copy and load the entire dataframe
    opportunity_to_insert_df = contracts_with_accounts_df

# drop end date column, will use in quotes
opportunity_to_insert_df.drop(['end_date'], axis = 1, inplace = True)

# rename columns to Salesforce field naming conventions
opportunity_to_insert_df.rename(columns = {"Id":"AccountId",
                                        "contract_number" : "Opportunity_External_ID__c",
                                        "start_date" : "CloseDate" }, inplace = True)
opportunity_to_insert_df["StageName"] = "Closed Won"
opportunity_to_insert_df["Name"] = opportunity_to_insert_df["Opportunity_External_ID__c"]
opportunity_to_insert_df = SF_Utils.format_date_to_salesforce_date(opportunity_to_insert_df, "CloseDate")

# add migrated record tag
opportunity_to_insert_df['Migrated_Record__c'] = True
print(opportunity_to_insert_df.head())

# insert net new records into salesforce Contact object
# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, opportunity_to_insert_df, object, 'insert', success_file, fallout_file)
