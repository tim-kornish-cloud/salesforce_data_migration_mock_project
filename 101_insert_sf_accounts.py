"""
Author: Timothy Kornish
CreatedDate: March - 30 - 2026
Description: Log into mssql server, query account staging table records.
             log into salesforce and query existing accounts.
             merge records from both systems to determine what records exist
             in both systems, and which records are net new.
             insert net new records into salesforce account table.

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

#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# success file path
success_file = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + database + ".csv"

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT
       [phone]
      ,[company_name]
      ,[industry]
      ,[annual_revenue]
      ,[account_number_external_id]
      ,[number_of_locations]
      ,[number_of_employees]
      ,[sla]
      ,[sla_serial_number]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Accounts]"""

# accounts in the mssql table shown in the query above
stg_account_df = MSSQL_Utils.query_mssql_return_dataframe(select_query, cursor)

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

#print(account_query_results)
# convert query results to a dataframe
sf_accounts_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
# encode the dataframe before uploading to delete
sf_accounts_df = Utils.encode_df(sf_accounts_df)

# perform merge of staging accounts and salesforce accounts
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_accounts_df) != 0:
    print("len != 0")
    # merge the csv data with the salesforce data to match SF Ids to the CSV accounts
    both_df, sf_only_accounts, accounts_to_insert_df = Utils.get_df_diffs(sf_accounts_df, stg_account_df, left_on = ['Account_Number_External_ID__c'], right_on = ['account_number_external_id'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # keep all net new records and drop any records existing in both systems
    accounts_to_insert_df.drop(['Id', 'Account_Number_External_ID__c', '_merge'], axis = 1, inplace = True)

else:
    print("len = 0")
    accounts_to_insert_df = stg_account_df

# convert float values to ints
accounts_to_insert_df['number_of_locations'] = accounts_to_insert_df['number_of_locations'].astype('int64')
accounts_to_insert_df['sla_serial_number'] = accounts_to_insert_df['sla_serial_number'].astype('int64')
accounts_to_insert_df['number_of_employees'] = accounts_to_insert_df['number_of_employees'].astype('int64')
accounts_to_insert_df['annual_revenue'] = accounts_to_insert_df['annual_revenue'].astype('int64')

# rename columns to salesforce column names
accounts_to_insert_df.rename(columns={'phone' : 'Phone',
                                      'company_name' : 'Name',
                                      'industry' : 'Industry',
                                      'annual_revenue' : 'AnnualRevenue',
                                      'account_number_external_id' : 'Account_Number_External_ID__c',
                                      'number_of_locations' : 'NumberofLocations__c',
                                      'number_of_employees' : 'NumberOfEmployees',
                                      'sla' : 'SLA__c',
                                      'sla_serial_number' : 'SLASerialNumber__c'}, inplace=True)

# add migrated record tag
accounts_to_insert_df['Migrated_Record__c'] = True

# insert net new records into salesforce account object
# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, accounts_to_insert_df, 'Account', 'insert', success_file, fallout_file)
