"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: Log into Salesforce and update a specific set of accounts
             based on query criteria

"""

import numpy as np
import pandas as pd
import os
from simple_salesforce import Salesforce
from custom_db_utilities import  Salesforce_Utilities, Custom_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# create and instance of the custom utilities class used to format and modify dataframe data
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'Dev'

# set database to Salesforce
database = "Salesforce"

# number of records to attempted
num_of_records = 5

# starting index to choose records
record_start = 5

# query string to select records from salesforce
# before uploading with a delete  DML operation
account_query = "SELECT Id FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + database + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_unit_test.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# load credentials for Salesforce and the Dev environement
# I use this method instead of a hardcoding credentials and instead of a
# properties or txt file due to parsing issues
# The credentials are stored as strings in a dictionary attribute of a class

# get username from credentials
username = Cred.get_username(database, environment)
# get password from credentials
password = Cred.get_password(database, environment)
# get login token from credentials
token = Cred.get_token(database, environment)

# create a instance of simple_salesforce to query and perform operations against salesforce with
sf = SF_Utils.login_to_salesForce(username, password, token)

# match queried accounts with CSV accounts based on join of accountNumber field
# query string to select records from salesforce
# before uploading with a delete  DML operation
account_query = "SELECT Id, Account_Number_External_ID__c FROM Account WHERE Unit_test_migrated_record__c = true"

# query salesforce and return the accounts to be deleted
account_query_results = SF_Utils.query_salesforce(sf, account_query)
# convert query results to a dataframe
accounts_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
# encode the dataframe before uploading to delete
accounts_df = Utils.encode_df(accounts_df)

# select only 10 records to process
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

# merge the csv data with the salesforce data to match SF Ids to the CSV accounts
accounts_to_update_df = Utils.merge_dfs(accounts_df, df_to_upload, left_on = ['Account_Number_External_ID__c'], right_on = ['Account_Number_External_ID__c'], how = 'inner', suffixes = ('_SF', '_CSV'), indicator = True)

# add new column called type and set all accounts to government
accounts_to_update_df.loc[:,"Type"] = "Prospect"
# add new column called type and set all accounts to government
accounts_to_update_df.loc[:,"Industry"] = "Government"

# isolate which account fields will be used in the update
# only updating the two fields Type and Industry
accounts_to_update_df = accounts_to_update_df[["Id", "Type", "Industry", ]]

# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, accounts_to_update_df, 'Account', 'update', success_file, fallout_file)
