"""
Author: Timothy Kornish
CreatedDate: June - 14 - 2026
Description: In salesforce configure CPQ to re-enable CPQ triggers
             Log into salesforce and query existing quotes.
             Update field SBQQ__Primary__c to true.
             wait 1 minute.
             Update field SBQQ__Ordered__c to true.
"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MSSQL_Utilities, Salesforce_Utilities, Custom_Utilities
from credentials import Credentials
import time

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
# set object name to update records into salesforce
object = 'SBQQ__Quote__c'
# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# success file path
success_file = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + object + "_" + database + ".csv"

# success file path
success_file_2 = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_2_" + environment + "_" + "SBQQ__Quote__c" + "_" + database + ".csv"
# fallout file path
fallout_file_2 = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_2_" + environment + "_" + "SBQQ__Quote__c" + "_" + database + ".csv"

# success file path
success_file_3 = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_3_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file_3 = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_3_" + environment + "_" + object + "_" + database + ".csv"

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

# Query SF quotes - relate contract line to quote
# query existing quotes from salesforce
# query string to select records from salesforce
quote_query = "SELECT Id FROM SBQQ__Quote__c WHERE Migrated_Record__c = True AND SBQQ__Primary__c = False"
# query salesforce and return the quotes to be deleted
quote_query_results = SF_Utils.query_salesforce(sf, quote_query)

# convert query results to a dataframe
sf_quotes_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_query_results)
# encode the dataframe before uploading to delete
sf_quotes_df = Utils.encode_df(sf_quotes_df)

# update quote to primary to sync quotelines with opportunity
sf_quotes_df['SBQQ__Primary__c'] = True

# update records in salesforce Quote object
# upload the records to salesforce
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_quotes_df, object, 'update', success_file, fallout_file, batch_size = 10)

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# mssql table name the dataframe is being updated into
success_table_name = "[dbo].[trgt_106_SBQQ__Quote__c_update_Primary_Success]"
# mssql table name the dataframe is being updated into
fallout_table_name = "[dbo].[trgt_106_SBQQ__Quote__c_update_Primary_Fallout]"

# generate column types from passing and fallout dataframes,
# should always be the same so redundant to run for each.
passing_dtypes = Utils.get_dtypes_as_list(passing_df)
fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)
# upload fallout records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)

# set delay after updating quotes before ordering them
print("Begin 30 second time delay before updating quotelines to calculate quotes.")
time.sleep(30)
print("Time delay over, start calculating quotes.")

# query existing quotelines from salesforce
# query string to select records from salesforce
quote_line_query = "SELECT Id, SBQQ__PricingMethod__c FROM SBQQ__QuoteLine__c WHERE SBQQ__Quote__r.SBQQ__Uncalculated__c = True AND SBQQ__Quote__r.Migrated_Record__c = True AND SBQQ__Quote__r.SBQQ__Primary__c = True AND SBQQ__Quote__r.SBQQ__Ordered__c = False  "
# query salesforce and return the quotes to be deleted
quote_line_query_results = SF_Utils.query_salesforce(sf, quote_line_query)

# convert query results to a dataframe
sf_quote_lines_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_line_query_results)
# encode the dataframe before uploading to delete
sf_quote_lines_df = Utils.encode_df(sf_quote_lines_df)

# upload the records to salesforce
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_quote_lines_df, 'SBQQ__QuoteLine__c', 'update', success_file_2, fallout_file_2, batch_size = 10)

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# mssql table name the dataframe is being inserted into
success_table_name = "[dbo].[SBQQ__Quote__c_106_ql_update_Success]"
# mssql table name the dataframe is being inserted into
fallout_table_name = "[dbo].[SBQQ__Quote__c_106_ql_update_Fallout]"

# generate column types from passing and fallout dataframes,
# should always be the same so redundant to run for each.
passing_dtypes = Utils.get_dtypes_as_list(passing_df)
fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)
# upload fallout records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)

# set delay after updating quotes before ordering them
print("Begin 1 minute time delay before ordering quotes.")
time.sleep(60)
print("Time delay over, start ordering quotes.")

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)

# query existing quotes from salesforce
# query string to select records from salesforce
quote_query = "SELECT Id FROM SBQQ__Quote__c WHERE Migrated_Record__c = True AND SBQQ__Primary__c = True AND SBQQ__Ordered__c = False  "
# query salesforce and return the quotes to be deleted
quote_query_results = SF_Utils.query_salesforce(sf, quote_query)

# convert query results to a dataframe
sf_quotes_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_query_results)
# encode the dataframe before uploading to delete
sf_quotes_df = Utils.encode_df(sf_quotes_df)

# update all quotes to SBQQ__Ordered__c = true
sf_quotes_df['SBQQ__Ordered__c'] = True

# upload the records to salesforce
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_quotes_df, object, 'update', success_file_3, fallout_file_3, batch_size = 10)

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# mssql table name the dataframe is being inserted into
success_table_name = "[dbo].[SBQQ__Quote__c_106_Ordered_Success]"
# mssql table name the dataframe is being inserted into
fallout_table_name = "[dbo].[SBQQ__Quote__c_106_Ordered_Fallout]"

# generate column types from passing and fallout dataframes,
# should always be the same so redundant to run for each.
passing_dtypes = Utils.get_dtypes_as_list(passing_df)
fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)
# upload fallout records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)
