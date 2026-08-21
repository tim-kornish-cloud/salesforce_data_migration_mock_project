"""
Author: Timothy Kornish
CreatedDate: March - 30 - 2026
Description: log into salesforce, query existing Contracts where SBQQ_Quote__r.Migrated_Record__c = true
             1) update status to draft.
             2) delete contract records

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import MSSQL_Utilities, Salesforce_Utilities, Custom_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
sf_environment = 'Dev'
# set database to Salesforce
sf_database = "Salesforce"
# set object for output files
object = "Contract"

# success file path
success_file_1 = dir_path + "\\Output\\UPDATE\\SUCCESS_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_1 = dir_path + "\\Output\\UPDATE\\FALLOUT_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"


# success file path
success_file_2 = dir_path + "\\Output\\DELETE\\SUCCESS_DELETE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_2 = dir_path + "\\Output\\DELETE\\FALLOUT_DELETE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"

# get credentials for salesforce login
# get username from credentials
sf_username = Cred.get_username(sf_database, sf_environment)
# get password from credentials
sf_password = Cred.get_password(sf_database, sf_environment)
# get login token from credentials
sf_token = Cred.get_token(sf_database, sf_environment)

# create a instance of simple_salesforce to query and perform operations against salesforce with
sf = SF_Utils.login_to_salesForce(sf_username, sf_password, sf_token)
# query string to select records from salesforce
contract_query = "SELECT Id FROM Contract WHERE SBQQ__Order__r.SBQQ__Quote__r.Migrated_Record__c = True"
# query salesforce and return the contracts to be deleted
contract_query_results = SF_Utils.query_salesforce(sf, contract_query)

#print(contract_query_results)
# convert query results to a dataframe
sf_contracts_df = SF_Utils.load_query_with_lookups_into_dataframe(contract_query_results)
# encode the dataframe before uploading to delete
sf_contracts_df = Utils.encode_df(sf_contracts_df)

# update contracts status to draft to deactivate
sf_contracts_df['Status'] = 'Draft'

# delete migrated salesforce contract records
# upload the records to salesforce for deletion
SF_Utils.upload_dataframe_to_salesforce(sf, sf_contracts_df, 'Contract', 'update', success_file_1, fallout_file_1)

# remove status column before deleting contracts
sf_contracts_df.drop(['Status'], axis = 1, inplace = True)

# delete migrated salesforce contract records
# upload the records to salesforce for deletion
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_contracts_df, 'Contract', 'delete', success_file_2, fallout_file_2)

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# mssql table name the dataframe is being inserted into
success_table_name = "[dbo].[Contract_208_Success]"
# mssql table name the dataframe is being inserted into
fallout_table_name = "[dbo].[Contract_208_Fallout]"

# generate column types from passing and fallout dataframes,
# should always be the same so redundant to run for each.
passing_dtypes = Utils.get_dtypes_as_list(passing_df)
fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)
# upload fallout records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)
