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
success_file_2 = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_2_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file_2 = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_2_" + environment + "_" + object + "_" + database + ".csv"

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
SF_Utils.upload_dataframe_to_salesforce(sf, sf_quotes_df, object, 'update', success_file, fallout_file, batch_size = 10)

# set delay after updating quotes before ordering them
print("Begin 1 minute time delay before ordering quotes.")
time.sleep(60)
print("Time delay over, start ordering quotes.")

# query existing quotes from salesforce
# query string to select records from salesforce
quote_query = "SELECT Id FROM SBQQ__Quote__c WHERE Migrated_Record__c = True AND SBQQ__Primary__c = True"
# query salesforce and return the quotes to be deleted
quote_query_results = SF_Utils.query_salesforce(sf, quote_query)

# convert query results to a dataframe
sf_quotes_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_query_results)
# encode the dataframe before uploading to delete
sf_quotes_df = Utils.encode_df(sf_quotes_df)

# update all quotes to SBQQ__Ordered__c = true
sf_quotes_df['SBQQ__Ordered__c'] = True

# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, sf_quotes_df, object, 'update', success_file_2, fallout_file_2, batch_size = 10)
