"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: Log into Salesforce and delete all accounts with CreatedBy.Name = 'Timothy Kornish'

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

# set database to MySQL
database = "Salesforce"

# query string to select records from salesforce
# before uploading with a delete  DML operation
account_query = "SELECT Id FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"

#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\DELETE\\SUCCESS_Delete_" + environment + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\DELETE\\FALLOUT_Delete_" + environment + "_" + database + ".csv"

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

# query salesforce and return the accounts to be deleted
account_query_results = SF_Utils.query_salesforce(sf, account_query)
# convert query results to a dataframe
accounts_to_delete_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
# encode the dataframe before uploading to delete
accounts_to_delete_df = Utils.encode_df(accounts_to_delete_df)

# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, accounts_to_delete_df, 'Account', 'delete', success_file, fallout_file)
