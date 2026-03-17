"""
Author: Timothy Kornish
CreatedDate: August - 9 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a Salesforce environment and load
             a set number of records from the mock data into the
             salesforce org.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  Salesforce_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'Dev'

# set database to MySQL
database = "Salesforce"

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 5

# query string to select records from salesforce
# before uploading with a delete  DML operation
account_query = "SELECT Id FROM Account WHERE CreatedBy.Name = 'Timothy Kornish'"

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set up fallout ans success path to save files to
# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + "_" + database + ".csv"

# set input path for mock data csv
input_csv_file = dir_path + ".\\MockData\\MOCK_DATA_unit_test.csv"

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

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
mock_df = pd.read_csv(input_csv_file)

# select only 10 records
df_to_upload = mock_df.iloc[record_start:record_start+num_of_records]

# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, df_to_upload, 'Account', 'insert', success_file, fallout_file)
