"""
Author: Timothy Kornish
CreatedDate: March - 30 - 2026
Description: log into salesforce, query existing opportunities where Migrated_Record__c = True
             delete all migrated account records from salesforce.

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import Salesforce_Utilities, Custom_Utilities
from credentials import Credentials

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

# success file path
success_file = dir_path + "\\Output\\DELETE\\SUCCESS_Delete_" + sf_environment + "_" + sf_database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\DELETE\\FALLOUT_Delete_" + sf_environment + "_" + sf_database + ".csv"

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
opportunity2_query = "SELECT Id FROM Opportunity2 WHERE Account.Migrated_Record__c = True"
# query salesforce and return the opportunity2 to be deleted
opportunity2_query_results = SF_Utils.query_salesforce(sf, opportunity2_query)

#print(opportunity2_query_results)
# convert query results to a dataframe
sf_opportunity2_df = SF_Utils.load_query_with_lookups_into_dataframe(opportunity2_query_results)
# encode the dataframe before uploading to delete
sf_opportunity2_df = Utils.encode_df(sf_opportunity2_df)

# delete migrated salesforce opportunity2 records
# upload the records to salesforce for deletion
SF_Utils.upload_dataframe_to_salesforce(sf, sf_opportunity2_df, 'Opportunity2', 'delete', success_file, fallout_file)
