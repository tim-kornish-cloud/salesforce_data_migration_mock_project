"""
Author: Timothy Kornish
CreatedDate: June - 14 - 2026
Description: Log into salesforce and query existing order from migrated quotes.
             Update field status to "Activated" allowing the order to then be contracted.
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
# set object name to update records into salesforce
object = 'Order'
# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# success file path
success_file = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + object + "_" + database + ".csv"


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

# query existing orders from salesforce
# query string to select records from salesforce
order_query = "SELECT Id FROM Order WHERE SBQQ__Quote__r.Migrated_Record__c = True"
# query salesforce and return the orders to be deleted
order_query_results = SF_Utils.query_salesforce(sf, order_query)

# convert query results to a dataframe
sf_orders_df = SF_Utils.load_query_with_lookups_into_dataframe(order_query_results)
# encode the dataframe before uploading to delete
sf_orders_df = Utils.encode_df(sf_orders_df)

# Update Orders to be activated so they can be contracted afterwards in the next script
sf_orders_df['Status'] = 'Activated'

# update records in salesforce Quote object
# upload the records to salesforce
SF_Utils.upload_dataframe_to_salesforce(sf, sf_orders_df, object, 'update', success_file, fallout_file)
