"""
Author: Timothy Kornish
CreatedDate: August - 21 - 2026
Description: log into salesforce, query existing Record jobs used to upload records,
             delete all record jobs. This opens up storage space.

"""

# to target apex async jobs, run the debugger in anonymous apex window:

// Target jobs finished before 30 days ago
Date targetDate = Date.today().addDays(-30);

// Purge a maximum of 5,000 old async jobs
Integer deletedCount = System.purgeOldAsyncJobs(targetDate, 5000);

System.debug('Deleted old async jobs count: ' + deletedCount);

# The commented below doesn't actually purge the apex jobs as intended, do not use

# import numpy as np
# import pandas as pd
# import os
# from custom_db_utilities import MSSQL_Utilities, Salesforce_Utilities, Custom_Utilities
# from credentials import Credentials
#
# # create and instance of the custom salesforce utilities class used to interact with Salesforce
# SF_Utils = Salesforce_Utilities()
# # create and instance of the custom  utilities class
# Utils = Custom_Utilities()
# # create instance of credentials class where creds are stored to load into the script
# Cred = Credentials()
#
# # set up directory pathway to load csv data and output fallout and success results to
# dir_path = os.path.dirname(os.path.realpath(__file__))
#
# # declare which environment this script will perform operations against,
# # can have multiple environments in the same script at the same time
# sf_environment = 'Dev'
# # set database to Salesforce
# sf_database = "Salesforce"
# #set object for output files
# object = "SBQQ__RecordJob__c"
#
# # success file path
# success_file = dir_path + "\\Output\\DELETE\\SUCCESS_Delete_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# # fallout file path
# fallout_file = dir_path + "\\Output\\DELETE\\FALLOUT_Delete_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
#
# # get credentials for salesforce login
# # get username from credentials
# sf_username = Cred.get_username(sf_database, sf_environment)
# # get password from credentials
# sf_password = Cred.get_password(sf_database, sf_environment)
# # get login token from credentials
# sf_token = Cred.get_token(sf_database, sf_environment)
#
# # create a instance of simple_salesforce to query and perform operations against salesforce with
# sf = SF_Utils.login_to_salesForce(sf_username, sf_password, sf_token)
# # query string to select records from salesforce
# recordjob_query = "SELECT Id FROM SBQQ__RecordJob__c "
# # query salesforce and return the recordjobs to be deleted
# recordjob_query_results = SF_Utils.query_salesforce(sf, recordjob_query)
#
# # convert query results to a dataframe
# sf_recordjobs_df = SF_Utils.load_query_with_lookups_into_dataframe(recordjob_query_results)
# # encode the dataframe before uploading to delete
# sf_recordjobs_df = Utils.encode_df(sf_recordjobs_df)
#
# # delete migrated salesforce recordjob records
# # upload the records to salesforce for deletion
# passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_recordjobs_df, object, 'delete', success_file, fallout_file)
