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
success_file_1 = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file_1 = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + object + "_" + database + ".csv"

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
order_query = "SELECT Id, SBQQ__Quote__r.SBQQ__StartDate__c FROM Order WHERE SBQQ__Quote__r.Migrated_Record__c = True"
# query salesforce and return the orders to be deleted
order_query_results = SF_Utils.query_salesforce(sf, order_query)

# convert query results to a dataframe
sf_orders_df = SF_Utils.load_query_with_lookups_into_dataframe(order_query_results)
# encode the dataframe before uploading to delete
sf_orders_df = Utils.encode_df(sf_orders_df)

# rename quoteline start date to effective date
sf_orders_df.rename({"SBQQ__Quote__r.SBQQ__StartDate__c" : "EffectiveDate"}, axis = 1, inplace = True)

# update records in salesforce Order object
# upload the records to salesforce
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_orders_df, object, 'update', success_file_1, fallout_file_1)

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# mssql table name the dataframe is being updated into
success_table_name = "[dbo].[trgt_107_Order_update_Success]"
# mssql table name the dataframe is being updated into
fallout_table_name = "[dbo].[trgt_107_Order_update_Fallout]"

# check if passing dataframe is empty before processing into reporting tables
if not (len(passing_df) == 0):
    # remove error column on passing df
    passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from passing dataframe
    passing_dtypes = Utils.get_dtypes_as_list(passing_df)
    # upload passing records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)

# check if fallout dataframe is empty before processing into reporting tables
if not (len(fallout_df) == 0):
    # generate column types from fallout dataframe
    fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)

# set object for update to OrderItem
object = "OrderItem"

# success file path
success_file_2 = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file_2 = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + object + "_" + database + ".csv"

# query existing orders from salesforce
# query string to select records from salesforce
order_line_query = "SELECT Id, SBQQ__QuoteLine__r.SBQQ__StartDate__c FROM OrderItem WHERE SBQQ__QuoteLine__r.Migrated_Record__c = True"
# query salesforce and return the orders to be deleted
order_line_query_results = SF_Utils.query_salesforce(sf, order_line_query)

# convert query results to a dataframe
sf_order_lines_df = SF_Utils.load_query_with_lookups_into_dataframe(order_line_query_results)
# encode the dataframe before uploading to delete
sf_order_lines_df = Utils.encode_df(sf_order_lines_df)

# rename quoteline start date to effective date
sf_order_lines_df.rename({"SBQQ__QuoteLine__r.SBQQ__StartDate__c" : "ServiceDate"}, axis = 1, inplace = True)

# update order items service date
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_order_lines_df, object, 'update', success_file_2, fallout_file_2)

# mssql table name the dataframe is being updated into
success_table_name = "[dbo].[trgt_107_OrderItem_update_Success]"
# mssql table name the dataframe is being updated into
fallout_table_name = "[dbo].[trgt_107_OrderItem_update_Fallout]"

# check if passing dataframe is empty before processing into reporting tables
if not (len(passing_df) == 0):
    # remove error column on passing df
    passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from passing dataframe
    passing_dtypes = Utils.get_dtypes_as_list(passing_df)
    # upload passing records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)

# check if fallout dataframe is empty before processing into reporting tables
if not (len(fallout_df) == 0):
    # generate column types from fallout dataframe
    fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)

# update orders - status = activated
object = "Order"
# success file path
success_file_3 = dir_path + "\\Output\\UPDATE\\SUCCESS_Update_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file_3 = dir_path + "\\Output\\UPDATE\\FALLOUT_Update_" + environment + "_" + object + "_" + database + ".csv"
# Update Orders to be activated so they can be contracted afterwards in the next script
sf_orders_df["Status"] = "Activated"

# only keep status and id fields for update
sf_orders_df = sf_orders_df[["Id", "Status"]]

# update order items service date
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_orders_df, object, 'update', success_file_3, fallout_file_3)

# mssql table name the dataframe is being updated into
success_table_name = "[dbo].[trgt_107_Order_activate_Success]"
# mssql table name the dataframe is being updated into
fallout_table_name = "[dbo].[trgt_107_Order_activate_Fallout]"

# check if passing dataframe is empty before processing into reporting tables
if not (len(passing_df) == 0):
    # remove error column on passing df
    passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from passing dataframe
    passing_dtypes = Utils.get_dtypes_as_list(passing_df)
    # upload passing records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)

# check if fallout dataframe is empty before processing into reporting tables
if not (len(fallout_df) == 0):
    # generate column types from fallout dataframe
    fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)
