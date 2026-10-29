"""
Author: Timothy Kornish
CreatedDate: March - 30 - 2026
Description: log into salesforce, query existing OrderItem WHERE SBQQ__QuoteLine__r.Migrated_Record__c = True"
             query existing Order WHERE SBQQ__Quote__r.Migrated_Record__c = True"
             1) on OrderItem set SBQQ__Status__c = Draft
             2) update SBQQ__Contract__c = False on Order and set SBQQ__Quote__c = NULL
             3) set status of order to draft
             4) delete orderItems of order

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

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
sf_environment = 'Dev'
# set database to Salesforce
sf_database = "Salesforce"
# set object for output files
object = "OrderItem"


# success file path
success_file_1 = dir_path + "\\Output\\UPDATE\\SUCCESS_1_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_1 = dir_path + "\\Output\\UPDATE\\FALLOUT_1_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# success file path
success_file_4 = dir_path + "\\Output\\DELETE\\SUCCESS_4_DELETE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_4 = dir_path + "\\Output\\DELETE\\FALLOUT_4_DELETE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"

# set object for output files
object = "Order"

# success file path
success_file_2 = dir_path + "\\Output\\UPDATE\\SUCCESS_2_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_2 = dir_path + "\\Output\\UPDATE\\FALLOUT_2_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"

# success file path
success_file_3 = dir_path + "\\Output\\UPDATE\\SUCCESS_3_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_3 = dir_path + "\\Output\\UPDATE\\FALLOUT_3_UPDATE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"

# success file path
success_file_5 = dir_path + "\\Output\\DELETE\\SUCCESS_5_DELETE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"
# fallout file path
fallout_file_5 = dir_path + "\\Output\\DELETE\\FALLOUT_5_DELETE_" + sf_environment + "_" + object + "_" + sf_database + ".csv"


# get username from credentials
sf_username = Cred.get_username(sf_database, sf_environment)
# get consumer_key from credentials
sf_consumer_key = Cred.get_consumer_key(sf_database, sf_environment)
# get privatekey_file from credentials
sf_privatekey_file = Cred.get_privatekey_file(sf_database, sf_environment)

# create a instance of simple_salesforce to query and perform operations against salesforce with
sf = SF_Utils.login_to_salesForce(username = sf_username, consumer_key = sf_consumer_key, privatekey_file = sf_privatekey_file)


# query string to select records from salesforce
order_item_query = "SELECT Id FROM OrderItem WHERE SBQQ__QuoteLine__r.Migrated_Record__c = True OR CreatedBy.Name = 'Timothy Kornish'"
# query salesforce and return the order_items to be deleted
order_item_query_results = SF_Utils.query_salesforce(sf, order_item_query)

# convert query results to a dataframe
sf_order_items_df = SF_Utils.load_query_with_lookups_into_dataframe(order_item_query_results)
# encode the dataframe before uploading to delete
sf_order_items_df = Utils.encode_df(sf_order_items_df)
sf_order_items_df['SBQQ__Status__c'] = 'Draft'

# upload the records to salesforce for deletion
order_item_draft_passing_df, order_item_draft_fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_order_items_df, 'OrderItem', 'update', success_file_1, fallout_file_1)

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# mssql table name the dataframe is being inserted into
order_item_draft_success_table_name = "[dbo].[trgt_del_207_OrderItem_update_status_Success]"
# mssql table name the dataframe is being inserted into
order_item_draft_fallout_table_name = "[dbo].[trgt_del_207_OrderItem_update_status_Fallout]"

# check if order_item_draft_passing_df dataframe is empty before processing into reporting tables
if not (len(order_item_draft_passing_df) == 0):
    # remove error column on order_item_draft_passing_df df
    order_item_draft_passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from order_item_draft_passing_df dataframe
    order_item_draft_passing_dtypes = Utils.get_dtypes_as_list(order_item_draft_passing_df)
    # upload order_item_draft_passing_df records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_item_draft_success_table_name, order_item_draft_passing_df, order_item_draft_passing_dtypes, drop_table = True)

# check if order_item_draft_fallout_df dataframe is empty before processing into reporting tables
if not (len(order_item_draft_fallout_df) == 0):
    # generate column types from order_item_draft_fallout_df dataframe
    order_item_draft_fallout_dtypes = Utils.get_dtypes_as_list(order_item_draft_fallout_df)
    # upload order_item_draft_fallout_df records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_item_draft_fallout_table_name, order_item_draft_fallout_df, order_item_draft_fallout_dtypes, drop_table = True)

# query string to select records from salesforce
order_query = "SELECT Id FROM Order WHERE SBQQ__Quote__r.Migrated_Record__c = True OR CreatedBy.Name = 'Timothy Kornish'"
# query salesforce and return the orders to be deleted
order_query_results = SF_Utils.query_salesforce(sf, order_query)

# convert query results to a dataframe
sf_orders_df = SF_Utils.load_query_with_lookups_into_dataframe(order_query_results)
# encode the dataframe before uploading to delete
sf_orders_df = Utils.encode_df(sf_orders_df)

# update contracted to false
sf_orders_df['SBQQ__Contracted__c'] = False
# remove relationship between order and quote setting SBQQ__Quote__c to null on orders
sf_orders_df['SBQQ__Quote__c'] = ""
# upload the records to salesforce for update
order_contracted_passing_df, order_contracted_fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_orders_df, 'Order', 'update', success_file_2, fallout_file_2)

# mssql table name the dataframe is being inserted into
order_contracted_success_table_name = "[dbo].[trgt_del_207_Order_contracted_udpate_Success]"
# mssql table name the dataframe is being inserted into
order_contracted_fallout_table_name = "[dbo].[trgt_del_207_Order_contracted_udpate_Fallout]"

# check if order_contracted_passing_df dataframe is empty before processing into reporting tables
if not (len(order_contracted_passing_df) == 0):
    # remove error column on order_contracted_passing_df df
    order_contracted_passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from order_contracted_passing_df dataframe
    order_contracted_passing_dtypes = Utils.get_dtypes_as_list(order_contracted_passing_df)
    # upload order_contracted_passing_df records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_contracted_success_table_name, order_contracted_passing_df, order_contracted_passing_dtypes, drop_table = True)

# check if fallout dataframe is empty before processing into reporting tables
if not (len(order_contracted_fallout_df) == 0):
    # generate column types from fallout dataframe
    order_contracted_fallout_dtypes = Utils.get_dtypes_as_list(order_contracted_fallout_df)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_contracted_fallout_table_name, order_contracted_fallout_df, order_contracted_fallout_dtypes, drop_table = True)

# update status  = draft
sf_orders_df['Status'] = 'Draft'
# remove contracted column
sf_orders_df.drop(['SBQQ__Contracted__c'], axis = 1, inplace = True)
# upload the records to salesforce for update
order_status_passing_df, order_status_fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_orders_df, 'Order', 'update', success_file_3, fallout_file_3)

# mssql table name the dataframe is being inserted into
order_status_success_table_name = "[dbo].[trgt_del_207_Order_status_udpate_Success]"
# mssql table name the dataframe is being inserted into
order_status_fallout_table_name = "[dbo].[trgt_del_207_Order_status_udpate_Fallout]"

# check if order_status_passing_df dataframe is empty before processing into reporting tables
if not (len(order_status_passing_df) == 0):
    # remove error column on order_status_passing_df df
    order_status_passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from order_status_passing_df dataframe
    order_status_passing_dtypes = Utils.get_dtypes_as_list(order_status_passing_df)
    # upload order_status_passing_df records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_status_success_table_name, order_status_passing_df, order_status_passing_dtypes, drop_table = True)

# check if fallout dataframe is empty before processing into reporting tables
if not (len(order_status_fallout_df) == 0):
    # generate column types from fallout dataframe
    order_status_fallout_dtypes = Utils.get_dtypes_as_list(order_status_fallout_df)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_status_fallout_table_name, order_status_fallout_df, order_status_fallout_dtypes, drop_table = True)

if not (len(sf_order_items_df) == 0):
    # When deleting only sumbit Id column
    sf_order_items_df = sf_order_items_df[['Id']]

# delete migrated salesforce order_item records
# upload the records to salesforce for deletion
order_item_passing_df, order_item_fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_order_items_df, 'OrderItem', 'delete', success_file_4, fallout_file_4)

# mssql table name the dataframe is being inserted into
order_item_success_table_name = "[dbo].[trgt_del_207_OrderItem_Success]"
# mssql table name the dataframe is being inserted into
order_item_fallout_table_name = "[dbo].[trgt_del_207_OrderItem_Fallout]"

# check if order_item_passing_df dataframe is empty before processing into reporting tables
if not (len(order_item_passing_df) == 0):
    # remove error column on order_item_passing_df df
    order_item_passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from order_item_passing_df dataframe
    order_item_passing_dtypes = Utils.get_dtypes_as_list(order_item_passing_df)
    # upload order_item_passing_df records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_item_success_table_name, order_item_passing_df, order_item_passing_dtypes, drop_table = True)

# check if order_item_fallout_df dataframe is empty before processing into reporting tables
if not (len(order_item_fallout_df) == 0):
    # generate column types from order_item_fallout_df dataframe
    order_item_fallout_dtypes = Utils.get_dtypes_as_list(order_item_fallout_df)
    # upload order_item_fallout_df records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_item_fallout_table_name, order_item_fallout_df, order_item_fallout_dtypes, drop_table = True)

# When deleting only sumbit Id column
sf_orders_df = sf_orders_df[['Id']]

# upload the records to salesforce for deletion
order_passing_df, order_fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, sf_orders_df, 'Order', 'delete', success_file_5, fallout_file_5)

# mssql table name the dataframe is being inserted into
order_success_table_name = "[dbo].[trgt_del_207_Order_Success]"
# mssql table name the dataframe is being inserted into
order_fallout_table_name = "[dbo].[trgt_del_207_Order_Fallout]"

# check if passing dataframe is empty before processing into reporting tables
if not (len(order_passing_df) == 0):
    # remove error column on passing df
    order_passing_df.drop(['RESULTS_errors'], axis = 1, inplace = True)
    # generate column types from passing dataframe
    order_passing_dtypes = Utils.get_dtypes_as_list(order_passing_df)
    # upload passing records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_success_table_name, order_passing_df, order_passing_dtypes, drop_table = True)

# check if fallout dataframe is empty before processing into reporting tables
if not (len(order_fallout_df) == 0):
    # generate column types from fallout dataframe
    order_fallout_dtypes = Utils.get_dtypes_as_list(order_fallout_df)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, order_fallout_table_name, order_fallout_df, order_fallout_dtypes, drop_table = True)
