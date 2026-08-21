"""
Author: Timothy Kornish
CreatedDate: June - 10 - 2026
Description: Log into mssql server, query contact staging table records.
             Log into salesforce and query existing accounts.
             Merge SF.accounts.Account_Number_External_ID__c = MSSQL.Contact.Account_Number_External_ID__c
             from both systems to align migrated accounts to staging contacts.
             query salesforce for any contacts to determine what related records exist
             in both systems, which records are net new, and which are missing prior attempted accounts, or other issues.
             Insert net new records into salesforce contact table.
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
# set object name to insert records into salesforce
object = 'Contact'
# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + "_" + object + "_" + database + ".csv"

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """ SELECT [account_number_external_id]
      ,[first_name]
      ,[last_name]
      ,[email]
      ,[title]
      ,[department]
      ,[languages]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contacts]"""

# contacts in the mssql table shown in the query above
stg_contact_df = MSSQL_Utils.query_mssql_return_dataframe(select_query, cursor)

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

# query existing accounts from salesforce
# query string to select records from salesforce
account_query = "SELECT Id, Account_Number_External_ID__c FROM Account WHERE Migrated_Record__c = True"
# query salesforce and return the accounts to be deleted
account_query_results = SF_Utils.query_salesforce(sf, account_query)

# convert query results to a dataframe
sf_accounts_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
# encode the dataframe before uploading to delete
sf_accounts_df = Utils.encode_df(sf_accounts_df)

# perform merge of staging accounts and salesforce accounts
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_accounts_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV accounts
    contacts_with_accounts_df, sf_accounts_only_df, mssql_contacts_only_df = Utils.get_df_diffs(sf_accounts_df, stg_contact_df, left_on = ['Account_Number_External_ID__c'], right_on = ['account_number_external_id'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # remove unneccessary column for insert
    contacts_with_accounts_df.drop(["account_number_external_id",  "Account_Number_External_ID__c", "_merge"], axis = 1, inplace = True)

    # remove _merge columns
    sf_accounts_only_df.drop(["_merge"], axis = 1, inplace = True)
    mssql_contacts_only_df.drop(["_merge"], axis = 1, inplace = True)

    # generate datatypes list for each dataframe of accounts in both systems and sf only accounts
    sf_only_accounts_dtypes = Utils.get_dtypes_as_list(sf_accounts_only_df)
    mssql_contacts_only_df_dtypes = Utils.get_dtypes_as_list(mssql_contacts_only_df)

    # mssql table name the dataframe is being inserted into
    sf_only_accounts_table = "[dbo].[Account_102_sf_accounts_only]"

    # mssql table name the dataframe is being inserted into
    mssql_contacts_only_df_table = "[dbo].[Account_102_mssql_contacts_only]"

    # upload success records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, sf_only_accounts_table, sf_accounts_only_df, sf_only_accounts_dtypes, drop_table = True)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, mssql_contacts_only_df_table, mssql_contacts_only_df, mssql_contacts_only_df_dtypes, drop_table = True)

# query existing Contacts from salesforce
# query string to select records from salesforce
contact_query = "SELECT Id, FirstName, LastName FROM Contact WHERE Migrated_Record__c = True"
# query salesforce and return the contacts to be deleted
contact_query_results = SF_Utils.query_salesforce(sf, contact_query)

# convert query results to a dataframe
sf_contacts_df = SF_Utils.load_query_with_lookups_into_dataframe(contact_query_results)
# encode the dataframe before uploading to delete
sf_contacts_df = Utils.encode_df(sf_contacts_df)

# perform merge of staging contacts and salesforce contacts
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_contacts_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV contacts
    both_df, sf_contacts_only_df, contacts_to_insert_df = Utils.get_df_diffs(sf_contacts_df, contacts_with_accounts_df, left_on = ['FirstName', 'LastName'], right_on = ['first_name', 'last_name'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # drop id columns and _merge column
    contacts_to_insert_df.drop(['Id_SF', 'Id_STG', '_merge'], axis = 1, inplace = True)

    # remove _merge columns
    sf_contacts_only_df.drop(["_merge"], axis = 1, inplace = True)
    both_df.drop(["_merge"], axis = 1, inplace = True)

    # generate datatypes list for each dataframe of accounts in both systems and sf only accounts
    sf_contacts_only_dtypes = Utils.get_dtypes_as_list(sf_contacts_only_df)
    both_df_dtypes = Utils.get_dtypes_as_list(both_df)

    # mssql table name the dataframe is being inserted into
    sf_contacts_only_table = "[dbo].[Account_102_sf_contacts_only]"

    # mssql table name the dataframe is being inserted into
    both_df_table = "[dbo].[Account_102_contact_exists_in_sf]"

    # upload success records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, sf_contacts_only_table, sf_contacts_only_df, sf_contacts_only_dtypes, drop_table = True)
    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, both_df_table, both_df, both_df_dtypes, drop_table = True)

else:
    # if there are no matching contacts in SF, copy and load the entire staging table dataframe with related account Ids
    contacts_to_insert_df = contacts_with_accounts_df


# add english to list of languages each contact knows
contacts_to_insert_df["languages"] = np.where((contacts_to_insert_df["languages"] == 'English') | (contacts_to_insert_df["languages"] == 'english'),
                                              contacts_to_insert_df["languages"],
                                              "English, " + contacts_to_insert_df["languages"])

# rename columns to Salesforce field naming conventions
contacts_to_insert_df.rename(columns = {"Id":"AccountId",
                                        "first_name" : "FirstName",
                                        "last_name" : "LastName",
                                        "department" : "Department",
                                        "email" : "Email",
                                        "title" : "Title",
                                        "languages" : "Languages__c"}, inplace = True)

# add migrated record tag
contacts_to_insert_df['Migrated_Record__c'] = True

# insert net new records into salesforce Contact object
# upload the records to salesforce
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, contacts_to_insert_df, object, 'insert', success_file, fallout_file)

# mssql table name the dataframe is being inserted into
success_table_name = "[dbo].[Account_102_Success]"
# mssql table name the dataframe is being inserted into
fallout_table_name = "[dbo].[Account_102_Fallout]"

# generate column types from passing and fallout dataframes,
# should always be the same so redundant to run for each.
passing_dtypes = Utils.get_dtypes_as_list(passing_df)
fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)
# upload fallout records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)
