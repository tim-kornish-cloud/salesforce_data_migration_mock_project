"""
Author: Timothy Kornish
CreatedDate: June - 13 - 2026
Description: Log into mssql server, query contract line staging table records.
             Log into salesforce and query existing quotes and quote lines.
             Merge SF.SBQQ__Quote__C.Quote_External_ID__c = MSSQL.Contract_Line.Contract_Number
             from both systems to determine what related records exist
             in both systems, which records are net new, and which are missing prior attempted quotes, or other issues.
             Insert net new records into salesforce SBQQ__QuoteLine__c table.
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
object = 'SBQQ__QuoteLine__c'
# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# success file path
success_file = dir_path + "\\Output\\INSERT\\SUCCESS_Insert_" + environment + "_" + object + "_" + database + ".csv"
# fallout file path
fallout_file = dir_path + "\\Output\\INSERT\\FALLOUT_Insert_" + environment + "_" + object + "_" + database + ".csv"

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT scl.[contract_line_number]
      ,scl.[contract_number]
      ,scl.[product]
      ,scl.[quantity]
      ,sc.[start_date],
      sc.[end_date]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contract_Lines] as scl
  INNER JOIN [Data_Engineering].[dbo].[STG_SOURCE_Contracts] as sc ON sc.contract_number = scl.contract_number
  ORDER BY contract_number"""

# accounts in the mssql table shown in the query above
stg_contract_line_df = MSSQL_Utils.query_mssql_return_dataframe(select_query, cursor)

stg_contract_line_df = SF_Utils.format_date_to_salesforce_date(stg_contract_line_df, "start_date")
stg_contract_line_df = SF_Utils.format_date_to_salesforce_date(stg_contract_line_df, "end_date")

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

# query existing quotes from salesforce
# query string to select records from salesforce
quote_query = "SELECT Id, Quote_External_ID__c FROM SBQQ__Quote__c WHERE Migrated_Record__c = True"
# query salesforce and return the quotes to be deleted
quote_query_results = SF_Utils.query_salesforce(sf, quote_query)

# convert query results to a dataframe
sf_quotes_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_query_results)
# encode the dataframe before uploading to delete
sf_quotes_df = Utils.encode_df(sf_quotes_df)

# perform merge of staging quotes and salesforce quotes
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_quotes_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV quotes
    contract_lines_with_quotes_df, sf_quotes_only_df, mssql_contract_lines_only_df = Utils.get_df_diffs(sf_quotes_df, stg_contract_line_df, left_on = ['Quote_External_ID__c'], right_on = ['contract_number'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # remove unneccessary column for insert
    contract_lines_with_quotes_df.drop(["Quote_External_ID__c", "_merge"], axis = 1, inplace = True)
    # rename Id column from merge to SBQQ__Quote__c
    contract_lines_with_quotes_df.rename(columns = {"Id" : "SBQQ__Quote__c"}, inplace = True)

    # remove _merge columns for reporting
    sf_quotes_only_df.drop(['_merge'], axis = 1, inplace = True)
    mssql_contract_lines_only_df.drop(['_merge'], axis = 1, inplace = True)

    # generate datatypes list for each dataframe of accounts in both systems and sf only accounts
    sf_quotes_only_df_dtypes = Utils.get_dtypes_as_list(sf_quotes_only_df)
    mssql_contract_lines_only_df_dtypes = Utils.get_dtypes_as_list(mssql_contract_lines_only_df)

    # mssql table name the dataframe is being inserted into
    sf_quotes_only_df_table = "[dbo].[trgt_105_SBQQ__QuoteLine__c_sf_only_quote]"
    # mssql table name the dataframe is being inserted into
    mssql_contract_lines_only_df_table = "[dbo].[trgt_105_SBQQ__QuoteLine__c_stg_only_contract_line]"

    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, sf_quotes_only_df_table, sf_quotes_only_df, sf_quotes_only_df_dtypes, drop_table = True)
    # upload success records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, mssql_contract_lines_only_df_table, mssql_contract_lines_only_df, mssql_contract_lines_only_df_dtypes, drop_table = True)

# Query SF Products - map product to sf product
# product mapping from source product name to salesforce product name
product_mapping  = {"diesel-large" : "GenWatt Diesel 1000kW",
                    "diesel-low" : "GenWatt Diesel 10kW",
                    "diesel-medium" : "GenWatt Diesel 200kW",
                    "gasoline-large" : "GenWatt Gasoline 2000kW",
                    "gasoline-low" : "GenWatt Gasoline 300kW",
                    "gasoline-medium" : "GenWatt Gasoline 750kW",
                    "install - high" : "Installation: Industrial - High",
                    "install - low" : "Installation: Industrial - Low",
                    "install - medium" : "Installation: Industrial - Medium",
                    "install - portable" : "Installation: Portable",
                    "propane-large" : "GenWatt Propane 1500kW",
                    "propane-low" : "GenWatt Propane 100kW",
                    "propane-medium" : "GenWatt Propane 500kW",
                    "sla - Bronze" : "SLA: Bronze",
                    "sla - Gold" : "SLA: Gold",
                    "sla - Platinum" : "SLA: Platinum",
                    "sla - Silver" : "SLA: Silver"}

# replace products in df with mapped values
contract_lines_with_quotes_df["product"] = contract_lines_with_quotes_df["product"].replace(product_mapping)

# query existing products from salesforce
# query string to select records from salesforce
product_query = "SELECT Id, Name FROM Product2"
# query salesforce and return the products to be deleted
product_query_results = SF_Utils.query_salesforce(sf, product_query)

# convert query results to a dataframe
sf_products_df = SF_Utils.load_query_with_lookups_into_dataframe(product_query_results)
# encode the dataframe before uploading to delete
sf_products_df = Utils.encode_df(sf_products_df)

# perform merge of staging products and salesforce products
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_products_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV products
    contract_lines_with_products_df, sf_only_products_df, mssql_contract_lines_only_df = Utils.get_df_diffs(sf_products_df, contract_lines_with_quotes_df, left_on = ['Name'], right_on = ['product'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # remove unneccessary column for insert
    contract_lines_with_products_df.drop(["Name", "_merge"], axis = 1, inplace = True)
    # rename product Id to SBQQ__Product__c
    contract_lines_with_products_df.rename(columns = {"Id" : "SBQQ__Product__c"}, inplace = True)

    # remove _merge columns for reporting
    sf_only_products_df.drop(['_merge'], axis = 1, inplace = True)
    mssql_contract_lines_only_df.drop(['_merge'], axis = 1, inplace = True)

    # generate datatypes list for each dataframe of accounts in both systems and sf only accounts
    sf_only_products_df_dtypes = Utils.get_dtypes_as_list(sf_only_products_df)
    mssql_contract_lines_only_df_dtypes = Utils.get_dtypes_as_list(mssql_contract_lines_only_df)

    # mssql table name the dataframe is being inserted into
    sf_only_products_df_table = "[dbo].[trgt_105_SBQQ__QuoteLine__c_sf_only_products]"
    # mssql table name the dataframe is being inserted into
    mssql_contract_lines_only_df_table = "[dbo].[trgt_105_SBQQ__QuoteLine__c_contract_line_no_sf_product]"

    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, sf_only_products_df_table, sf_only_products_df, sf_only_products_df_dtypes, drop_table = True)
    # upload success records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, mssql_contract_lines_only_df_table, mssql_contract_lines_only_df, mssql_contract_lines_only_df_dtypes, drop_table = True)

# Query Pricebook entries to get list unit price for mapped Products
# query existing Pricebook entries from salesforce
# query string to select records from salesforce
pricebookentry_query = "SELECT Id, Product2Id, UnitPrice FROM PricebookEntry WHERE PriceBook2.IsStandard = True"
# query salesforce and return the Pricebook entries
pricebookentry_query_results = SF_Utils.query_salesforce(sf, pricebookentry_query)
# convert query results to a dataframe
sf_pricebookentry_df = SF_Utils.load_query_with_lookups_into_dataframe(pricebookentry_query_results)
# encode the dataframe before uploading to delete
sf_pricebookentry_df = Utils.encode_df(sf_pricebookentry_df)

# perform merge of staging pricebookentry and salesforce pricebookentry
# cannot merge a df with empty df, check if any salesforce migrated records exist
if len(sf_pricebookentry_df) != 0:
    # merge the csv data with the salesforce data to match SF Ids to the CSV pricebookentry
    contract_lines_with_pricebookentry_df, sf_pricebookentry_only_df, mssql_contract_lines_only_df = Utils.get_df_diffs(sf_pricebookentry_df, contract_lines_with_products_df, left_on = ['Product2Id'], right_on = ['SBQQ__Product__c'], how = 'outer', suffixes = ('_SF', '_STG'), indicator = True)
    # remove unneccessary column for insert
    contract_lines_with_pricebookentry_df.drop(["Product2Id", "_merge"], axis = 1, inplace = True)
    # rename pricebookentry Id and unit price
    contract_lines_with_pricebookentry_df.rename(columns = {"Id" : "SBQQ__PricebookEntryId__c", "UnitPrice" : "SBQQ__ListPrice__c"}, inplace = True)

    # remove _merge columns for reporting
    sf_pricebookentry_only_df.drop(['_merge'], axis = 1, inplace = True)
    mssql_contract_lines_only_df.drop(['_merge'], axis = 1, inplace = True)

    # generate datatypes list for each dataframe of accounts in both systems and sf only accounts
    sf_pricebookentry_only_df_dtypes = Utils.get_dtypes_as_list(sf_pricebookentry_only_df)
    mssql_contract_lines_only_df_dtypes = Utils.get_dtypes_as_list(mssql_contract_lines_only_df)

    # mssql table name the dataframe is being inserted into
    sf_pricebookentry_only_df_table = "[dbo].[trgt_105_SBQQ__QuoteLine__c_pricebook_missing_product]"
    # mssql table name the dataframe is being inserted into
    mssql_contract_lines_only_df_table = "[dbo].[trgt_105_SBQQ__QuoteLine__c_stg_contract_line_no_pricebook]"

    # upload fallout records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, sf_pricebookentry_only_df_table, sf_pricebookentry_only_df, sf_pricebookentry_only_df_dtypes, drop_table = True)
    # upload success records to reporting table
    MSSQL_Utils.upload_reports(connection, cursor, mssql_contract_lines_only_df_table, mssql_contract_lines_only_df, mssql_contract_lines_only_df_dtypes, drop_table = True)



# if name contains install, number = 2, if name contains sla, number = 3, else number = 1
# set SBQQ__Number__c based on product type
contract_lines_with_pricebookentry_df["SBQQ__Number__c"] = np.where(contract_lines_with_pricebookentry_df['product'].str.contains('GenWatt'), 1,
                                                                    np.where(contract_lines_with_pricebookentry_df['product'].str.contains('SLA'), 2, 3))
# drop extra columns not needed for insert
contract_lines_with_pricebookentry_df.drop(["contract_number", "product"], axis = 1, inplace = True)
# rename staging table column names to salesforce column names
contract_lines_with_pricebookentry_df.rename(columns = {"quantity" : "SBQQ__Quantity__c",
                                                        "contract_line_number" : "QuoteLine_External_ID__c",
                                                        "start_date" : "SBQQ__StartDate__c",
                                                        "end_date" : "SBQQ__EndDate__c"}, inplace = True)

# format start and end date of quote lines
#contract_lines_with_pricebookentry_df = SF_Utils.format_date_to_salesforce_date(contract_lines_with_pricebookentry_df, "SBQQ__StartDate__c")
#contract_lines_with_pricebookentry_df = SF_Utils.format_date_to_salesforce_date(contract_lines_with_pricebookentry_df, "SBQQ__EndDate__c")

# set pricing method to list
contract_lines_with_pricebookentry_df["SBQQ__PricingMethod__c"] = "List"
# set SBQQ__SubscriptionPricing__c to fixed price
contract_lines_with_pricebookentry_df["SBQQ__SubscriptionPricing__c"] = "Fixed Price"
# add migrated record
contract_lines_with_pricebookentry_df['Migrated_Record__c'] = True
# set subscription term to 12
contract_lines_with_pricebookentry_df['SBQQ__SubscriptionTerm__c'] = 12
# copy list price of quote line to unit cost since all quotelines are quantity of 1
contract_lines_with_pricebookentry_df['SBQQ__UnitCost__c'] = contract_lines_with_pricebookentry_df['SBQQ__ListPrice__c']

# insert net new records into salesforce QuoteLine object
# upload the records to salesforce
passing_df, fallout_df = SF_Utils.upload_dataframe_to_salesforce(sf, contract_lines_with_pricebookentry_df, object, 'insert', success_file, fallout_file)

# mssql table name the dataframe is being inserted into
success_table_name = "[dbo].[trgt_105_SBQQ__QuoteLine__c_insert_Success]"
# mssql table name the dataframe is being inserted into
fallout_table_name = "[dbo].[trgt_105_SBQQ__QuoteLine__c_insert_Fallout]"

# generate column types from passing and fallout dataframes,
# should always be the same so redundant to run for each.
passing_dtypes = Utils.get_dtypes_as_list(passing_df)
fallout_dtypes = Utils.get_dtypes_as_list(fallout_df)

# upload success records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, success_table_name, passing_df, passing_dtypes, drop_table = True)
# upload fallout records to reporting table
MSSQL_Utils.upload_reports(connection, cursor, fallout_table_name, fallout_df, fallout_dtypes, drop_table = True)
