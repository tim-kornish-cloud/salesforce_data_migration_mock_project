"""
Author: Timothy Kornish
CreatedDate: October - 23 - 2026
Description: setup tests to validate the source data from mysql db
             is properly setup in mssql database tables.

"""

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from tests.conftest import setup_salesforce_connection, setup_mssql_connection
from custom_db_utilities import MSSQL_Utilities, Salesforce_Utilities, Custom_Utilities

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# set up utils instance
Utils = Custom_Utilities()

pd.set_option('display.max_columns', None)

def test_migrated_accounts(setup_salesforce_connection, setup_mssql_connection):
    sf_select_query = """SELECT
	Account_Number_External_ID__c,
    Phone,
	Name,
    Industry,
    AnnualRevenue,
	NumberofLocations__c,
	NumberOfEmployees,
	SLA__c,
	SLASerialNumber__c
    FROM Account
    WHERE Migrated_Record__c = TRUE
    ORDER BY Account_Number_External_ID__c
    """

    mssql_select_query = """SELECT
          [account_number_external_id]
          ,[phone]
          ,[company_name]
          ,[industry]
          ,[annual_revenue]
          ,[number_of_locations]
          ,[number_of_employees]
          ,[sla]
          ,[sla_serial_number]
    FROM [Data_Engineering].[dbo].[STG_SOURCE_Accounts]
    ORDER BY account_number_external_id"""

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated accounts
    account_query_results = SF_Utils.query_salesforce(sf, sf_select_query)
    # convert query results to a dataframe
    sf_accounts_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
    # encode the dataframe before uploading to delete
    sf_accounts_df = Utils.encode_df(sf_accounts_df)
    # query mssql account records
    mssql_account_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename salesforce column names to match mssql column names
    sf_accounts_df.rename(columns = {"Phone" : "phone",
                                	"Name" : "company_name",
                                    "Industry" : "industry",
                                    "AnnualRevenue" : "annual_revenue",
                                	"Account_Number_External_ID__c" : "account_number_external_id",
                                	"NumberofLocations__c" : "number_of_locations",
                                	"NumberOfEmployees" : "number_of_employees",
                                	"SLA__c" : "sla",
                                	"SLASerialNumber__c" : "sla_serial_number"}, inplace = True)
    # only compare the mssql staging accounts with what is loaded into salesforce
    mssql_account_df = mssql_account_df[mssql_account_df['account_number_external_id'].isin(sf_accounts_df['account_number_external_id'])]
    # convert nan to empty string in industry column
    sf_accounts_df['industry'] = sf_accounts_df['industry'].fillna('')
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_account_df = mssql_account_df.astype(sf_accounts_df.dtypes)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_accounts_df.reset_index(drop=True), mssql_account_df.reset_index(drop=True))

# def test_migrated_contacts(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_opportunities(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_quotes(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_quote_lines(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_orders(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_order_lines(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_contracts(setup_salesforce_connection, setup_mssql_connection):
#     pass
#
# def test_migrated_contract_lines(setup_salesforce_connection, setup_mssql_connection):
#     pass
