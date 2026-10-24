"""
Author: Timothy Kornish
CreatedDate: October - 23 - 2026
Description: setup tests to validate the source data from mysql db
             is properly setup in mssql database tables.

"""

import numpy as np
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
    sf_account_query = """SELECT
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

    mssql_account_query = """SELECT
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
    account_query_results = SF_Utils.query_salesforce(sf, sf_account_query)
    # convert query results to a dataframe
    sf_accounts_df = SF_Utils.load_query_with_lookups_into_dataframe(account_query_results)
    # query mssql account records
    mssql_account_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_account_query, mssql_connection)
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

def test_migrated_contacts(setup_salesforce_connection, setup_mssql_connection):
    sf_contact_query = """SELECT
	Account.Account_Number_External_ID__c,
    FirstName,
    LastName,
	Title,
	Email,
	Department,
	Languages__c
    FROM Contact
    WHERE Migrated_Record__c = TRUE
    ORDER BY Account.Account_Number_External_ID__c, LastName, FirstName
    """

    mssql_contact_query = """SELECT
       [account_number_external_id]
      ,[first_name]
      ,[last_name]
      ,[email]
      ,[title]
      ,[department]
      ,[languages]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contacts]
  ORDER BY [account_number_external_id],[last_name],[first_name]"""

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated contacts
    contact_query_results = SF_Utils.query_salesforce(sf, sf_contact_query)
    # convert query results to a dataframe
    sf_contacts_df = SF_Utils.load_query_with_lookups_into_dataframe(contact_query_results)
    # query mssql contact records
    mssql_contact_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_contact_query, mssql_connection)
    # rename salesforce column names to match mssql column names
    sf_contacts_df.rename(columns = {"Account.Account_Number_External_ID__c" : "account_number_external_id",
                                    "FirstName" : "first_name",
                                	"LastName" : "last_name",
                                    "Email" : "email",
                                    "Title" : "title",
                                	"Department" : "department",
                                	"Languages__c" : "languages"}, inplace = True)
    # add english to list of languages each contact knows
    mssql_contact_df["languages"] = np.where((mssql_contact_df["languages"] == 'English') | (mssql_contact_df["languages"] == 'english'),
                                           mssql_contact_df["languages"], "English, " + mssql_contact_df["languages"])
    # only compare the mssql staging contacts with what is loaded into salesforce
    mssql_contact_df = mssql_contact_df[mssql_contact_df['account_number_external_id'].isin(sf_contacts_df['account_number_external_id'])]
    # sort columns in both dataframes to have same order
    sf_contacts_df = sf_contacts_df[sorted(sf_contacts_df.columns)]
    mssql_contact_df = mssql_contact_df[sorted(mssql_contact_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_contact_df = mssql_contact_df.astype(sf_contacts_df.dtypes)
    # reset index for comparison
    sf_contacts_df.reset_index(drop=True, inplace = True)
    mssql_contact_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_contacts_df, mssql_contact_df)

def test_migrated_opportunities(setup_salesforce_connection, setup_mssql_connection):
    sf_opportunity_query = """SELECT
	Name,
	Account.Account_Number_External_ID__c,
	CloseDate,
	Opportunity_External_ID__c,
	Pricebook2Id,
	StageName
    FROM Opportunity
    WHERE Migrated_Record__c = True
    ORDER BY Account.Account_Number_External_ID__c, Name
    """

    mssql_select_query = """SELECT [account_number_external_id]
          ,[contract_number]
          ,[start_date]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]
      ORDER BY [account_number_external_id], [contract_number]"""

    sf_pricebook2_query = "SELECT Id, IsStandard, Name FROM Pricebook2 WHERE IsStandard = True"

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated opportunities
    opportunities_query_results = SF_Utils.query_salesforce(sf, sf_opportunity_query)
    # convert query results to a dataframe
    sf_opportunities_df = SF_Utils.load_query_with_lookups_into_dataframe(opportunities_query_results)
    # query salesforce and return the migrated pricebook2
    pricebook2_query_results = SF_Utils.query_salesforce(sf, sf_pricebook2_query)
    # get pricebook id to add to staged data for comparison
    sf_pricebook2_df = SF_Utils.load_query_with_lookups_into_dataframe(pricebook2_query_results)
    # query mssql opportunities records
    mssql_opportunities_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename salesforce column names to match mssql column names
    mssql_opportunities_df.rename(columns = {"account_number_external_id" : "Account.Account_Number_External_ID__c",
                                             "contract_number" : "Name",
                                	         "start_date" : "CloseDate"}, inplace = True)
    # only compare the mssql staging opportunities with what is loaded into salesforce
    mssql_opportunities_df = mssql_opportunities_df[mssql_opportunities_df['Name'].isin(sf_opportunities_df['Opportunity_External_ID__c'])]
    # add pricebook to mssql opportunity dataframe for comparison
    mssql_opportunities_df["Pricebook2Id"] = sf_pricebook2_df.iloc[0,0]
    # add stage name to mssql opportunity dataframe for comparison
    mssql_opportunities_df["StageName"] = "Closed Won"
    # copy name field to Opportunity_External_ID__c
    mssql_opportunities_df["Opportunity_External_ID__c"] = mssql_opportunities_df["Name"]
    # sort columns in both dataframes to have same order
    sf_opportunities_df = sf_opportunities_df[sorted(sf_opportunities_df.columns)]
    mssql_opportunities_df = mssql_opportunities_df[sorted(mssql_opportunities_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_opportunities_df = mssql_opportunities_df.astype(sf_opportunities_df.dtypes)
    # reset index for comparison
    sf_opportunities_df.reset_index(drop=True, inplace = True)
    mssql_opportunities_df.reset_index(drop=True, inplace = True)

    # assert the source table is equal to the target table
    assert_frame_equal(sf_opportunities_df, mssql_opportunities_df)

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
