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

# create and instance of the custom salesforce utilities class used to interact with MSSQL
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# set up utils instance
Utils = Custom_Utilities()

pd.set_option('display.max_columns', None)

def test_migrated_accounts(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the account is migrated into Salesforce.
    This test can be run immediately after the accounts migration or after the entire
    end to end migration process.
    """
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
    """Description: run this test after the contact is migrated into Salesforce.
    This test can be run immediately after the contacts migration or after the entire
    end to end migration process.
    """
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
    """Description: run this test after the opportunity is migrated into Salesforce.
    This test can be run immediately after the opportunities migration or after the entire
    end to end migration process.

    This test does not confirm the primary checkbox on the related quote is checked.
    confirming the proper quote is related requires an update to the validation and
    could only be run after quotes are migrated and calcualted.
    """
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
    # rename mssql column names to match salesforce column names
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

def test_migrated_quotes_before_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the quotes are migrated into Salesforce.
    This test can only be run immediately after the quote migration on insert,
    running after updates will change the data and cause a failure.
    """
    sf_quote_query = """SELECT
	Quote_External_ID__c,
	SBQQ__Account__r.Account_Number_External_ID__c,
	SBQQ__ContractingMethod__c,
	SBQQ__EndDate__c,
	SBQQ__Opportunity2__r.Opportunity_External_ID__c,
	SBQQ__Ordered__c,
	SBQQ__PriceBook__c,
	SBQQ__Primary__c,
	SBQQ__StartDate__c,
	SBQQ__Status__c,
	SBQQ__SubscriptionTerm__c,
	SBQQ__Type__c
    FROM SBQQ__Quote__c
    WHERE Migrated_Record__c = True
    ORDER BY SBQQ__Account__r.Account_Number_External_ID__c, Quote_External_ID__c
    """

    mssql_select_query = """SELECT [account_number_external_id]
          ,[contract_number]
          ,[start_date]
          ,[end_date]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]
      ORDER BY [account_number_external_id], [contract_number]"""

    sf_pricebook2_query = "SELECT Id, IsStandard, Name FROM Pricebook2 WHERE IsStandard = True"

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated quote
    quote_query_results = SF_Utils.query_salesforce(sf, sf_quote_query)
    # convert query results to a dataframe
    sf_quote_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_query_results)
    # query salesforce and return the migrated pricebook2
    pricebook2_query_results = SF_Utils.query_salesforce(sf, sf_pricebook2_query)
    # get pricebook id to add to staged data for comparison
    sf_pricebook2_df = SF_Utils.load_query_with_lookups_into_dataframe(pricebook2_query_results)
    # query mssql quote records
    mssql_quote_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_quote_df.rename(columns = {"account_number_external_id" : "SBQQ__Account__r.Account_Number_External_ID__c",
                                             "contract_number" : "Quote_External_ID__c",
                                	         "start_date" : "SBQQ__StartDate__c",
                                             "end_date" : "SBQQ__EndDate__c"}, inplace = True)
    # only compare the mssql staging quotes with what is loaded into salesforce
    mssql_quote_df = mssql_quote_df[mssql_quote_df['Quote_External_ID__c'].isin(sf_quote_df['Quote_External_ID__c'])]
    # add pricebook to mssql quote dataframe for comparison
    mssql_quote_df["SBQQ__PriceBook__c"] = sf_pricebook2_df.iloc[0,0]
    # set contracting method, stays the same after update
    mssql_quote_df["SBQQ__ContractingMethod__c"] = "By Subscription End Date"
    # set ordered to false, will be true in after update test
    mssql_quote_df["SBQQ__Ordered__c"] = False
    # set primary to false, will be true in after update test
    mssql_quote_df["SBQQ__Primary__c"] = False
    # set status to draft, will be Accepted after update test
    mssql_quote_df["SBQQ__Status__c"] = "Draft"
    # set subscription term of contract to 12, will stay same after update
    mssql_quote_df["SBQQ__SubscriptionTerm__c"] = 12
    # set quote type to quote, will stay the same after update
    mssql_quote_df["SBQQ__Type__c"] = "Quote"
    # copy name field to Opportunity_External_ID__c
    mssql_quote_df["SBQQ__Opportunity2__r.Opportunity_External_ID__c"] = mssql_quote_df["Quote_External_ID__c"]
    # sort columns in both dataframes to have same order
    sf_quote_df = sf_quote_df[sorted(sf_quote_df.columns)]
    mssql_quote_df = mssql_quote_df[sorted(mssql_quote_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_quote_df = mssql_quote_df.astype(sf_quote_df.dtypes)
    # reset index for comparison
    sf_quote_df.reset_index(drop=True, inplace = True)
    mssql_quote_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_quote_df, mssql_quote_df)

def test_migrated_quote_lines_before_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the quote lines are migrated into Salesforce.
    This test can only be run immediately after the quote migration on insert,
    running after updates will change the data and cause a failure.
    """
    sf_quote_lines_query = """SELECT
    QuoteLine_External_ID__c,
	SBQQ__EndDate__c,
	SBQQ__ListPrice__c,
	SBQQ__Number__c,
	SBQQ__PricingMethod__c,
	SBQQ__Product__r.Name,
	SBQQ__Quote__r.Quote_External_ID__c,
    SBQQ__Quantity__c,
	SBQQ__StartDate__c,
	SBQQ__SubscriptionPricing__c,
	SBQQ__SubscriptionTerm__c,
	SBQQ__UnitCost__c
    FROM SBQQ__QuoteLine__c
    WHERE Migrated_Record__c = True
    ORDER BY SBQQ__Quote__r.Quote_External_ID__c, QuoteLine_External_ID__c
    """

    mssql_select_query = """SELECT
           [CON].[contract_number]
          ,[contract_line_number]
          ,[start_date]
          ,[end_date]
          ,[product]
          ,[quantity]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts] AS CON
      INNER JOIN [Data_Engineering].[dbo].[STG_SOURCE_Contract_Lines] AS CON_LINE on [CON].[contract_number] =  [CON_LINE].[contract_number]
      ORDER BY [CON].[contract_number], [CON_LINE].[contract_line_number]"""

    pricebookentry_query = "SELECT Product2.Name, UnitPrice FROM PricebookEntry WHERE PriceBook2.IsStandard = True"

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated quote
    quote_lines_query_results = SF_Utils.query_salesforce(sf, sf_quote_lines_query)
    # convert query results to a dataframe
    sf_quote_lines_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_lines_query_results)
    # query salesforce and return the Pricebook entries
    pricebookentry_query_results = SF_Utils.query_salesforce(sf, pricebookentry_query)
    # convert query results to a dataframe
    sf_pricebookentry_df = SF_Utils.load_query_with_lookups_into_dataframe(pricebookentry_query_results)
    # query mssql quote records
    mssql_quote_lines_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_quote_lines_df.rename(columns = {"contract_number" : "SBQQ__Quote__r.Quote_External_ID__c",
                                           "contract_line_number" : "QuoteLine_External_ID__c",
                                	       "start_date" : "SBQQ__StartDate__c",
                                           "end_date" : "SBQQ__EndDate__c",
                                           "product" : "SBQQ__Product__r.Name",
                                           "quantity" : "SBQQ__Quantity__c"}, inplace = True)
    # only compare the mssql staging quotes with what is loaded into salesforce
    mssql_quote_lines_df = mssql_quote_lines_df[mssql_quote_lines_df['QuoteLine_External_ID__c'].isin(sf_quote_lines_df['QuoteLine_External_ID__c'])]
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
    mssql_quote_lines_df["SBQQ__Product__r.Name"] = mssql_quote_lines_df["SBQQ__Product__r.Name"].replace(product_mapping)
    # merge mssql contract lines with pricebook entries to get unit cose and list price which are the same value called: UnitPrice
    mssql_quote_lines_df = Utils.merge_dfs(left = mssql_quote_lines_df, right = sf_pricebookentry_df, left_on = "SBQQ__Product__r.Name", right_on = "Product2.Name", how = "inner", indicator = False)
    # set SBQQ__Number__c based on product type
    mssql_quote_lines_df["SBQQ__Number__c"] = np.where(mssql_quote_lines_df['SBQQ__Product__r.Name'].str.contains('GenWatt'), 1,
                                                       np.where(mssql_quote_lines_df['SBQQ__Product__r.Name'].str.contains('SLA'), 2, 3))
    # add pricing fields to mssql df for comparison
    mssql_quote_lines_df["SBQQ__ListPrice__c"] = mssql_quote_lines_df["UnitPrice"]
    mssql_quote_lines_df["SBQQ__PricingMethod__c"] = "List"
    mssql_quote_lines_df["SBQQ__SubscriptionPricing__c"] = "Fixed Price"
    mssql_quote_lines_df["SBQQ__SubscriptionTerm__c"] = 12
    mssql_quote_lines_df["SBQQ__UnitCost__c"] = mssql_quote_lines_df["UnitPrice"]
    # drop extra columns from merge with pricebook entries
    mssql_quote_lines_df.drop(["Product2.Name", "UnitPrice"], axis = 1, inplace = True)
    # sort columns in both dataframes to have same order
    sf_quote_lines_df = sf_quote_lines_df[sorted(sf_quote_lines_df.columns)]
    mssql_quote_lines_df = mssql_quote_lines_df[sorted(mssql_quote_lines_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_quote_lines_df = mssql_quote_lines_df.astype(sf_quote_lines_df.dtypes)
    # reset index for comparison
    sf_quote_lines_df.reset_index(drop=True, inplace = True)
    mssql_quote_lines_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_quote_lines_df, mssql_quote_lines_df)

def test_migrated_quotes_after_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the quotes are migrated into Salesforce.
    This test can only be run immediately after the quote migration is updated.
    Running before updates will cause a failure.
    """
    sf_quote_query = """SELECT
	Quote_External_ID__c,
	SBQQ__Account__r.Account_Number_External_ID__c,
	SBQQ__ContractingMethod__c,
	SBQQ__EndDate__c,
	SBQQ__Opportunity2__r.Opportunity_External_ID__c,
	SBQQ__Ordered__c,
	SBQQ__PriceBook__c,
	SBQQ__Primary__c,
	SBQQ__StartDate__c,
	SBQQ__Status__c,
	SBQQ__SubscriptionTerm__c,
	SBQQ__Type__c
    FROM SBQQ__Quote__c
    WHERE Migrated_Record__c = True
    ORDER BY SBQQ__Account__r.Account_Number_External_ID__c, Quote_External_ID__c
    """

    mssql_select_query = """SELECT [account_number_external_id]
          ,[contract_number]
          ,[start_date]
          ,[end_date]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]
      ORDER BY [account_number_external_id], [contract_number]"""

    sf_pricebook2_query = "SELECT Id, IsStandard, Name FROM Pricebook2 WHERE IsStandard = True"

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated quote
    quote_query_results = SF_Utils.query_salesforce(sf, sf_quote_query)
    # convert query results to a dataframe
    sf_quote_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_query_results)
    # query salesforce and return the migrated pricebook2
    pricebook2_query_results = SF_Utils.query_salesforce(sf, sf_pricebook2_query)
    # get pricebook id to add to staged data for comparison
    sf_pricebook2_df = SF_Utils.load_query_with_lookups_into_dataframe(pricebook2_query_results)
    # query mssql quote records
    mssql_quote_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_quote_df.rename(columns = {"account_number_external_id" : "SBQQ__Account__r.Account_Number_External_ID__c",
                                             "contract_number" : "Quote_External_ID__c",
                                	         "start_date" : "SBQQ__StartDate__c",
                                             "end_date" : "SBQQ__EndDate__c"}, inplace = True)
    # only compare the mssql staging quotes with what is loaded into salesforce
    mssql_quote_df = mssql_quote_df[mssql_quote_df['Quote_External_ID__c'].isin(sf_quote_df['Quote_External_ID__c'])]
    # add pricebook to mssql quote dataframe for comparison
    mssql_quote_df["SBQQ__PriceBook__c"] = sf_pricebook2_df.iloc[0,0]
    # set contracting method, stays the same after update
    mssql_quote_df["SBQQ__ContractingMethod__c"] = "By Subscription End Date"
    # set ordered to false, will be true in after update test
    mssql_quote_df["SBQQ__Ordered__c"] = True
    # set primary to false, will be true in after update test
    mssql_quote_df["SBQQ__Primary__c"] = True
    # set status to draft, will be Accepted after update test
    mssql_quote_df["SBQQ__Status__c"] = "Accepted"
    # set subscription term of contract to 12, will stay same after update
    mssql_quote_df["SBQQ__SubscriptionTerm__c"] = 12
    # set quote type to quote, will stay the same after update
    mssql_quote_df["SBQQ__Type__c"] = "Quote"
    # copy name field to Opportunity_External_ID__c
    mssql_quote_df["SBQQ__Opportunity2__r.Opportunity_External_ID__c"] = mssql_quote_df["Quote_External_ID__c"]
    # sort columns in both dataframes to have same order
    sf_quote_df = sf_quote_df[sorted(sf_quote_df.columns)]
    mssql_quote_df = mssql_quote_df[sorted(mssql_quote_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_quote_df = mssql_quote_df.astype(sf_quote_df.dtypes)
    # reset index for comparison
    sf_quote_df.reset_index(drop=True, inplace = True)
    mssql_quote_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_quote_df, mssql_quote_df)

def test_migrated_quote_lines_after_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the quote lines are migrated into Salesforce.
    This test can only be run immediately after the quote lines migration are updated.
    Running before updates will cause a failure.
    """
    sf_quote_lines_query = """SELECT
    QuoteLine_External_ID__c,
	SBQQ__EndDate__c,
	SBQQ__ListPrice__c,
	SBQQ__Number__c,
	SBQQ__PricingMethod__c,
	SBQQ__Product__r.Name,
	SBQQ__Quote__r.Quote_External_ID__c,
    SBQQ__Quantity__c,
	SBQQ__StartDate__c,
	SBQQ__SubscriptionPricing__c,
	SBQQ__SubscriptionTerm__c,
	SBQQ__UnitCost__c
    FROM SBQQ__QuoteLine__c
    WHERE Migrated_Record__c = True
    ORDER BY SBQQ__Quote__r.Quote_External_ID__c, QuoteLine_External_ID__c
    """

    mssql_select_query = """SELECT
           [CON].[contract_number]
          ,[contract_line_number]
          ,[start_date]
          ,[end_date]
          ,[product]
          ,[quantity]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts] AS CON
      INNER JOIN [Data_Engineering].[dbo].[STG_SOURCE_Contract_Lines] AS CON_LINE on [CON].[contract_number] =  [CON_LINE].[contract_number]
      ORDER BY [CON].[contract_number], [CON_LINE].[contract_line_number]"""

    pricebookentry_query = "SELECT Product2.Name, UnitPrice FROM PricebookEntry WHERE PriceBook2.IsStandard = True"

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated quote
    quote_lines_query_results = SF_Utils.query_salesforce(sf, sf_quote_lines_query)
    # convert query results to a dataframe
    sf_quote_lines_df = SF_Utils.load_query_with_lookups_into_dataframe(quote_lines_query_results)
    # query salesforce and return the Pricebook entries
    pricebookentry_query_results = SF_Utils.query_salesforce(sf, pricebookentry_query)
    # convert query results to a dataframe
    sf_pricebookentry_df = SF_Utils.load_query_with_lookups_into_dataframe(pricebookentry_query_results)
    # query mssql quote records
    mssql_quote_lines_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_quote_lines_df.rename(columns = {"contract_number" : "SBQQ__Quote__r.Quote_External_ID__c",
                                           "contract_line_number" : "QuoteLine_External_ID__c",
                                	       "start_date" : "SBQQ__StartDate__c",
                                           "end_date" : "SBQQ__EndDate__c",
                                           "product" : "SBQQ__Product__r.Name",
                                           "quantity" : "SBQQ__Quantity__c"}, inplace = True)
    # only compare the mssql staging quotes with what is loaded into salesforce
    mssql_quote_lines_df = mssql_quote_lines_df[mssql_quote_lines_df['QuoteLine_External_ID__c'].isin(sf_quote_lines_df['QuoteLine_External_ID__c'])]
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
    mssql_quote_lines_df["SBQQ__Product__r.Name"] = mssql_quote_lines_df["SBQQ__Product__r.Name"].replace(product_mapping)
    # merge mssql contract lines with pricebook entries to get unit cose and list price which are the same value called: UnitPrice
    mssql_quote_lines_df = Utils.merge_dfs(left = mssql_quote_lines_df, right = sf_pricebookentry_df, left_on = "SBQQ__Product__r.Name", right_on = "Product2.Name", how = "inner", indicator = False)
    # set SBQQ__Number__c based on product type
    mssql_quote_lines_df["SBQQ__Number__c"] = np.where(mssql_quote_lines_df['SBQQ__Product__r.Name'].str.contains('GenWatt'), 1,
                                                       np.where(mssql_quote_lines_df['SBQQ__Product__r.Name'].str.contains('SLA'), 2, 3))
    # add pricing fields to source df for comparison
    mssql_quote_lines_df["SBQQ__ListPrice__c"] = mssql_quote_lines_df["UnitPrice"]
    mssql_quote_lines_df["SBQQ__PricingMethod__c"] = "List"
    mssql_quote_lines_df["SBQQ__SubscriptionPricing__c"] = "Fixed Price"
    mssql_quote_lines_df["SBQQ__SubscriptionTerm__c"] = 12
    mssql_quote_lines_df["SBQQ__UnitCost__c"] = mssql_quote_lines_df["UnitPrice"]
    # drop extra columns from merge with pricebook entries
    mssql_quote_lines_df.drop(["Product2.Name", "UnitPrice"], axis = 1, inplace = True)
    # sort columns in both dataframes to have same order
    sf_quote_lines_df = sf_quote_lines_df[sorted(sf_quote_lines_df.columns)]
    mssql_quote_lines_df = mssql_quote_lines_df[sorted(mssql_quote_lines_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_quote_lines_df = mssql_quote_lines_df.astype(sf_quote_lines_df.dtypes)
    # reset index for comparison
    sf_quote_lines_df.reset_index(drop=True, inplace = True)
    mssql_quote_lines_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_quote_lines_df, mssql_quote_lines_df)

def test_migrated_orders_after_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the migrated quotes are ordered and orders generated.
    This test can only be run after the orders have dates updated, status = activated and contracted = true.
    Running after generated but before updates will cause a failure.
    """
    sf_order_query = """SELECT
	Account.Account_Number_External_ID__c,
	EffectiveDate,
	EndDate,
	SBQQ__Contracted__c,
	SBQQ__Quote__r.Quote_External_ID__c,
	Status
    FROM Order
    WHERE SBQQ__Quote__r.Migrated_Record__c = TRUE
    ORDER BY Account.Account_Number_External_ID__c, SBQQ__Quote__r.Quote_External_ID__c
    """

    mssql_select_query = """SELECT [account_number_external_id]
          ,[contract_number]
          ,[start_date]
          ,[end_date]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]
      ORDER BY [account_number_external_id], [contract_number]"""

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated quote
    order_query_results = SF_Utils.query_salesforce(sf, sf_order_query)
    # convert query results to a dataframe
    sf_order_df = SF_Utils.load_query_with_lookups_into_dataframe(order_query_results)
    # query mssql order records

    mssql_order_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_order_df.rename(columns = {"account_number_external_id" : "Account.Account_Number_External_ID__c",
                                             "contract_number" : "SBQQ__Quote__r.Quote_External_ID__c",
                                	         "start_date" : "EffectiveDate",
                                             "end_date" : "EndDate"}, inplace = True)
    # only compare the mssql staging orders with what is loaded into salesforce
    mssql_order_df = mssql_order_df[mssql_order_df['SBQQ__Quote__r.Quote_External_ID__c'].isin(sf_order_df['SBQQ__Quote__r.Quote_External_ID__c'])]
    # add contracted = true
    mssql_order_df["SBQQ__Contracted__c"] = True
    # add contracted = true
    mssql_order_df["Status"] = "Activated"
    # sort columns in both dataframes to have same order
    sf_order_df = sf_order_df[sorted(sf_order_df.columns)]
    mssql_order_df = mssql_order_df[sorted(mssql_order_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_order_df = mssql_order_df.astype(sf_order_df.dtypes)
    # reset index for comparison
    sf_order_df.reset_index(drop=True, inplace = True)
    mssql_order_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_order_df, mssql_order_df)

def test_migrated_order_lines_after_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the quote lines are migrated into Salesforce.
    This test can only be run immediately after the quote lines migration are updated.
    Running before updates will cause a failure.
    """
    sf_order_lines_query = """SELECT
	EndDate,
	Order.SBQQ__Quote__r.Quote_External_ID__c,
	Product2.Name,
	Quantity,
    SBQQ__OrderedQuantity__c,
	SBQQ__QuoteLine__r.QuoteLine_External_ID__c,
	ServiceDate
    FROM OrderItem
    WHERE Order.SBQQ__Quote__r.Migrated_Record__c = True
    ORDER BY Order.SBQQ__Quote__r.Quote_External_ID__c, SBQQ__QuoteLine__r.QuoteLine_External_ID__c
    """

    mssql_select_query = """SELECT
           [CON].[contract_number]
          ,[contract_line_number]
          ,[start_date]
          ,[end_date]
          ,[product]
          ,[quantity]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts] AS CON
      INNER JOIN [Data_Engineering].[dbo].[STG_SOURCE_Contract_Lines] AS CON_LINE on [CON].[contract_number] =  [CON_LINE].[contract_number]
      ORDER BY [CON].[contract_number], [CON_LINE].[contract_line_number]"""

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated order
    order_lines_query_results = SF_Utils.query_salesforce(sf, sf_order_lines_query)
    # convert query results to a dataframe
    sf_order_lines_df = SF_Utils.load_query_with_lookups_into_dataframe(order_lines_query_results)
    # query mssql order records
    mssql_order_lines_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_order_lines_df.rename(columns = {"contract_number" : "Order.SBQQ__Quote__r.Quote_External_ID__c",
                                           "contract_line_number" : "SBQQ__QuoteLine__r.QuoteLine_External_ID__c",
                                	       "start_date" : "ServiceDate",
                                           "end_date" : "EndDate",
                                           "product" : "Product2.Name",
                                           "quantity" : "Quantity"}, inplace = True)
    # only compare the mssql staging orders with what is loaded into salesforce
    mssql_order_lines_df = mssql_order_lines_df[mssql_order_lines_df['SBQQ__QuoteLine__r.QuoteLine_External_ID__c'].isin(sf_order_lines_df['SBQQ__QuoteLine__r.QuoteLine_External_ID__c'])]
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
    mssql_order_lines_df["Product2.Name"] = mssql_order_lines_df["Product2.Name"].replace(product_mapping)
    # add pricing fields to source df for comparison
    mssql_order_lines_df["SBQQ__OrderedQuantity__c"] = mssql_order_lines_df["Quantity"]
    # sort columns in both dataframes to have same order
    sf_order_lines_df = sf_order_lines_df[sorted(sf_order_lines_df.columns)]
    mssql_order_lines_df = mssql_order_lines_df[sorted(mssql_order_lines_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_order_lines_df = mssql_order_lines_df.astype(sf_order_lines_df.dtypes)
    # reset index for comparison
    sf_order_lines_df.reset_index(drop=True, inplace = True)
    mssql_order_lines_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_order_lines_df, mssql_order_lines_df)

def test_migrated_contracts_after_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the migrated contracts are activated.
    This test can only be run after the contracts are updated.
    Running before update will cause a failure.
    """
    sf_contract_query = """SELECT
	EndDate,
	SBQQ__Order__r.Account.Account_Number_External_ID__c,
	SBQQ__Order__r.SBQQ__Quote__r.Quote_External_ID__c,
	StartDate,
	Status
    FROM Contract
    WHERE SBQQ__Order__r.SBQQ__Quote__r.Migrated_Record__c = TRUE
    ORDER BY SBQQ__Order__r.Account.Account_Number_External_ID__c, SBQQ__Order__r.SBQQ__Quote__r.Quote_External_ID__c
    """

    mssql_select_query = """SELECT [account_number_external_id]
          ,[contract_number]
          ,[start_date]
          ,[end_date]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]
      ORDER BY [account_number_external_id], [contract_number]"""

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated quote
    contract_query_results = SF_Utils.query_salesforce(sf, sf_contract_query)
    # convert query results to a dataframe
    sf_contract_df = SF_Utils.load_query_with_lookups_into_dataframe(contract_query_results)
    # query mssql contract records
    mssql_contract_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_contract_df.rename(columns = {"account_number_external_id" : "SBQQ__Order__r.Account.Account_Number_External_ID__c",
                                        "contract_number" : "SBQQ__Order__r.SBQQ__Quote__r.Quote_External_ID__c",
                                	    "start_date" : "StartDate",
                                        "end_date" : "EndDate"}, inplace = True)
    # only compare the mssql staging contracts with what is loaded into salesforce
    mssql_contract_df = mssql_contract_df[mssql_contract_df['SBQQ__Order__r.SBQQ__Quote__r.Quote_External_ID__c'].isin(sf_contract_df['SBQQ__Order__r.SBQQ__Quote__r.Quote_External_ID__c'])]
    # add contracted = true
    mssql_contract_df["Status"] = "Activated"
    # sort columns in both dataframes to have same contract
    sf_contract_df = sf_contract_df[sorted(sf_contract_df.columns)]
    mssql_contract_df = mssql_contract_df[sorted(mssql_contract_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_contract_df = mssql_contract_df.astype(sf_contract_df.dtypes)
    # reset index for comparison
    sf_contract_df.reset_index(drop=True, inplace = True)
    mssql_contract_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_contract_df, mssql_contract_df)

def test_migrated_contract_lines_after_update(setup_salesforce_connection, setup_mssql_connection):
    """Description: run this test after the quote lines are migrated into Salesforce.
    This test can only be run immediately after the quote lines migration are updated.
    Running before updates will cause a failure.
    """
    sf_subscriptions_query = """SELECT
	SBQQ__EndDate__c,
	SBQQ__ProductName__c,
	SBQQ__Quantity__c,
	SBQQ__QuoteLine__r.QuoteLine_External_ID__c,
	SBQQ__QuoteLine__r.SBQQ__Quote__r.Quote_External_ID__c,
	SBQQ__StartDate__c
    FROM SBQQ__Subscription__c
    WHERE SBQQ__QuoteLine__r.Migrated_Record__c = True
    ORDER BY SBQQ__QuoteLine__r.SBQQ__Quote__r.Quote_External_ID__c, SBQQ__QuoteLine__r.QuoteLine_External_ID__c
    """

    mssql_select_query = """SELECT
           [CON].[contract_number]
          ,[contract_line_number]
          ,[start_date]
          ,[end_date]
          ,[product]
          ,[quantity]
      FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts] AS CON
      INNER JOIN [Data_Engineering].[dbo].[STG_SOURCE_Contract_Lines] AS CON_LINE on [CON].[contract_number] =  [CON_LINE].[contract_number]
      ORDER BY [CON].[contract_number], [CON_LINE].[contract_line_number]"""

    # set up salesforce connection
    sf = setup_salesforce_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection
    # query salesforce and return the migrated order
    subscriptions_query_results = SF_Utils.query_salesforce(sf, sf_subscriptions_query)
    # convert query results to a dataframe
    sf_subscriptions_df = SF_Utils.load_query_with_lookups_into_dataframe(subscriptions_query_results)
    # query mssql order records
    mssql_subscriptions_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)
    # rename mssql column names to match salesforce column names
    mssql_subscriptions_df.rename(columns = {"contract_number" : "SBQQ__QuoteLine__r.SBQQ__Quote__r.Quote_External_ID__c",
                                           "contract_line_number" : "SBQQ__QuoteLine__r.QuoteLine_External_ID__c",
                                	       "start_date" : "SBQQ__StartDate__c",
                                           "end_date" : "SBQQ__EndDate__c",
                                           "product" : "SBQQ__ProductName__c",
                                           "quantity" : "SBQQ__Quantity__c"}, inplace = True)
    # only compare the mssql staging orders with what is loaded into salesforce
    mssql_subscriptions_df = mssql_subscriptions_df[mssql_subscriptions_df['SBQQ__QuoteLine__r.QuoteLine_External_ID__c'].isin(sf_subscriptions_df['SBQQ__QuoteLine__r.QuoteLine_External_ID__c'])]
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
    mssql_subscriptions_df["SBQQ__ProductName__c"] = mssql_subscriptions_df["SBQQ__ProductName__c"].replace(product_mapping)
    # sort columns in both dataframes to have same order
    sf_subscriptions_df = sf_subscriptions_df[sorted(sf_subscriptions_df.columns)]
    mssql_subscriptions_df = mssql_subscriptions_df[sorted(mssql_subscriptions_df.columns)]
    # convert mssql column datatypes to be the same as salesforce datatypes
    mssql_subscriptions_df = mssql_subscriptions_df.astype(sf_subscriptions_df.dtypes)
    # reset index for comparison
    sf_subscriptions_df.reset_index(drop=True, inplace = True)
    mssql_subscriptions_df.reset_index(drop=True, inplace = True)
    # assert the source table is equal to the target table
    assert_frame_equal(sf_subscriptions_df, mssql_subscriptions_df)
