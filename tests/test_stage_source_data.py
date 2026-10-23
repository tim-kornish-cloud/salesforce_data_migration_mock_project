"""
Author: Timothy Kornish
CreatedDate: October - 23 - 2026
Description: setup tests to validate the source data from mysql db
             is properly setup in mssql database tables.

"""

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from tests.conftest import setup_mysql_connection, setup_mssql_connection

from custom_db_utilities import MSSQL_Utilities, Salesforce_Utilities, MySQL_Utilities


pd.set_option('display.max_columns', None)

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = MySQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()

def test_account_tables(setup_mysql_connection, setup_mssql_connection):
    mssql_select_query = """SELECT
           [phone]
          ,[company_name]
          ,[industry]
          ,[annual_revenue]
          ,[account_number_external_id]
          ,[number_of_locations]
          ,[number_of_employees]
          ,[sla]
          ,[sla_serial_number]
    FROM [Data_Engineering].[dbo].[STG_SOURCE_Accounts]
    ORDER BY [account_number_external_id]"""

    mysql_select_query = """SELECT
           phone
          ,company_name
          ,industry
          ,annual_revenue
          ,account_number_external_id
          ,number_of_locations
          ,number_of_employees
          ,sla
          ,sla_serial_number
    FROM data_engineering.accounts
    ORDER BY account_number_external_id"""

    # get mysql connection and cursor to interact with mysql tables
    mysql_connection, mysql_cursor = setup_mysql_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection

    # query mysql account records
    mysql_account_df = MySQL_Utils.query_mysql_return_dataframe(mysql_select_query, mysql_connection)
    # query mssql account records
    mssql_account_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)

    # mssql query interprets this column as an object instead of float64
    mssql_account_df['annual_revenue'] = mssql_account_df['annual_revenue'].astype('float64')
    # assert the source table is equal to the target table
    assert_frame_equal(mysql_account_df, mssql_account_df)

def test_contact_tables(setup_mysql_connection, setup_mssql_connection):
    mssql_select_query = """SELECT
       [account_number_external_id]
      ,[first_name]
      ,[last_name]
      ,[email]
      ,[title]
      ,[department]
      ,[languages]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contacts]
  ORDER BY [account_number_external_id], [last_name], [first_name]"""

    mysql_select_query = """SELECT
       account_number_external_id
      ,first_name
      ,last_name
      ,email
      ,title
      ,department
      ,languages
  FROM data_engineering.contacts
  ORDER BY account_number_external_id, last_name, first_name"""

    # get mysql connection and cursor to interact with mysql tables
    mysql_connection, mysql_cursor = setup_mysql_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection

    # query mysql contact records
    mysql_contact_df = MySQL_Utils.query_mysql_return_dataframe(mysql_select_query, mysql_connection)
    # query mssql contact records
    mssql_contact_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)

    # assert the source table is equal to the target table
    assert_frame_equal(mysql_contact_df, mssql_contact_df)

def test_contract_tables(setup_mysql_connection, setup_mssql_connection):
    mssql_select_query = """SELECT
       [account_number_external_id]
      ,[contract_number]
      ,[start_date]
      ,[end_date]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contracts]
  ORDER BY [account_number_external_id], [contract_number]"""

    mysql_select_query = """SELECT
       account_number_external_id
      ,contract_number
      ,start_date
      ,end_date
  FROM data_engineering.contracts
  ORDER BY account_number_external_id, contract_number"""

    # get mysql connection and cursor to interact with mysql tables
    mysql_connection, mysql_cursor = setup_mysql_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection

    # query mysql contract records
    mysql_contract_df = MySQL_Utils.query_mysql_return_dataframe(mysql_select_query, mysql_connection)
    # query mssql contract records
    mssql_contract_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)

    # assert the source table is equal to the target table
    assert_frame_equal(mysql_contract_df, mssql_contract_df)

def test_contract_lines_tables(setup_mysql_connection, setup_mssql_connection):
    mssql_select_query = """SELECT
       [contract_line_number]
      ,[contract_number]
      ,[product]
      ,[quantity]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Contract_Lines]
  ORDER BY [contract_line_number] ,[contract_number]"""

    mysql_select_query = """SELECT
       contract_line_number
      ,contract_number
      ,product
      ,quantity
  FROM data_engineering.contract_lines
  ORDER BY contract_line_number ,contract_number"""

    # get mysql connection and cursor to interact with mysql tables
    mysql_connection, mysql_cursor = setup_mysql_connection
    # get mssql connection and cursor to interact with mssql tables
    mssql_connection, mssql_cursor = setup_mssql_connection

    # query mysql contract records
    mysql_contract_df = MySQL_Utils.query_mysql_return_dataframe(mysql_select_query, mysql_connection)
    # query mssql contract records
    mssql_contract_df = MSSQL_Utils.query_mssql_return_dataframe(mssql_select_query, mssql_connection)

    # assert the source table is equal to the target table
    assert_frame_equal(mysql_contract_df, mssql_contract_df)
