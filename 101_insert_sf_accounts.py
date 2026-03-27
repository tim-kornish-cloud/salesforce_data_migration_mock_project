"""
Author: Timothy Kornish
CreatedDate: August - 15 - 2025
Description: Load a csv of mock data into a pandas dataframe.
             log into a MSSQL table.
             Perform select query on table and load records into pandas DataFrame
             merge records from table and csv
             print out which records exist in which tables based on join Id column

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MSSQL_Utilities, Custom_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'localhost'

#set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# initiate an MS SQL cursor to query with
connection, cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# select accounts to match against the csv to not attempt to insert duplicates
select_query = """SELECT
       [phone]
      ,[company_name]
      ,[industry]
      ,[annual_revenue]
      ,[account_number_external_id]
      ,[number_of_locations]
      ,[number_of_employees]
      ,[sla]
      ,[sla_serial_number]
  FROM [Data_Engineering].[dbo].[STG_SOURCE_Accounts]"""

# accounts in the mssql table shown in the query above
account_df = MSSQL_Utils.query_mssql_return_dataframe(select_query, cursor)
