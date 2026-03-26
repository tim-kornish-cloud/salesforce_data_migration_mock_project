"""
Author: Timothy Kornish
CreatedDate: August - 25 - 2026
Description: log into a MySQL Database.
             query contract records.
             log into MSSQL staging database and insert records into staging table
             - contract_liness
"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import MSSQL_Utilities, MySQL_Utilities, Custom_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = MySQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
env = 'localhost'

# set database to MySQL
database = "MySQL"

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# retrieve credentials to connect to mysql table
server = Cred.get_server(dbms = database, env = env)
database = Cred.get_database(dbms = database, env = env)
username = Cred.get_mysql_username(dbms = database, env = env)
password = Cred.get_mysql_password(dbms = database, env = env)

# initiate a MySQL engine to query with
mysql_connection, mysql_cursor = MySQL_Utils.login_to_mysql(server = server, database = database,
                                         username = username, password = password)

# set query to query all the records that are on the table
accounts_query = "SELECT * FROM data_engineering.contract_lines"

# query the records inserted
account_df = MySQL_Utils.query_mysql_return_dataframe(accounts_query, mysql_connection)

# fill na with blank string
account_df = account_df.fillna('')

# initiate an MS SQL connection and cursor to query with
mssql_connection, mssql_cursor = MSSQL_Utils.login_to_mssql(server = Cred.get_server(), database = Cred.get_database())

# list of data types to convert the df columns to fit MSSQL
# need to find a way to parse the df.columns and generate this automatically
# this is a temporaray bandaid being hardcoded, honestly may perform a subset of
# hardcoding these types instead of the entire dataframe
column_types = ('str', 'str', 'str', 'int')

# mssql table name the dataframe is being inserted into
table_name = 'STG_SOURCE_Contract_lines'

# insert subset of the csv  from a dataframe into the mssql table
MSSQL_Utils.insert_dataframe_into_mssql_table(mssql_connection, mssql_cursor, account_df, table_name, column_types)
