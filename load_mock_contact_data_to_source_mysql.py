"""
Author: Timothy Kornish
CreatedDate: August - 24 - 2026
Description: Load csv mock data into a pandas dataframes.
             log into a MySQL Database.
             insert each csv data set into respective tables
             - contacts

"""

import numpy as np
import pandas as pd
import os
from custom_db_utilities import  MySQL_Utilities
from credentials import Credentials

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = MySQL_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
env = 'localhost'

# set database to MySQL
database = "MySQL"

# number of records to attempted
num_of_records = 10

# starting index to choose records
record_start = 10

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# set input path for mock data csv
contact_csv_file = dir_path + ".\\MockData\\MOCK_DATA_full_contact_list.csv"

# read mock data csv into pandas dataframe
contact_df = pd.read_csv(contact_csv_file)

# fill na with blank string
contact_df = contact_df.fillna('')

# retrieve credentials to connect to mysql table
server = Cred.get_server(dbms = database, env = env)
database = Cred.get_database(dbms = database, env = env)
username = Cred.get_mysql_username(dbms = database, env = env)
password = Cred.get_mysql_password(dbms = database, env = env)

# initiate a MySQL engine to query with
connection, cursor = MySQL_Utils.login_to_mysql(server = server, database = database,
                                         username = username, password = password)

# contacts in the mssql table shown in the query above
MySQL_Utils.insert_dataframe_into_mysql_table(connection, cursor, contact_df, 'contacts')
