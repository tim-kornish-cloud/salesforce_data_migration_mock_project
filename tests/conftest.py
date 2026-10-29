"""
Author: Timothy Kornish
CreatedDate: October - 22 - 2026
Description: test config file for connecting to slaesforce, MySQL, and MSSQL.
             Use these session functions to set up single connection to each db
             instead of recreating db connection with each test function.

"""
import pytest
import pandas as pd
from custom_db_utilities import MSSQL_Utilities, Salesforce_Utilities, MySQL_Utilities, Custom_Utilities
from credentials import Credentials

import os

# create and instance of the custom salesforce utilities class used to interact with Salesforce
MySQL_Utils = MySQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
MSSQL_Utils = MSSQL_Utilities()
# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# create and instance of the custom  utilities class
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
env = 'localhost'
# set database to MySQL
mysql_db = "MySQL"
# set database to MSSQL
mssql_db = "MSSQL"
# set salesforce environment to dev
sf_environment = 'Dev'
# set database to Salesforce
sf_database = "Salesforce"



@pytest.fixture(scope = "session")
def setup_mysql_connection():
    # retrieve credentials to connect to mysql table
    mysql_server = Cred.get_server(dbms = mysql_db, env = env)
    mysql_database = Cred.get_database(dbms = mysql_db, env = env)
    mysql_username = Cred.get_mysql_username(dbms = mysql_db, env = env)
    mysql_password = Cred.get_mysql_password(dbms = mysql_db, env = env)
    # initiate a MySQL engine to query with
    mysql_connection, mysql_cursor = MySQL_Utils.login_to_mysql(server = mysql_server, database = mysql_database,
                                                                username = mysql_username, password = mysql_password)
    yield mysql_connection, mysql_cursor

@pytest.fixture(scope = "session")
def setup_mssql_connection():
    # get server and database from credentials
    mssql_server = Cred.get_server(dbms = mysql_db, env = env)
    mssql_database = Cred.get_database(dbms = mysql_db, env = env)
    # initiate an MS SQL cursor to query with
    mssql_connection, mssql_cursor = MSSQL_Utils.login_to_mssql(server = mssql_server, database = mssql_database)
    yield mssql_connection, mssql_cursor

@pytest.fixture(scope = "session")
def setup_salesforce_connection():
    # get username from credentials
    sf_username = Cred.get_username(sf_database, sf_environment)
    # get consumer_key from credentials
    sf_consumer_key = Cred.get_consumer_key(sf_database, sf_environment)
    # get privatekey_file from credentials
    sf_privatekey_file = Cred.get_privatekey_file(sf_database, sf_environment)
    # login and yeild sf instance
    sf = SF_Utils.login_to_salesForce(username = sf_username, consumer_key = sf_consumer_key, privatekey_file = sf_privatekey_file)

    yield sf
