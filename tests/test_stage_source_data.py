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

def test_account_tables(setup_mysql_connection, setup_mssql_connection):

def test_contact_tables(setup_mysql_connection, setup_mssql_connection):

def test_contract_tables(setup_mysql_connection, setup_mssql_connection):

def test_contract_lines_tables(setup_mysql_connection, setup_mssql_connection):
