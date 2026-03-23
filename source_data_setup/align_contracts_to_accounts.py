"""
Author: Timothy Kornish
CreatedDate: March - 21 - 2026
Description: take mock data of accounts and contracts from mockaroo and use the
             id column to add the account_number_external_id to the contract csvs,
             then concat all contract csvs into a single file.

"""

import numpy as np
import pandas as pd
import os

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# display every column when called in print call
# Set option to display all columns
pd.set_option('display.max_columns', None)

# set input path for mock account data csv
account_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_Account.csv"

# set input path for mock contract data csv
contract_1_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contracts_1.csv"

# set input path for mock contract data csv
contract_2_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contracts_2.csv"

# set input path for mock contract data csv
contract_3_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contracts_3.csv"

# set output path for contracts list
contract_list_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_full_contracts_list.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
account_df = pd.read_csv(account_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_1_df = pd.read_csv(contract_1_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_2_df = pd.read_csv(contract_2_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 400 records
contract_3_df = pd.read_csv(contract_3_csv_file)

# isolate the account number external id field with the row number "id"
account_id_df = account_df[["id", "account_number_external_id"]]

# merge the account number external Id to the contract file
contract_with_account_1_df = pd.merge(account_id_df, contract_1_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_with_account_2_df = pd.merge(account_id_df, contract_2_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_with_account_3_df = pd.merge(account_id_df, contract_3_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)

#print columns
print(contract_with_account_1_df.columns)
print(contract_with_account_1_df.head())

# concat all dfs to create a single csv of contracts to pull from
contracts_df = pd.concat([contract_with_account_1_df, contract_with_account_2_df, contract_with_account_3_df], axis = 0)

# remove id column, no longer needed
contracts_df.drop('id', axis = 1, inplace = True)

# set start date column to be a datetime datatype to set up end_date column
contracts_df['start_date'] = pd.to_datetime(contracts_df['start_date'])
# set up end_date one year in the future minus 1 day
contracts_df['end_date'] = contracts_df['start_date'] + pd.offsets.DateOffset(years=1) - pd.Timedelta(days=1)


#print columns
print(len(contracts_df))
print(contracts_df.columns)
print(contracts_df.head())

# output contracts dataframe to a new csv
contracts_df.to_csv(contract_list_csv_file, index = False)
