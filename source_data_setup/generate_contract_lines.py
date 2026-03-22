"""
Author: Timothy Kornish
CreatedDate: March - 21 - 2026
Description: take mock data of contract lines and add contract Id to them and generate a single list of contract lines
             each contract needs 3 lines, gen type, installation, and SLA
             gen type and installation from csvs, SLA from related account

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

# set input path for mock contract_lines data csv
contract_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_full_contracts_list.csv"

# set input path for mock contract_lines data csv
contract_lines_1_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_1.csv"

# set input path for mock contract_lines data csv
contract_lines_2_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_2.csv"

# set input path for mock contract_lines data csv
contract_lines_3_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_3.csv"

# set input path for mock contract_lines data csv
contract_lines_4_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_4.csv"

# set input path for mock contract_lines data csv
contract_lines_5_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_5.csv"

# set input path for mock contract_lines data csv
contract_lines_6_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_6.csv"

# set output path for contract_liness list
contract_lines_list_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_full_contract_lines_list.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
account_df = pd.read_csv(account_csv_file)

# read compiled list of contracts into pandas dataframe
# file contains 3000 records
contract_df = pd.read_csv(contract_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_1_df = pd.read_csv(contract_lines_1_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_2_df = pd.read_csv(contract_lines_2_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 400 records
contract_lines_3_df = pd.read_csv(contract_lines_3_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_4_df = pd.read_csv(contract_lines_4_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_5_df = pd.read_csv(contract_lines_5_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 400 records
contract_lines_6_df = pd.read_csv(contract_lines_6_csv_file)

# isolate the account number external id field with the row number "id"
account_id_df = account_df[["id", "account_number_external_id"]]

contract_line_generator_df
contract_line_installation_df
contract_line_sla_df

# merge the account number external Id to the contract_lines file
contract_lines_with_account_1_df = pd.merge(account_id_df, contract_lines_1_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_lines_with_account_2_df = pd.merge(account_id_df, contract_lines_2_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_lines_with_account_3_df = pd.merge(account_id_df, contract_lines_3_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)

#print columns
print(contract_lines_with_account_1_df.columns)
print(contract_lines_with_account_1_df.head())

# concat all dfs to create a single csv of contract_liness to pull from
contract_liness_df = pd.concat([contract_lines_with_account_1_df, contract_lines_with_account_2_df, contract_lines_with_account_3_df], axis = 0)

contract_liness_df.drop('id', axis = 1, inplace = True)

#print columns
print(len(contract_liness_df))
print(contract_liness_df.columns)
print(contract_liness_df.head())

contract_liness_df.to_csv(contract_lines_list_csv_file, index = False)
