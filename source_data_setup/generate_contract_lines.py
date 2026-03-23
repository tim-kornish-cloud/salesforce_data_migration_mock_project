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

# set input path for mock contract_lines data csv
contract_lines_7_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_7.csv"

# set input path for mock contract_lines data csv
contract_lines_8_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_8.csv"

# set input path for mock contract_lines data csv
contract_lines_9_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contract_lines_9.csv"

# set output path for contract_liness list
contract_lines_list_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_full_contract_lines_list.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_1_df = pd.read_csv(contract_lines_1_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_2_df = pd.read_csv(contract_lines_2_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_3_df = pd.read_csv(contract_lines_3_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_4_df = pd.read_csv(contract_lines_4_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_5_df = pd.read_csv(contract_lines_5_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_6_df = pd.read_csv(contract_lines_6_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_7_df = pd.read_csv(contract_lines_7_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_8_df = pd.read_csv(contract_lines_8_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contract_lines_9_df = pd.read_csv(contract_lines_9_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
account_df = pd.read_csv(account_csv_file)

# read compiled list of contracts into pandas dataframe
# file contains 3000 records
contract_df = pd.read_csv(contract_csv_file)

# isolate the account number external id field with the row number "id"
account_id_df = account_df[["id", "account_number_external_id", "sla"]]

#isolate the join fields on the contracts to add to contract lines files
contract_df = contract_df[["account_number_external_id", "contract_number"]]

# files 1-3
contract_line_generator_df = pd.concat([contract_lines_1_df, contract_lines_2_df, contract_lines_3_df], axis = 0)
# files 4-6
contract_line_installation_df = pd.concat([contract_lines_4_df, contract_lines_5_df, contract_lines_6_df], axis = 0)
# files 7-9
contract_line_sla_df = pd.concat([contract_lines_7_df, contract_lines_8_df, contract_lines_9_df], axis = 0)

# drop id column and reset with new values
contract_line_generator_df.drop('id', axis = 1, inplace = True)
contract_line_installation_df.drop('id', axis = 1, inplace = True)
contract_line_sla_df.drop('id', axis = 1, inplace = True)

# reset id column from 1-3000
contract_df["id"] = [i for i in range(1,3001,1)]
contract_line_generator_df["id"] = [i for i in range(1,3001,1)]
contract_line_installation_df["id"] = [i for i in range(1,3001,1)]
contract_line_sla_df["id"] = [i for i in range(1,3001,1)]

# add contract number to all contract lines
contract_lines_with_gen_df = pd.merge(contract_line_generator_df, contract_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_lines_with_install_df = pd.merge(contract_line_installation_df, contract_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_lines_with_sla_df = pd.merge(contract_line_sla_df, contract_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contract_lines_with_sla_df = pd.merge(contract_lines_with_sla_df, account_id_df, on = "account_number_external_id", how ="inner", suffixes = ("_left", "_right"), indicator = False)

contract_lines_with_gen_df["product"] = contract_lines_with_gen_df["gen_type"].str.cat(contract_lines_with_gen_df["gen_type_capacity"], sep = "-", na_rep = "")
contract_lines_with_install_df["product"] = "install - " + contract_lines_with_install_df["installation"]
contract_lines_with_sla_df["product"] = "sla - " + contract_lines_with_sla_df["sla"]

contract_lines_with_gen_df.drop(['gen_type', 'gen_type_capacity', 'id', 'account_number_external_id'], axis = 1, inplace = True)
contract_lines_with_install_df.drop([ 'installation', 'id', 'account_number_external_id'], axis = 1, inplace = True)
contract_lines_with_sla_df.drop(['id_left', 'account_number_external_id', 'id_right', 'sla'], axis = 1, inplace = True)


print(len(contract_lines_with_gen_df))
print(contract_lines_with_gen_df.columns)
print(contract_lines_with_gen_df.head())

print(len(contract_lines_with_install_df))
print(contract_lines_with_install_df.columns)
print(contract_lines_with_install_df.head())

print(len(contract_lines_with_sla_df))
print(contract_lines_with_sla_df.columns)
print(contract_lines_with_sla_df.head())

# concat all dfs to create a single csv of contract_liness to pull from
contract_lines_df = pd.concat([contract_lines_with_gen_df, contract_lines_with_install_df, contract_lines_with_sla_df], axis = 0)
#
contract_lines_df["quantity"] = 1

#print columns
print(len(contract_lines_df))
print(contract_lines_df.columns)
print(contract_lines_df.head())

contract_lines_df.to_csv(contract_lines_list_csv_file, index = False)
