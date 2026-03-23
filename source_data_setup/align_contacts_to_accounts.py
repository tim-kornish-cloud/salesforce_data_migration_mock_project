"""
Author: Timothy Kornish
CreatedDate: March - 21 - 2026
Description: take mock data of accounts and contacts from mockaroo and use the
             id column to add the account_number_external_id to the contact csvs,
             then concat all contact csvs into a single file.

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

# set input path for mock contact data csv
contact_1_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contact_1.csv"

# set input path for mock contact data csv
contact_2_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contact_2.csv"

# set input path for mock contact data csv
contact_3_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_contact_3.csv"

# set output path for contacts list
contact_list_csv_file = dir_path + ".\\..\\MockData\\MOCK_DATA_full_contact_list.csv"

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
account_df = pd.read_csv(account_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contact_1_df = pd.read_csv(contact_1_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 1000 records
contact_2_df = pd.read_csv(contact_2_csv_file)

# read mock data csv from mockaroo.com into a pandas datafrome
# file contains 400 records
contact_3_df = pd.read_csv(contact_3_csv_file)

# isolate the account number external id field with the row number "id"
account_id_df = account_df[["id", "account_number_external_id"]]

# merge the account number external Id to the contact file
contact_with_account_1_df = pd.merge(account_id_df, contact_1_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contact_with_account_2_df = pd.merge(account_id_df, contact_2_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)
contact_with_account_3_df = pd.merge(account_id_df, contact_3_df, on = "id", how ="inner", suffixes = ("_left", "_right"), indicator = False)

#print columns
print(contact_with_account_1_df.columns)
print(contact_with_account_1_df.head())

# concat all dfs to create a single csv of contacts to pull from
contacts_df = pd.concat([contact_with_account_1_df, contact_with_account_2_df, contact_with_account_3_df], axis = 0)

# remove id column, no longer needed after merges
contacts_df.drop('id', axis = 1, inplace = True)

# print columns
print(len(contacts_df))
print(contacts_df.columns)
print(contacts_df.head())

# output contacts concatenated files into a single new csv
contacts_df.to_csv(contact_list_csv_file, index = False)
