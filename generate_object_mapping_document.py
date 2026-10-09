"""
Author: Timothy Kornish
CreatedDate: September - 15 - 2026
Description:
             # log into salesforce
             # call metadata function on account
             # convert return values into pandas dataframe
             # add empty columns for source system to map to target SF Account fields
             # write df to an excel file in a single tab

"""

import numpy as np
import pandas as pd
import os
import json
from custom_db_utilities import  Salesforce_Utilities, Custom_Utilities
from credentials import Credentials

def generate_object_mapping(object, fields_metadata_to_keep, target_column_rename_dict, source_columns_to_add, add_source_on_left = True):
    """
    Description: retreive salesforce object metadata and reformat for a mapping document sheet
    Parameters:

    object                      - String, api name of sf object
    fields_metadata_to_keep     - list, which metadata properties of each field to keep
    target_column_rename_dict   - dict, rename metadata properties for mapping document columns
    source_columns_to_add       - list, add source system mapping columns to mapping document sheet
    add_source_on_left = True   - boolean, add source system columns on left of target system columns

    Return:                     - pandas df, return mapping sheet as a df to be written to excel file at end of script
    """
    # retrieve metadata on account and load into pandas dataframe
    metadata_df = SF_Utils.retrieve_object_metadata(sf, object, fields_metadata_to_keep)
    # rename columns to use target as prefix
    metadata_df.rename(columns = target_column_rename_dict, inplace = True)
    # add mapping fields to metadata fields and return new df
    mapping_df = Utils.add_mapping_fields(metadata_df, source_columns_to_add, True)
    # add target object column with object name
    mapping_df['Target Object'] = object
    # return mapping df with source to target mapping columns set up
    return mapping_df

# show all columns in print output
pd.set_option('display.max_columns', None)

# create and instance of the custom salesforce utilities class used to interact with Salesforce
SF_Utils = Salesforce_Utilities()
# create and instance of the custom utilities class used to format and modify dataframe data
Utils = Custom_Utilities()
# create instance of credentials class where creds are stored to load into the script
Cred = Credentials()

# declare which environment this script will perform operations against,
# can have multiple environments in the same script at the same time
environment = 'Dev'

# set database to Salesforce
database = "Salesforce"

# set up directory pathway to load csv data and output fallout and success results to
dir_path = os.path.dirname(os.path.realpath(__file__))

# get username from credentials
username = Cred.get_username(database, environment)
# get password from credentials
password = Cred.get_password(database, environment)
# get login token from credentials
token = Cred.get_token(database, environment)

# create a instance of simple_salesforce to query and perform operations against salesforce with
sf = SF_Utils.login_to_salesForce(username, password, token)

# set list of field metadata to keep
fields_metadata_to_keep = ['name', 'label', 'type', 'length', 'precision', 'unique', 'nillable', 'picklistValues', 'custom', 'calculated']

# create list of source system columns to add to beginning of mapping document
source_columns_to_add = ['Source DB', 'Source Object', 'Source Field', 'Source Field Data Type', 'Source Field Description', 'Transformation Logic', 'Map Field', 'Target Object']

# target field renaming dictionary
target_column_rename_dict = {'name' : 'Target Field API name',
                             'label' : 'Target Field Lable',
                             'type' : 'Target Field Type',
                             'length' : 'Target Field Length',
                             'precision' : 'Target Field Precision',
                             'unique' : 'Target Field is Unique',
                             'nillable' : 'Target Field is nillable',
                             'picklistValues' : 'Target Field Picklist Values',
                             'custom' : 'Target Field is custom',
                             'calculated' : 'Target Field is Calculated'}

# set object to grab metadata for and will also be used as sheet names
objects = ["Account", "Contact", "Opportunity", "SBQQ__Quote__c",  "SBQQ__QuoteLine__c", "Order", "OrderItem", "Contract", "SBQQ__Subscription__c"]

# set up list of dataframes to be converted to excel sheets
dfs = []
# loop through all objects and create a mapping dataframe for each
for object in objects:
    # generate mapping dataframe
    mapping_df = generate_object_mapping(object, fields_metadata_to_keep, target_column_rename_dict, source_columns_to_add, add_source_on_left = True)
    # add mapping dataframe to list
    dfs = dfs + [mapping_df]

# mapping document file name
file_name = "Mapping_Document.xlsx"
# name of each sheet in mapping document that coordinates to each dataframe in dfs
sheet_names = objects
# output dataframes in excel file
Utils.write_df_to_excel(dfs, file_name, sheet_names)
