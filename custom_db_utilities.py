"""
Author: Timothy Kornish
CreatedDate: August - 10 - 2025
Description: A backend utility file for logging into Salesforce, MySQL, MSSQL, S3, MongoDB, PostgreSQL DBMS's
             performing insert/update/delete and querying results with added pre-processing, post-processing,
             and logging to the console for time recording of execution start and end results.

             At the end also includes a custom_utilities class for useful functions non-DBMS specific.

"""
# util libraries
from ctypes import util
from datetime import datetime
from collections import OrderedDict
import time
import logging as log
import coloredlogs
# pandas and numpy
import numpy as np
import pandas as pd
# salesforce connector
from simple_salesforce import Salesforce
# MSSQL connector
import pyodbc
# AWS S3 EC2 connector
import boto3
import awswrangler as wr
# mysql connector
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql
import mysql.connector
# MongoDB
from pymongo import MongoClient
# psycopg2 for connecting postgresql database
import psycopg2
from psycopg2.extras import execute_values
# snowflake connection
import snowflake.connector

# initialize the console logging to aid in time estimate of execution scripts
coloredlogs.install()
# set debug level for which debugging is output to console,
# currently only using info debug level comments
log.basicConfig(level = log.ERROR)
# set copy on write on to suppress writing a slice of a dataframe warnings
pd.set_option("mode.copy_on_write", True)

class Salesforce_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           can add login credentials as instance variables to utilize in functions
        """

    def login_to_salesForce(self, username, password, security_token, environment = ""):
        """
        Description: log into a Salesforce or and return salesforce client
                     to operate with.
        Parameters:

        Username        - string, salesforce Username
        Password        - string, salesforce Password
        security_token  - string, salesforce token
        environments    - string, used for logger to state which org being logged into

        Return: sf      - Salesforce instance to query against
        """
        # try except block
        try:
            # log status to console
            log.info(f"[Logging into source org: {environment}]")
            # log into salesforce using simple_salesforce
            sf = Salesforce(username = username,
                            password = password,
                            security_token = security_token
                            )
            # log to console the login was successful. if not successful,
            # there will be a error from Salesforce on the console
            log.info("[Logged in successfully]")
            # return instance of salesforce to perform operations with
            return sf
        # exception block - error logging into salesforce
        except Exception as e:
            # log error when logging into salesforce
            log.exception(f"[Error logging into source org: {environment}...{e}]")

    def query_salesforce(self, sf, query, include_deleted = False):
        """
        Description: upload a SOQL query to salesforce and return
                     a JSON object full of records.
        Parameters:

        sf              - Salesforce instance to query against
        query           - string, SOQL query

        Return:         - query_results - JSON formatted records
        """
        # try except block
        try:
            # log status to console of querying Salesforce
            log.info(f"[Querying Salesforce orgs, include deleted records: {str(include_deleted)}]")
            # query salesforce and return results in json format
            query_results = sf.query_all(query, include_deleted = include_deleted)
            # return the json query results
            return query_results
        # exception block - error querying Salesforce orgs
        except Exception as e:
            # log error when querying Salesforce orgs
            log.exception(f"[Error querying Salesforce orgs...{e}]")

    def format_date_to_salesforce_date(self, df, columns, format = "%m/%d/%Y"):
        """
        Description: format specified columns in a dataframe to be compatible with Salesforce
        Parameters:

        df          - pandas.DataFrame
        columns     - list of string or string column names to format
        format      - default salesforce format = '%m/%d/%Y'

        Return:     - query_results - JSON formatted records
        """
        # try except block
        try:
            #setup df that will be returned by function
            return_df = df
            # check if formatting a single column
            if type(columns) == str:
                # reformat the single column, apply lambda to remove extra values only retaining the mm-dd-yyyy values including the hyphens
                return_df[columns] = pd.to_datetime(df[columns], format = format).dt.normalize().apply(lambda x : str(x)[:10])
            # formatting list of columns
            elif type(columns) == list:
                # loop through list of columns to format one by one
                for column in columns:
                    # format the column in this loop of the list, apply lambda to remove extra values only retaining the mm-dd-yyyy values including the hyphens
                    return_df[column] = pd.to_datetime(df[column], format = format).dt.normalize().apply(lambda x : str(x)[:10])
                    # replace blanks due to reformat
                    return_df.replace({column : {"NaT" : None, "" : None, " " : None}}, inplace = True)
            # return the reformatted dataframe
            return return_df
        # exception block - error formatting date column to Salesforce format
        except Exception as e:
            # log error when formatting date column to Salesforce format
            log.exception(f"[Error formatting date column to Salesforce format...{e}]")

    def load_query_into_dataframe(self, query_results):
        """
        Description: intermediary function to load SOQL query
                     that has lookup fields, requires more processing time.
                     By default auto sends to the flatten_lookup_fieldname_hierarchy function, can add parameter to assume no lookups in query
        Parameters:

        query_results - OrderedDict, JSON formatted records

        Return:       - pandas.DataFrame - DataFrame of the Salesforce Records
        """
        # try except block
        try:
            # use function to process query since it
            # has log to detect if query uses lookups or not
            return Utilities.load_query_with_lookups_into_dataframe(self, query_results)
        # exception block - error loading query results into dataframe
        except Exception as e:
            # log error when loading query results into dataframe
            log.exception(f"[Error loading query results into dataframe...{e}]")

    def flatten_lookup_fieldname_hierarchy(self, df, continue_loop = False, use_subset = True, subset_size = 1000):
        """
        Description: Process dataframe created from results of Salesforce Query
                     loop through all the columns of the dataframe and
                     check if each column has a nested dictionary indicating
                     this column is a nested lookup to un nest.
        Parameters:

        df            - DataFrame of the Salesforce Records
        continue_loop - recursive parameter to continue unpacking nested columns when set to true
        use_subset    - use batches
        subset_size   - batch size default to 1000

        Return:       - pandas.DataFrame - DataFrame of the Salesforce Records
        """
        # try except block
        try:
            # use function to process query since it
            # loop through each column in the dataframe
            for column in df.columns:
                # log to console which column is being checked
                log.info(f"[Checking if column: {column} is a lookup]")
                # use batches instead of the entire dataset
                if use_subset:
                    # only check a batch size of data for lookups (only batch number of rows)
                    list_to_check = [True if type(row[column]) == OrderedDict else False for index, row in df.head(subset_size).iterrows()]
                # dont use batches, use the entire dataset
                else:
                    # check the entire dataset for lookups (load every row, more data intensive)
                    list_to_check = [True if type(row[column]) == OrderedDict else False for index, row in df.iterrows()]
                # check if the list_to_check has any values created needing unnesting
                column_contains_nested_values = np.any(list_to_check)
                # if there are nested columns, pull the json object out
                # and re-insert it into a flat structure
                if column_contains_nested_values == True:
                    # create a temporary new dataframe with column count = 1
                    new_df = df[column].apply(pd.Series)
                    # remove attributes columns if exists to avoid parsing issue.
                    if "attributes" in new_df.columns:
                        # drop attributes columns,
                        # creates duplicate with original layer if left in
                        new_df.drop("attributes", axis = 1, inplace = True)
                    # modify name of unnested column
                    new_df = new_df.add_prefix(column + ".")
                    # add unnested column to the dataframe
                    df = pd.concat([df.drop(column, axis = 1), new_df], axis = 1)

            # parse through all columns checking for any deeper unnested columns
            # columns can have multiple layers of nesting,
            # this function works one layer of all columns at a time
            # not by one column finding every layer at a time
            for column in df.columns:
                # check if any column has type of ordered dict needing unnesting
                if type(df[column][0]) == OrderedDict:
                    # continue recursion loop
                    continue_loop = True
            # Recursion check, if more unnested columns exist, go again
            if continue_loop:
                # recursive loop back again.
                # note the continue loop parameter is reset to false by defulat when not included
                return self.flatten_lookup_fieldname_hierarchy(df)
            # no more nested columns found to unnest, function is complete, return dataframe
            else:
                # return results of query with every columns properly separated.
                return df
        # exception block - error flattening lookup fieldname hierarchy
        except Exception as e:
            # log error when flattening lookup fieldname hierarchy
            log.exception(f"[Error flattening lookup fieldname hierarchy...{e}]")

    def load_query_with_lookups_into_dataframe(self, query_results, use_subset = True, subset_size = 1000):
        """
        Description: Load SOQL query that has lookup fields, requires more processing time.
        Parameters:

        query_results   - OrderedDict, JSON formatted records

        Return:         - pandas.DataFrame - DataFrame of the Salesforce Records
        """
        # try except block
        try:
            # log info to console
            log.info("[loading query results into DataFrames]")
            # load query results json into a diction, then convert the dictionary
            # to a pandas Dataframe
            df = pd.DataFrame.from_dict(dict(query_results)["records"])
            # check if there are nested object fields in the query results
            if "attributes" in df.columns:
                # drop this column to avoid issue with unnesting the lookup fields
                 df.drop(["attributes"], axis = 1, inplace = True)
            # log to console
            log.info(f"[Unnesting columns for DF with: {str(len(df))} records]")
            # unnest lookup fields from query onto a flat array and return as a dataframe
            df = self.flatten_lookup_fieldname_hierarchy(df, use_subset = use_subset, subset_size = subset_size)
            # where a notnull NaN value is found, replace with None
            df = df.where((pd.notnull(df)), None)
            # log status of unnesting lookups into a new dataframe
            log.info(f"[loaded  {str(len(df))}  records into DataFrame]")
            # return the query results as a pandas dataframe
            return df
        # exception block - error loading query with lookups into dataframe
        except Exception as e:
            # log error when loading query with lookups into dataframe
            log.exception(f"[Error loading query with lookups into dataframe...{e}]")

    def reformat_dataframe_to_salesforce_records(self, df):
        """
        Description: Reformat df into list of dicts where each dict is a SF record
        Parameters:

        df                  - DataFrame, Salesforce records

        Return: sf_records  - list of dicts, each dict is a single layer deep, no nesting.
        """
        # try except block
        try:
            # log to console, reformatting records to json format
            log.info("[Reformatting data for SF JSON]")
            # conver the dataframe to a json dictionary
            sf_records = df.to_dict("records")
            # return the records in salesforce format
            return sf_records
        # exception block - error reformatting dataframe to salesforce json records
        except Exception as e:
            # log error when reformatting dataframe to salesforce json records
            log.exception(f"[Error reformatting dataframe to salesforce json records...{e}]")

    def upload_dataframe_to_salesforce(self, sf, df, object_name, dml_operation, success_file = None, fallout_file = None, batch_size = 1000, external_id_field=None, time_delay = None):
        """
        Description: upload dataframe of records to salesforce with dml operation.
                     This function includes pre processing of dataframe to json for
                     as well as post processing of results into separate
                     success and fallout dataframes writing output to select files
        Parameters:

        sf                  - simple_salesforce instance used to log in and perform operations again Salesforce
        df                  - pandas data frame of the data to be uploaded, formatting will occur in function
        object_name         - Salesforce object to perform operations against, both standard and custom objects
        dml_operation       - insert/upsert/update/delete, hard_delete should conceptually work, but I haven't tested it
        success_file        - string, path to store the success output file
        fallout_file        - string, path to store the fallout output file
        batch_size          - set batch size of records to upload in a single attempt
        external_id_field   - string, name of the external id field
        time_delay          - add a time delay between batch record uploads in case custom code needs to process between batches.

        Return:             - array of length 2, the fallout and success results separated in two DataFrames
        """
        # try except block
        try:
            # keep track of records attempted
            records_loaded = 0
            # keep  track of how many records successfully loaded
            passing = 0
            # keep track of how many records unsuccessfully loaded
            fallout = 0
            # array of the results to return at end of function
            results_list = []
            # quick check that we're not attempting to load 0 records, quit out immediately if so
            if len(df) != 0:
                # reformat the records from a pandas dataframe to JSON in salesforce compatibale format
                records_to_commit = self.reformat_dataframe_to_salesforce_records(df)
                # record how many records are going to be attempted
                records_count = len(records_to_commit)
                # log to console status
                log.info(f"[Starting DML.. records to {dml_operation} : {str(records_count)} ]")
                # perform as many loops as necessary to upload the records on the selected batch size.
                for index in range(0, records_count, batch_size):
                    # if there are more records to upload after the current batch upload a full batch,
                    if index+batch_size <= records_count:
                        # select a full batch of data
                        data = records_to_commit[index:index+batch_size]
                    # the number of records remaining to upload is less than the selected batch size
                    else:
                        # adjust how many records to select in the last batch to upload
                        data = records_to_commit[index:records_count]
                    # perform insert/upsert/update/delete operations using the submit_dml function
                    results = sf.bulk.submit_dml(object_name, dml_operation, data, external_id_field)
                    # convert the list of json records that was attempted into a pandas dataframe
                    data_df = pd.DataFrame(data)
                    # convert the results from the upload into a pandas dataframe
                    # add a suffix to all new columns created from the upload
                    results_df = pd.DataFrame(results).add_prefix("RESULTS_")
                    # split the results into two group, passing and fallout
                    # passing is success = true
                    passing = passing + len(results_df[results_df["RESULTS_success"] == True])
                    # fallout is success = false
                    fallout = fallout + len(results_df[results_df["RESULTS_success"] == False])
                    # concat the results of this batch to the dataframe the holds the results of the entire df being batch
                    results_df = pd.concat([data_df, results_df.reindex(data_df.index)], axis = 1)
                    # upload the resulting dataframe into an array
                    results_list.append(results_df)
                    # log the status of how many records passed vs failed
                    log.info(f"[{str(passing)}/{str(records_count)} rows of data - {dml_operation} rows of data loaded, failed rows: {str(fallout)}...]")
                    # if using a time delay between uploads, extecute the delay here at the end of the loop
                    if time_delay != None:
                        # time delay
                        time.sleep(time_delay)
                # full list of every record attempted
                results_df = pd.concat(results_list)
                # split the results int passing and fallout again
                # passing : "RESULTS_success" == True
                passing_df = results_df[results_df["RESULTS_success"] == True]
                # fallout : "RESULTS_success" == False
                fallout_df = results_df[results_df["RESULTS_success"] == False]

                # if a success file pathway is added, write the success datafame to the csv
                if success_file != None:
                    # open the file location in write mode
                    with open(success_file, mode = "w", newline = "\n") as file:
                        # write the dataframe to the file using a commma as the delimeter
                        passing_df.to_csv(file, sep = ",", index = False)
                # if a fallout file pathway is added, write the fallout datafame to the csv
                if fallout_file != None:
                    # open the file location in write mode
                    with open(fallout_file, mode = "w", newline = "\n") as file:
                        # write the dataframe to the file using a commma as the delimeter
                        fallout_df.to_csv(file, sep = ",", index = False)
                # return both the passing and fallout dataframes
                return [passing_df, fallout_df]
            # no records are in the dataframe, nothing to process
            else:
                # log to console, nothing included in dataframe to process
                log.info("[No Records to process]")
        # exception block - error uploading dataframe of records to salesforce
        except Exception as e:
            # log error when uploading dataframe of records to salesforce
            log.exception(f"[Error uploading dataframe of records to salesforce...{e}]")

class MSSQL_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           can add login credentials as instance variables to utilize in functions
        """

    def login_to_mssql(self, driver = "{ODBC Driver 17 for SQL Server}", server = None, database = "", username = None, password = None, use_windows_authentication = True, trusted_connection = "yes"):
        """
        Description: login to a MSSQL server and return a cursor object to query with
        Parameters:

        driver                      - SQL Server Driver use {SQL Driver} or {ODBC Driver 17 for SQL Server}
        server                      - string, IP address of server, I.E. 127.0.0.1
        database                    - string, Database name
        Username                    - string, MSSQL server Username
        Password                    - string, MSSQL server Password
        use_windows_authentication  - bool, use window authentication instead of a username and password
        trusted_connection          - string, values = 'yes', 'no'

        Return:         - MSSQL cursor
        """
        # try except block
        try:
            # login using connection string
            if use_windows_authentication:
                # log to console status of logging into database
                log.info(f"[Logging into MSSQL DB using windows Auth on DB: {database}]")
                # establish a connection
                cursor_connection = pyodbc.connect(driver=driver,
                                                   host=server,
                                                   database=database,
                                                   trusted_connection=trusted_connection)
            # log in using credentials: username/password
            else:
                # log to console status of logging into database
                log.info(f"[Logging into MSSQL DB using credentials on DB: {database}]")
                # create instance of cursor to connect to MSSQL database.
                cursor_connection = pyodbc.connect(driver=driver, host=server, database=database,
                            user=username, password=password)
            # convert the instance to a cursor
            cursor = cursor_connection.cursor()
            # log to console the cursor is created
            log.info("[Creating Cursor]")
            # return the connection and cursor to use to query against the database
            return (cursor_connection, cursor)
        # exception block - error
        except Exception as e:
            # log error when
            log.exception(f"[Error Logging into MSSQL DB...{e}]")

    def query_mssql_return_dataframe(self, query, cursor):
        """
        Description: query a MSSQL server with a logged in cursor and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query           - query string
        cursor          - cursor creating upon login to execute the query

        Return:         - pandas.DataFrame
        """
        # try except block
        try:
            # log to console beginning query against mssql database
            log.info("[Querying MS SQL DB...]")
            # execute query with cursor
            cursor = cursor.execute(query)
            # convert the results into a list of columns
            columns = [column[0] for column in cursor.description]
            # log to console status of querying records
            log.info("[Condensing results into Dict...]")
            # convert the columns and rows of data into a list of dicts
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            # log to console status of querying records
            log.info("[transforming Dict into DataFrame...]")
            # convert the list of dicts into a pandas dataframe
            results_df = pd.DataFrame(results)
            # log to console status of querying records
            log.info(f"[loaded {str(len(results_df))} records into DataFrame]")
            # return the results of the query as a pandas data frame
            return results_df
        # exception block - error querying mssql table
        except Exception as e:
            # log error when querying mssql table
            log.exception(f"[Error querying mssql table...{e}]")

    def insert_dataframe_into_mssql_table(self, connection, cursor, df, table_name, column_types = [], cols = "", use_all_columns_in_df = True):
        """Description: insert a dataframe into a mssql table, the whole dataframe will be inserted
        Parameters:

        connection              - MSSQL database connection
        cursor                  - MSSQL connection cursor
        df                      - dataframe to insert
        table_name              - table name of database to insert records into
        column_types            - set column datatypes before insert, auto datatype setting can sometimes be inaccurate
        cols                    - list of columns, currently experimental
        use_all_columns_in_df   - boolean to use all columns or not, currently experimental

        return:                 - none - insert records into mssql # DEBUG:
        Current issue 8/11/25:
        # current method causes warning with chained indexing instead of using .loc or .iloc
        # error is just making a getattr call twice instead of once and some other things that improve speed but not a breaking issue
        # will update to .loc/.iloc at a later point, just need a working function for now

        """
        # try except block
        try:
            # if the df column list matches the table, use all columns
            if use_all_columns_in_df:
                # generate a list of all columns
                cols = ",".join([k for k in df.dtypes.index])
            # generate a list of "?" to be replaced by the actual values of the dataframe
            params = ",".join("?" * len(df.columns))
            # generate the sql commit with the dataframe
            sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, cols, params)

            # for loop only works when provided a list of column converted types
            log.info("[Converting data types in DataFrame...]")
            # loop through each column to convert the type every value
            for index, col in enumerate(df.columns):
                # confirm the index is still within range of acceptable indexes
                if index < len(column_types):
                    # check if type == int
                    if column_types[index] == "int":
                        df[col] = df[col].astype(int)
                    # check if type == string
                    if column_types[index] == "str":
                        df[col] = df[col].astype(str)
                    # check if type == float
                    if column_types[index] == "float":
                        df[col] = df[col].astype(float)
                    # check if type == boolean
                    if column_types[index] == "bool":
                        df[col] = df[col].astype(bool)
            # convert the rows in the dataframe into tuples
            data = [tuple(x) for x in df.values]
            # set the bulk insert for pyodbc cursor.fast_executemany = True
            cursor.fast_executemany = True
            # execute insert of records
            cursor.executemany(sql, data)
            # commit the sql statement
            connection.commit()
            # close the connection if desired
        # exception block - error inserting dataframe into mssql table
        except Exception as e:
            # log error when inserting dataframe into mssql table
            log.exception(f"[Error inserting dataframe into mssql table: {table_name}...{e}]")

    def update_rows_in_mssql_table(self, connection, cursor, df, table_name, columns_to_update, where_column_name):
        """
        Description: update multiples columns in MSSQL table from a dataframe on a where in list condition

        sql_update =  example:
        UPDATE <table_name>
        SET <column1_name> = <value, corresponding column1 value>, <column2_name> = <value, corresponding column2 value>,
        WHERE <Where_column_name> in < list of corresponding conditional value>;


        UPDATE users
        SET email = %s, status = %s
        WHERE user_id = %s

        Parameters:

        connection               - MSSQL login connection
        cursor                   - MSSQL connection cursor
        table_name               - table in MSSQL to update
        df                       - dataframe
        columns_to_update        - column names in MSSQL table to update
        where_column_name        - single field, condition for update

        Return:                  - None - update records
        """
        # try except block
        try:
            # log to console, creating update statement to upload
            log.info("[Creating Update SQL statement...]")
            # create beginning of update, add table name
            # create beginning of update string, add table name
            sql_update = f"UPDATE {table_name} SET "
            # create string of comma delimited columns to add to the sql string
            col_list = ""
            # create an array of columns to upload starting with where column,
            df_col_list = []
            # add columns to list to trim down the which columns in dataframe are sent in update
            df_col_list.extend(columns_to_update)
            # loop through columns to populate several variables
            for col in columns_to_update:
                # assume table and dataframe column names are identical, rename df column names if mismatch
                sql_update = sql_update + f"{col} = ?, "
                # add column to list to add into sql string post for loop
                col_list = col_list + col + ", "
            # remove extra white space and comma from string
            sql_update = sql_update[:-2]
            # remove extra white space and comma from string
            col_list = col_list[:-2]
            # add variables to sql string
            sql_update = sql_update + f" WHERE {where_column_name} = ?"
            # add where column to list at the end
            df_col_list.extend([where_column_name])
            # trim dataframe based on columns to include in update
            df_to_update = df[df_col_list]
            # convert the rows in the dataframe into a list of tuples
            data = [tuple(x) for x in df_to_update.values]
            # execute insert of records
            cursor.executemany(sql_update, data)
            # commit the sql statement
            connection.commit()
        # exception block - error updating rows in mssql table
        except Exception as e:
            # log error when attempting to update rows in mssql table
            log.exception(f"[Error updating rows in mssql table...{e}]")

    def delete_rows_in_mssql_table(self, connection, cursor, table_name, column_name, record_list):
        """Description: generate a query string to delete records from a MSSQL table
           Parameters:

           connection               - MSSQL login connection
           cursor                   - MSSQL connection cursor
           table_name               - table in MSSQL to update
           columns_name             - column name in MSSQL table with key used to delete
           record_list              - list of key IDs to delete records

           Return:                  - None - delete records
        """
        # try except block
        try:
            # Example with parameterization
            sql_delete = "DELETE FROM " + table_name + " WHERE " + column_name + " IN " + record_list + ";"
            # execute the deletion of records
            cursor.execute(sql_delete)
            # commit the sql statement
            connection.commit()
            # log to console deleting records
            log.info(f"[Deleting records in table: {table_name}...]")
        # exception block - error deleting rows in mssql table
        except Exception as e:
            # log error when deleting rows in mssql table
            log.exception(f"[Error deleting rows in mssql table...{e}]")

class MySQL_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           can add login credentials as instance variables to utilize in functions
        """

    def login_to_mysql(self, server = None, database = "", username = None, password = None, create_engine = False, driver = "{MySQL ODBC 8.0 Unicode Driver}"):
        """
        Description: login to a MySQL server and return a cursor object to query with
        Parameters:

        server          - IP address of server, I.E. 127.0.0.1
        database        - Database name
        Username        - string, mysql Username
        Password        - string, mysql Password
        create_engine   - create an engine or connection
        driver          - SQL Server Driver use {SQL Driver} or DRIVER={MySQL ODBC 8.0 Unicode Driver}


        Return:         - MySQL engine
        """
        # try except block
        try:
            # if using an engine interact directly between dataframe and database
            if create_engine:
                # log to console engine is created
                log.info("[MySQL engine connected...]")
                # create engine to connect to MySQL
                engine = create_engine("mysql+pymysql://" + username + ":" + password + "@" + server + "/" + database)
                # return engine to perform operations with
                return engine
            # create a conneciton and cursor to query with
            else:
                # create connection to mysql table
                connection = pymysql.connect(host=server,user=username,password=password,database=database)
                # establish cursor to execute sql commands
                cursor = connection.cursor()
                # return both to perform operations
                return (connection, cursor)
        # exception block - error logging into mysql
        except Exception as e:
            # log error when logging into mysql
            log.exception(f"[Error logging into mysql...{e}]")

    def query_mysql_return_dataframe(self, query, connection):
        """
        Description: query a MySQL server with a logged in cursor and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query              - query string
        engine             - engine used to query database and load results into dataframe

        Return:            - pandas.DataFrame
        """
        # try except block
        try:
            # log to console beginning query against mssql database
            log.info("[Querying MySQL DB...]")
            # read query into dataframe
            df = pd.read_sql(query, con = connection)
            # return the dataframe of results from the MySQL table
            return df
        # exception block - error querying mysql and returning a dataframe
        except Exception as e:
            # log error when querying mysql and returning a dataframe
            log.exception(f"[Error querying mysql and returning a dataframe...{e}]")

    def insert_dataframe_into_mysql_table(self, connection, cursor, df, table_name, index = False):
        """Description: attempt to insert an entire dataframe into a MySQL table
        Parameters:

        engine          - database engine
        df              - dataframe to insert into the MySQL table
        tablename       - table to insert records into
        index           - attempt to convert the index into a column to use on the insert, default to false

        return:         - none - insert records into mysql
        """

        # try except block
        try:
            # if the df column list matches the table, use all columns
            log.info(f"[Uploading Dataframe to MySQL DB Table: {table_name}...]")
            # generate a list of all columns
            cols = ",".join([k for k in df.dtypes.index])
            # generate a list of %s value place holders
            values = ",".join(['%s' for _ in df.dtypes.index])
            # generate the sql commit with the dataframe
            sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, cols, values)
            print(sql)
            # convert the rows in the dataframe into tuples
            data = [tuple(x) for x in df.values]
            print(data)
            # insert rows into table
            cursor.executemany(sql, data)
            # commit to write to table
            connection.commit()
            # log error when inserting dataframe into postgres table
            log.info(f"[inserting dataframe rows into mysql table: {table_name}...]")
        # exception block - error inserting dataframe into mysql table
        except Exception as e:
            # log error when inserting dataframe into mysql table
            log.exception(f"[Error inserting dataframe into mysql table: {table_name}...{e}]")

    def update_rows_in_mysql_table(self, connection, cursor, df, table_name, columns_to_update, where_column_name):
        """
        Description: update multiples columns in MySQL table from a dataframe on a where in list condition

        sql_update =  example:
        UPDATE <table_name>
        SET <column1_name> = <value, corresponding column1 value>, <column2_name> = <value, corresponding column2 value>,
        WHERE <Where_column_name> in < list of corresponding conditional value>;


            UPDATE users
            SET email = %s, status = %s
            WHERE user_id = %s

        Parameters:

        engine                   - MySQL engine engine
        Connection               - MySQL connection
        cursor                   - MySQL connection cursor instance
        df                       - dataframe
        table_name               - table in MySQL to update
        columns_to_update        - column names in MySQL table to update
        where_column_name        - single field, condition for update

        Return:                  - None - delete records
        """
        # try except block
        try:
            # log to console, creating update statement to upload
            log.info("[Creating Update SQL statement...]")
            # create beginning of update string, add table name
            sql_update = f"UPDATE {table_name} SET "
            # create string of comma delimited columns to add to the sql string
            col_list = ""
            # create an array of columns to upload starting with where column,
            df_col_list = []
            # add columns to list to trim down the which columns in dataframe are sent in update
            df_col_list.extend(columns_to_update)
            # loop through columns to populate several variables
            for col in columns_to_update:
                # assume table and dataframe column names are identical, rename df column names if mismatch
                sql_update = sql_update + f"{col} = %s, "
                # add column to list to add into sql string post for loop
                col_list = col_list + col + ", "
            # remove extra white space and comma from string
            sql_update = sql_update[:-2]
            # remove extra white space and comma from string
            col_list = col_list[:-2]
            # add variables to sql string
            sql_update = sql_update + f" WHERE {where_column_name} = %s"
            # add where column to list at the end
            df_col_list.extend([where_column_name])
            # trim dataframe based on columns to include in update
            df_to_update = df[df_col_list]
            # convert the rows in the dataframe into a list of tuples
            data = [tuple(x) for x in df_to_update.values]
            # execute insert of records
            cursor.executemany(sql_update, data)
            # commit the sql statement
            connection.commit()
            # log to console commiting update to table now
            log.info(f"[Commiting update to MySQL table: {table_name}...]")
        # exception block - error updating rows in mysql table
        except Exception as e:
            # log error when updating rows in mysql table
            log.exception(f"[Error updating rows in mysql table: {table_name}...{e}]")

    def delete_rows_in_mysql_table(self, connection, cursor,  table_name, column_name, record_list):
        """Description: generate a query string to delete records from a MySQL table
           Parameters:

           engine                   - MySQL login connection
           table_name               - table in MySQL to update
           columns_name             - column name in MySQL table with key used to delete
           record_list              - list of key IDs to delete records

           Return:                  - None - delete records
        """
        # try except block
        try:
            # Example with parameterization
            sql_delete = "DELETE FROM " + table_name + " WHERE " + column_name + " IN " + record_list + ";"
            print(sql_delete)
            # execute the sql to delete records on the table
            cursor.execute(sql_delete)
            # commit the action deleting records off the table
            connection.commit()
            # log to console commiting update to table now
            log.info(f"[Commiting delete to MySQL table: {table_name}...]")
        # exception block - error deleting rows in mysql table
        except Exception as e:
            # log error when deleting rows in mysql table
            log.exception(f"[Error deleting rows in mysql table: {table_name}...{e}]")

class EC2_S3_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           # Alternatively, using awswrangler for more robust S3 integration
           # try:
           #     df_wr = wr.s3.read_csv(path=f"s3://your-s3-bucket-name/path/to/your/data.csv")
           #     print("DataFrame from S3 (using awswrangler):")
           #     print(df_wr.head())
           # except Exception as e:
           #     print(f"Error reading CSV from S3 with awswrangler: {e}")
        """

    def create_s3_client(self, service_name, region_name, aws_access_key_id, aws_secret_access_key):
        """
        Description: # Connect to an S3 bucket using an IAM user access key pair

        Parameters:
        service_name            - string, default = s3
        region_name             - string, default us-east-2
        aws_access_key_id       - string, set up IAM User and create access key
        aws_secret_access_key   - string, set up IAM User and create access key

        Return:                 - boto3.client()
        """
        # try except block on uploading the dataframe to s3
        try:
            # log to console setting up client to access s3 buckets
            log.info("[Initiating boto3 client connection...]")
            return boto3.client(service_name = service_name,
                                region_name = region_name,
                                aws_access_key_id = aws_access_key_id,
                                aws_secret_access_key = aws_secret_access_key)
        except Exception as e:
            # log error when attempting to upload file to s3 bucket
            log.exception(f"[Error initiating boto3 client connection]")

    def upload_dataframe_to_s3(self, df, bucket_name = "your-s3-bucket-name", object_key = "path/to/your/file.txt"):
        """
        Description: # Define your bucket name, object key (file path in S3), and local file path

        Parameters:
        df              - pandas dataframe to upload as csv
        bucket_name     - string, name of s3 bucket
        object_key      - string, name of file to upload dataframe into, make sure to end with .csv


        Return:         - None - upload dataframe to s3 bucket
        """
        # set path string variable of uri to upload file to
        s3_path = f"s3://{bucket_name}/{object_key}"
        # try except block on uploading the dataframe to s3
        try:
            # log to console about to upload dataframe to s3 bucket
            log.info(f"[Uploading dataframe as file: {object_key} to s3 bucket: {bucket_name}...]")
            # upload dataframe to s3 bucket
            df.to_csv(s3_path, index = False)
        except Exception as e:
            # log error when attempting to upload file to s3 bucket
            log.exception(f"[Error uploading CSV to S3: {e} at path: {s3_path}]")

    def download_dataframe_from_s3(self, bucket_name = "your-s3-bucket-name", object_key = "path/to/your/data_file.csv"):
        # Read CSV directly from S3 using pandas
        """
        Description: # Define your bucket name, object key (file path in S3), and local file path

        Parameters:
        bucket_name     - string, name of s3 bucket
        object_key      - string, name of file to upload dataframe into, make sure to end with .csv

        Return:         - pandas dataframe
        """
        # set path string variable of uri to upload file to
        s3_path = f"s3://{bucket_name}/{object_key}"
        # try except block to download csv from s3 bucket
        try:
            # read the s3 object into a dataframe
            df = pd.read_csv(s3_path)
            # log successfully loading the csv into a dataframe
            log.info(f"[File '{object_key}' downloaded from '{bucket_name}' successfully.]")
            #return the downloaded file as dataframe
            return df
        except Exception as e:
            # log error to console, couldn't process request
            log.exception(f"[Error downloading CSV from S3: {e}]")

    def delete_dataframe_in_s3(self, s3_client, bucket_name, file_key):
        """
        Description: delete a file from an s3 bucket with a logged in boto3 client
        Parameters:
        s3_client       - boto3 client instance
        bucket_name     - string, name of bucket in s3
        file_key        - string, file to be deleted

        Return:         - response from s3_client.delete_object()
        """
        # try except block attempting to delete a file from s3 bucket
        try:
            # get response from attempting to delete object/file from bucket
            response = s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            #check the response body for status of 204 meaning success and no message body to send back
            if(response["ResponseMetadata"]["HTTPStatusCode"] == 204):
                # log to console successful attempt to delete file from bucket
                log.info(f"[File '{file_key}' deleted successfully from bucket '{bucket_name}'.]")
            # return the response to view
            return response
        # catch exception if the bucket name does not exist
        # except s3_client.exceptions.NoSuchBucket as e:
        #     # log to console no bucket with a matching name is found
        #     log.info(f"[Error deleting file,  no bucket found: '{bucket_name}': {e}]")
        # # catch exception if the object does not exist in the s3 bucket
        # except s3_client.exceptions.NoSuchKey as e:
        #     # log to console no file in this bucket is found
        #     log.info(f"[Error deleting file; in bucket '{bucket_name}' no file found: '{file_key}': {e}]")
        # any other general exception
        except Exception as e:
            log.exception(f"[Error deleting file '{file_key}': {e}]")

class MongoDB_Utilities:
    def __init__(self):
        """
        Constructor Parameters:
        - currently no customization used.

        list of ETL functions for MongoDB using pymongo
        # bulk_write(), as long as UpdateMany or DeleteMany are not included.
        # delete_one()
        # delete_many()
        # insert_one()
        # insert_many()
        # replace_one()
        # update_one()
        # update_many()
        # find_one_and_delete()
        # find_one_and_replace()
        # find_one_and_update()
        """

    def create_mongo_client(self, uri = "mongodb://localhost:27017/"):
        """
        Description: connect to a mongodb database and return a client
        - in the future may add other paramters for non-localhost

        Parameters:
        uri         - string, path to the mongodb database

        Return:     - MongoClient
        """
        # try except block on connecting to a mongodb client,
        # will not error connecting to nothing or random strings, not sure why
        try:
            # create client using an URI
            client = MongoClient(uri)
            # log to console setting up client to access s3 buckets
            log.info(f"[Initiating mongodb client connection: {uri}]")
            #return the client
            return client
        # exception block - error connecting to mongodb
        except Exception as e:
            # log error when attempting to upload file to s3 bucket
            log.exception(f"[Error initiating mongo client connection... {e}]")

    def insert_dataframe_into_mongodb_collection(self, df, client, db, collection, field = None, value = None, close_connection = True):
        """
        Description: upload a dataframe into a mongodb database collection
        if the collection does not exist, it will be created, then records inserted into it.
        - in future can perform query against mongo to check if collection exists first
          so as not to accidentally create new collections

        Parameters:
        client              - MongoClient connection
        db                  - string, database name in mongodb
        collection          - string, collection name in database to insert records
        close_connection    - boolean, default to false, true to close mongodb connection

        Return:             - result from upload of records, single record upload will return id
        """
        # try except block on connecting to a mongodb client
        try:
            # if there is only 1 record in the dataframe to insert
            if len(df) == 1:
                # convert the dataframe to a dictionary
                record_to_insert = df.to_dict("records")[0]
                # attempt to insert the record as a dictionary to the mongodb collection
                result = collection.insert_one(record_to_insert)
                # log to console results of inserting record to collection
                log.info(f"[Single record inserted with ID: {result.inserted_id}]")
            # if the dataframe has more than 1 record to upload
            else:
                # convert the dataframe to a list of dictionaries
                records_to_insert = df.to_dict("records")
                # attempt to upload the list of dictionaries
                result = collection.insert_many(records_to_insert)
                # log the results of the upload to the console
                log.info(f"[Multiple records inserted: {result}]")
            # if the user wants to close the mongo db connection after retreiving the data
            if close_connection:
                # close the connection to mongoDB
                client.close()
            # return the results of the upload
            return result
        # exception block - error inserting records into collection
        except Exception as e:
            # log error when attempting to insert records into collection
            log.exception(f"[Error inserting records into mongodb collection... {e}]")

    def query_dataframe_from_mongodb_collection(self, client, db, collection, field = None, value = None, close_connection = False):
        """
        Description: query against a mongodb database collection,
        and either return a subset of records in the collection or the entire collection
        leave field and value set to None to retrieve all records in collection

        Parameters:
        client              - MongoClient connection
        db                  - string, database name in mongodb
        collection          - string, collection name in database to insert records
        field               - string, collection field or pandas column name
        value               - string, records that match the value for the above field in a collection
        close_connection    - boolean, default to false, true to close mongodb connection

        Return:             - pandas dataframe
        """
        # try except block on connecting to a mongodb client
        try:
            # if both field and value are not none, for records matching this pair
            if field is not None and value is not None:
                # find all records in collection with field-value pair
                cursor = collection.find({field : value})
            # else - if field and value are none, query all records
            else:
                # find all records in the collection
                cursor = collection.find({})
            # convert the results of the query into a dataframe
            df = pd.DataFrame(list(cursor))
            # if the user wants to close the mongo db connection after retreiving the data
            if close_connection:
                # close the connection to mongoDB
                client.close()
            # log results converted to dataframe
            log.info(f"[Records converted to a pandas dataframe]")
            # return the generated dateframe from the mongodb collection
            return df
        # exception block - error retrieving records from collection
        except Exception as e:
            # log error when attempting to upload file to s3 bucket
            log.exception(f"[Error querying records/converting to a dataframe...{e}]")

    def delete_dataframe_from_mongodb_collection(self, df, client, db, collection, field = None, value = None, field_is_unique = False, close_connection = False):
        """
        Description: delete a single record or a list of records based on a single condition
        add functionality to with df.iterrows(): to delete based on condition in every row of dataframe
        need new parameter for looping delete on multiple conditions

        Parameters:
        client              - MongoClient connection
        db                  - string, database name in mongodb
        field               - string, unique id column used to identify the deleting records
        Value               - string, unique id value used to identify the deleting records
        collection          - string, collection name in database to insert records
        field_is_unique     - boolean, default to false, true if column has only unique values
        close_connection    - boolean, default to false, true to close mongodb connection

        Return:             - result from upload of records, single record upload will return id
        """
        # try except block deleting records from a mongodb collection based on single column value pair
        try:
            # if deleting records based on single condition
            if field is not None and value is not None:
                # create query dict from the record and field
                record_to_delete_query = {field : value}
                # attempt to insert the record as a dictionary to the mongodb collection
                result = collection.delete_many(record_to_delete_query)
                # log to console results of deleting record to collection
                log.info(f"[Records deleted based on field value pair {field} : {value}; results: {result}]")
            # loop through dataframe andf delete one record ata a time based on column and row value
            else:
                # convert the dataframe to a dictionary
                record_to_delete = df.to_dict("records")
                # loop through each row
                for index, row in df.iterrows():
                    # create query dict from the record and field
                    record_to_delete_query = {field : row[field]}
                    # if field has only unique values
                    if field_is_unique:
                        # attempt to delete a single record from the collection
                        result = collection.delete_one(record_to_delete_query)
                    # the field may have duplicates in other records
                    else:
                        # attempt to delete multiple records from the collection
                        result = collection.delete_many(record_to_delete_query)
                    # log to console results of deleting records to collection
                    log.info(f"[Multiple records deleted: {result}]")
            # if the user wants to close the mongo db connection after retreiving the data
            if close_connection:
                # close the connection to mongoDB
                client.close()
            # return the results of the upload
            return result
        # exception block - error deleting records from mongodb collection
        except Exception as e:
            # log error when attempting to delete records from mongodb collection
            log.exception(f"[Error deleting records from mongodb collection...{e}]")

    def update_dataframe_in_mongodb_collection(self, df, client, db, collection, field = None, close_connection = False):
        """
        Description: upload a refined dataframe to perform an update on every field of every record in the mongodb collection,
        can add in the future to only perform an update on a subset of fields.
        can also add update on single field and value to use update_many(filter_query, update_data)
        add a tracker variable to keep track of updated records.

        Parameters:
        client              - MongoClient connection
        db                  - string, database name in mongodb
        field               - string, unique id column used to identify the updating records
        collection          - string, collection name in database to update records
        close_connection    - boolean, default to false, true to close mongodb connection

        Return:             - result from upload of records, single record upload will return id
        """
        # try except block deleting records from a mongodb collection based on single column value pair
        try:
            # loop through each row  of dataframe to update independelty,
            # assumes update key is unique so cannot use update_many
            for index, row in df.iterrows():
                # create dictionary to identify a record using a field and value pair to update other fields on same record
                filter_query = {field: row[field]}
                # remove the id field and use all other fields on the row to create a dictionary
                update_dict = row.drop(field).to_dict()
                # create update call dictionary with primary key = '$set'
                update_data = {"$set": update_dict}
                # attempt to update a single record in the mongo db collection
                result = collection.update_one(filter_query, update_data)
                # log to console results of deleting record to collection
                log.info(f"[Record updated based on field value pair {field} : {row[field]}; results: {result}]")
            # if the user wants to close the mongo db connection after retreiving the data
            if close_connection:
                # close the connection to mongoDB
                client.close()
            # return the results of the upload
            return result
        # exception block - error updating records in mongodb collection
        except Exception as e:
            # log error when attempting to update records in mongodb collection
            log.exception(f"[Error updating records in mongodb collection...{e}]")

class Postgres_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           can add login credentials as instance variables to utilize in functions
        """

    def login_to_postgresql(self, host = "localhost", database = "financial_db", user = "postgres", password = "postgres", port = 5432):
        """
        Description: login to a postgres server and return a cursor object to query with
        Parameters:

        host            - string, default to "localhost"
        database        - string, default to "financial_db"
        user            - string, default to "postgres"
        password        - string, default to "postgres"
        port            - int, default to 5432

        Return:         - postgres cursor
        """
        # try except block
        try:

            # log to console status of logging into database
            log.info(f"[Logging into postgres DB: {database}]")
            # establish a connection
            cursor_connection = psycopg2.connect(host=host,
                                                 database=database,
                                                 user=user,
                                                 password=password,
                                                 port=port
                                                 )

            # convert the instance to a cursor
            cursor = cursor_connection.cursor()
            # log to console the cursor is created
            log.info("[Creating Cursor]")
            # return the connection and cursor to use to query against the database
            return (cursor_connection, cursor)
        # exception block - error
        except Exception as e:
            # log error when logging in
            log.exception(f"[Error Logging into postgres DB...{e}]")

    def query_postgres_return_dataframe(self, query, connection):
        """
        Description: query a Postgres server with a logged in connection and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query           - query string
        connection      - db connection to execute the query with

        Return:         - pandas.DataFrame
        """
        # try except block
        try:
            # log to console beginning query against postgres database
            log.info("[Querying Postgres DB...]")
            # execute query with cursor
            results_df = pd.read_sql(query, connection)
            # return the results of the query as a pandas data frame
            return results_df
        # exception block - error querying postgres table
        except Exception as e:
            # log error when querying postgres table
            log.exception(f"[Error querying postgres table...{e}]")

    def insert_dataframe_into_postgres_table(self, connection, cursor, df, table_name, column_types = [], cols = "", close_connection = True):
        """Description: insert a dataframe into a postgres table, the whole dataframe will be inserted
        Parameters:

        connection              - Postgres database connection
        cursor                  - Postgres connection cursor
        df                      - dataframe to insert
        table_name              - table name of database to insert records into
        column_types            - set column datatypes before insert, auto datatype setting can sometimes be inaccurate
        cols                    - list of columns, currently experimental
        close_connection        - boolean, close connection after insert.

        return:                 - none - insert records into postgres # DEBUG:
        """
        # try except block
        try:
            # log error when inserting dataframe into postgres table
            log.info(f"[preparing datagrame before inserting into postgres table: {table_name}...]")
            # generate a list of all columns
            cols = ",".join([k for k in df.dtypes.index])
            # generate the sql commit with the dataframe
            sql = "INSERT INTO {0} ({1}) VALUES %s".format(table_name, cols)
            # loop through each column to convert the type every value
            for index, col in enumerate(df.columns):
                # confirm the index is still within range of acceptable indexes
                if index < len(column_types):
                    # check if type == int
                    if column_types[index] == "int":
                        df[col] = df[col].astype(int)
                    # check if type == string
                    if column_types[index] == "str":
                        df[col] = df[col].astype(str)
                    # check if type == float
                    if column_types[index] == "float":
                        df[col] = df[col].astype(float)
                    # check if type == boolean
                    if column_types[index] == "bool":
                        df[col] = df[col].astype(bool)
                    # check if type == date
                    if column_types[index] == "date":
                        # do nothing
                        continue
            # convert the rows in the dataframe into tuples
            data = [tuple(x) for x in df.values]
            # log error when inserting dataframe into postgres table
            log.info(f"[inserting dataframe rows into postgres table: {table_name}...]")
            # execute insert of records
            execute_values(cursor, sql, data)
            # commit the sql statement
            connection.commit()
            # close the connection if desired
            if close_connection:
                # close the connection
                connection.close()
        # exception block - error inserting dataframe into postgres table
        except Exception as e:
            # log error when inserting dataframe into postgres table
            log.exception(f"[Error inserting dataframe into postgres table: {table_name}...{e}]")

    def update_rows_in_postgres_table(self, connection, cursor,  df, table_name, columns_to_update, where_column_name):
        """
        Description: update multiples columns in Postgres table from a dataframe on where table.id_field = df.id_field

        sql_update =  example:
        execute_values(cur,
                       UPDATE test SET v1 = data.v1, v2 = data.v2 FROM (VALUES %s) AS data (id, v1, v2)
                       WHERE test.id = data.id,
                       [(1, 'a', 20), (4, 'b', 50)])

        Parameters:

        connection               - Postgres login connection
        cursor                   - Postgres connection cursor
        df                       - dataframe of records to upload in update call
        table_name               - table in Postgres to update
        columns_to_update        - column names in Postgres table to update
        where_column_name        - single field, condition for update

        Return:                  - None - update records
        """
        # try except block
        try:
            # log to console, creating update statement to upload
            log.info("[Creating Update SQL statement...]")
            # create beginning of update string, add table name
            sql_update = f"UPDATE {table_name} SET "
            # create string of comma delimited columns to add to the sql string
            col_list = ""
            # create an array of columns to upload starting with where column,
            df_col_list = [where_column_name]
            # add columns to list to trim down the which columns in dataframe are sent in update
            df_col_list.extend(columns_to_update)
            # loop through columns to populate several variables
            for col in columns_to_update:
                # assume table and dataframe column names are identical, rename df column names if mismatch
                sql_update = sql_update + f"{col} = data.{col}, "
                # add column to list to add into sql string post for loop
                col_list = col_list + col + ", "
            # remove extra white space and comma from string
            sql_update = sql_update[:-2]
            # remove extra white space and comma from string
            col_list = col_list[:-2]
            # add variables to sql string
            sql_update = sql_update + f" FROM (VALUES %s) AS data ({where_column_name}, {col_list}) WHERE {table_name}.{where_column_name} = data.{where_column_name}"
            # trim dataframe based on columns to include in update
            df_to_update = df[df_col_list]
            # convert the rows in the dataframe into a list of tuples
            data = [tuple(x) for x in df_to_update.values]
            # execute insert of records
            execute_values(cursor, sql_update, data)
            # log to console commiting update to table now
            log.info(f"[Commiting update to postgres table: {table_name}...]")
            # commit the sql statement
            connection.commit()
        # exception block - error updating rows in postgres table
        except Exception as e:
            # log error when attempting to update rows in postgres table
            log.exception(f"[Error updating rows in postgres table...{e}]")

    def delete_rows_in_postgres_table(self, connection, cursor, table_name, column_name, record_list):
        """Description: generate a query string to delete records from a Postgres table
           Parameters:

           connection               - Postgres login connection
           cursor                   - Postgres connection cursor
           table_name               - table in Postgres to update
           columns_name             - column name in Postgres table with key used to delete
           record_list              - list of key IDs to delete records

           Return:                  - None - delete records
        """
        # try except block
        try:
            # Example with parameterization
            sql_delete = "DELETE FROM " + table_name + " WHERE " + column_name + " IN " + record_list + ";"
            # execute the deletion of records
            cursor.execute(sql_delete)
            # commit the sql statement
            connection.commit()
            # log to console deleting records
            log.info(f"[Deleting records in table: {table_name}...]")
        # exception block - error deleting rows in mssql table
        except Exception as e:
            # log error when deleting rows in postgres table
            log.exception(f"[Error deleting rows in postgres table...{e}]")

class Snowflake_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.

           can add login credentials as instance variables to utilize in functions
        """

    def login_to_snowflake(self, user='your_user', password='your_password', account='your_account', warehouse='your_warehouse', database='your_database', schema='your_schema'):
        """
        Description: login to a Snowflake server and return a cursor object to query with
        Parameters:

        user='your_user',
        password='your_password',
        account='your_account',
        warehouse='your_warehouse',
        database='your_database',
        schema='your_schema'

        Return:         - Snowflake cursor
        """
        # try except block
        try:
            # log to console status of logging into database
            log.info(f"[Logging into Snowflake DB: {database}]")
            # establish a connection
            cursor_connection = snowflake.connector.connect(
                                    user=user,
                                    password=password,
                                    account=account, # e.g., 'youraccount.us-east-1'
                                    warehouse=warehouse,
                                    database=database,
                                    schema=schema
                                )

            # convert the instance to a cursor
            cursor = cursor_connection.cursor()
            # log to console the cursor is created
            log.info("[Creating Cursor]")
            # return the connection and cursor to use to query against the database
            return (cursor_connection, cursor)
        # exception block - error
        except Exception as e:
            # log error when logging in
            log.exception(f"[Error Logging into Snowflake DB...{e}]")

    def query_snowflake_return_dataframe(self, query, connection, cursor):
        """
        Description: query a snwoflake server with a logged in connection and
        process results into a pandas dataframe the return the dataframe.
        Parameters:

        query           - query string
        connection      - db connection to execute the query with
        cursor          - cursor to perform executemany()

        Return:         - pandas.DataFrame
        """
        # try except block
        try:
            # log to console beginning query against snowflake database
            log.info("[Querying snwoflake DB...]")
            # execute query with cursor
            results_df = pd.read_sql(query, connection)
            # return the results of the query as a pandas data frame
            return results_df
        # exception block - error querying snowflake table
        except Exception as e:
            # log error when querying snowflake table
            log.exception(f"[Error querying snowflake table...{e}]")

    def insert_dataframe_into_snowflake_table(self, connection, cursor, df, table_name, index = False):
        """Description: attempt to insert an entire dataframe into a Snowflake table
        Parameters:

        connection      - db connection to execute the query with
        cursor          - cursor to perform executemany()
        df              - dataframe to insert into the Snowflake table
        tablename       - table to insert records into
        index           - attempt to convert the index into a column to use on the insert, default to false

        return:         - none - insert records into snowflake
        """

        # try except block
        try:
            # if the df column list matches the table, use all columns
            log.info(f"[Uploading Dataframe to Snowflake DB Table: {table_name}...]")
            # generate a list of all columns
            cols = ",".join([k for k in df.dtypes.index])
            # generate a list of %s value place holders
            values = ",".join(['%s' for _ in df.dtypes.index])
            # generate the sql commit with the dataframe
            sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, cols, values)
            print(sql)
            # convert the rows in the dataframe into tuples
            data = [tuple(x) for x in df.values]
            print(data)
            # insert rows into table
            cursor.executemany(sql, data)
            # commit to write to table
            connection.commit()
            # log error when inserting dataframe into postgres table
            log.info(f"[inserting dataframe rows into snowflake table: {table_name}...]")
        # exception block - error inserting dataframe into snowflake table
        except Exception as e:
            # log error when inserting dataframe into snowflake table
            log.exception(f"[Error inserting dataframe into snowflake table: {table_name}...{e}]")

    def update_rows_in_snowflake_table(self, connection, cursor, df, table_name, columns_to_update, where_column_name):
        """
        Description: update multiples columns in Snowflake table from a dataframe on a where in list condition

        sql_update =  example:
        UPDATE <table_name>
        SET <column1_name> = <value, corresponding column1 value>, <column2_name> = <value, corresponding column2 value>,
        WHERE <Where_column_name> in < list of corresponding conditional value>;


            UPDATE users
            SET email = %s, status = %s
            WHERE user_id = %s

        Parameters:

        engine                   - snowflake engine engine
        Connection               - snowflake connection
        cursor                   - snowflake connection cursor instance
        df                       - dataframe
        table_name               - table in snowflake to update
        columns_to_update        - column names in snowflake table to update
        where_column_name        - single field, condition for update

        Return:                  - None - delete records
        """
        # try except block
        try:
            # log to console, creating update statement to upload
            log.info("[Creating Update SQL statement...]")
            # create beginning of update string, add table name
            sql_update = f"UPDATE {table_name} SET "
            # create string of comma delimited columns to add to the sql string
            col_list = ""
            # create an array of columns to upload starting with where column,
            df_col_list = []
            # add columns to list to trim down the which columns in dataframe are sent in update
            df_col_list.extend(columns_to_update)
            # loop through columns to populate several variables
            for col in columns_to_update:
                # assume table and dataframe column names are identical, rename df column names if mismatch
                sql_update = sql_update + f"{col} = %s, "
                # add column to list to add into sql string post for loop
                col_list = col_list + col + ", "
            # remove extra white space and comma from string
            sql_update = sql_update[:-2]
            # remove extra white space and comma from string
            col_list = col_list[:-2]
            # add variables to sql string
            sql_update = sql_update + f" WHERE {where_column_name} = %s"
            # add where column to list at the end
            df_col_list.extend([where_column_name])
            # trim dataframe based on columns to include in update
            df_to_update = df[df_col_list]
            # convert the rows in the dataframe into a list of tuples
            data = [tuple(x) for x in df_to_update.values]
            # execute insert of records
            cursor.executemany(sql_update, data)
            # commit the sql statement
            connection.commit()
            # log to console commiting update to table now
            log.info(f"[Commiting update to snowflake table: {table_name}...]")
        # exception block - error updating rows in snowflake table
        except Exception as e:
            # log error when updating rows in snowflake table
            log.exception(f"[Error updating rows in snowflake table: {table_name}...{e}]")

    def delete_rows_in_snowflake_table(self, connection, cursor,  table_name, column_name, record_list):
        """Description: generate a query string to delete records from a snowflake table
           Parameters:

           engine                   - snowflake login connection
           table_name               - table in snowflake to update
           columns_name             - column name in snowflake table with key used to delete
           record_list              - list of key IDs to delete records

           Return:                  - None - delete records
        """
        # try except block
        try:
            # Example with parameterization
            sql_delete = "DELETE FROM " + table_name + " WHERE " + column_name + " IN " + record_list + ";"
            print(sql_delete)
            # execute the sql to delete records on the table
            cursor.execute(sql_delete)
            # commit the action deleting records off the table
            connection.commit()
            # log to console commiting update to table now
            log.info(f"[Commiting delete to snowflake table: {table_name}...]")
        # exception block - error deleting rows in snowflake table
        except Exception as e:
            # log error when deleting rows in snowflake table
            log.exception(f"[Error deleting rows in snowflake table: {table_name}...{e}]")

class Custom_Utilities:
    def __init__(self):
        """Constructor Parameters:
           - currently no customization used.
        """

    def merge_dfs(self, left, right, left_on, right_on, how ="inner", suffixes = ("_left", "_right"), indicator = True, validate = None):
        """
        Description: merge two dataframes based on list of columns to join on
        Parameters:

        left                - left dataframe
        right               - right dataframe
        left_on             - list of string column names to perform merge on
        right_on            - list of string column names to perform merge on
        how='inner'         - what type of join to use for the merge, inner reduces duplicate the best
        suffixes            - tuple of string to append to the end of columns from each dataframe
        indicator           - indicate left/right/both dataframe the row is found in
        validate            - can check for 1:1/1:many/many:1/many:many merges

        Return: merged dataframe
        """
        # try except block
        try:
            # log to console merging of dataframe is occuring
            log.info("[Merging dataframes...]")
            # return merged dataframe
            return pd.merge(left=left, right=right,
                            how=how, left_on=left_on, right_on=right_on,
                            suffixes=suffixes, indicator=indicator, validate=validate)
        # exception block - error merging dataframes
        except Exception as e:
            # log error when merging dataframes
            log.exception(f"[Error merging dataframes...{e}]")

    def get_df_diffs(self, left, right, left_on, right_on, how ="inner", suffixes = ("_left", "_right"), indicator = True, validate = None, drop_merge = False):
        """
        Description: merge two dataframes based on list of columns to join on,
                     then return a tuple of 3 dataframes, 1 where records exist in both left and right,
                     1 for left only and 1 for right only
        Parameters:

        left                - left dataframe
        right               - right dataframe
        left_on             - list of string, column names to perform merge on
        right_on            - list of string, column names to perform merge on
        how="inner"         - what type of join to use for the merge, inner reduces duplicate the best
        suffixes            - tuple of string to append to the end of columns from each dataframe
        indicator           - indicate left/right/both dataframe the row is found in
        validate            - can check for 1:1/1:many/many:1/many:many merges
        drop_merge          - remove the _merge column before returning the tuple

        Return: tuple of three dataframes
        """
        # try except block
        try:
            # log to console merging of dataframe is occuring
            log.info("[Merging dataframes...]")
            # merge the two input dataframes
            merged_df = self.merge_dfs(left=left, right=right,
                            how=how, left_on=left_on, right_on=right_on,
                            suffixes=suffixes, indicator=indicator, validate=validate)
            # separate the merged dataframe basedf on _merge value, 1 dataframe for records in both input df
            both_df = merged_df[merged_df["_merge"] == "both"]
            # separate the merged dataframe basedf on _merge value, 1 dataframe for records in the left dataframe
            left_only_df = merged_df[merged_df["_merge"] == "left_only"]
            # separate the merged dataframe basedf on _merge value, 1 dataframe for records in the right dataframe
            right_only_df = merged_df[merged_df["_merge"] == "right_only"]
            # log to console splitting of dataframe is occuring
            log.info("[Analyzing and splitting input dataframes...]")
            # perform cleanup: drop the _merge column before returning tuple
            if drop_merge:
                # drop "_merge" from both_df
                both_df.drop("_merge", inplace = True)
                # drop "_merge" from left_only_df
                left_only_df.drop("_merge", inplace = True)
                # drop "_merge" from right_only_df
                right_only_df.drop("_merge", inplace = True)
            # return a tuple showing records from both input df split among three new dataframes
            return (both_df, left_only_df, right_only_df)
        # exception block - error merging and return tuple of df diff
        except Exception as e:
            # log error when merging and return tuple of df diff
            log.exception(f"[Error merging and return tuple of df diff...{e}]")

    def get_slice_of_dataframe(self, df, starting_index, number_of_records):
        """
        Description:
        Parameters:

        df                  - pandas dataframe to slice
        starting_index      - int, starting index/row to pull records from dataframe
        number_of_records   - how many records to grab from the dataframe

        Return: tuple of three dataframes
        """
        # try except block
        try:
            # log to console slicing of dataframe is occuring
            log.info(f"[Scliging dataframes from index: {starting_index} to {starting_index+number_of_records}...]")
            # create df_to_return variable to not slice original df
            df_to_return = df.iloc[starting_index : starting_index + number_of_records]
            # return the sliced dataframe
            return df_to_return
        # exception block - error slicing dataframe
        except Exception as e:
            # log error when slicing dataframe
            log.exception(f"[Error slicing dataframe, check range of slice...{e}]")

    def format_columns_dtypes(self, df, column_types = [], use_columns = False):
        """
        Description: reformat dataframe columns before merge,
                     needs merge columns to be of same type,
                     honestly better to just update the data type
                     of the merging columns hardcoded instead of every column
        Parameters:

        df              - dataframe to reformat column datatypes
        column_types    - list of strings, each being a datatype to convert the column
        use_columns     - bool, decision to use column_types or not

        Return: dataframe
        """
        # try except block
        try:
            # log to console updating the dataframe datatypes
            log.info("[updating datatypes of dataframe...]")
            # loop through each column in the dataframe to format
            for index, col in enumerate(df.columns):
                # if using custom list of datatypes
                if use_columns:
                    # confirm the loop index exists in the range of columns
                    if index < len(df.columns):
                        # check if type == int
                        if df[col].dtypes == "int64":
                            # set column to type int
                            df[col] = df[col].astype(column_types[index])
                        # check if type == string, date, or object
                        if df[col].dtypes == "object":
                            # set column to type str
                            df[col] = df[col].astype(column_types[index])
                        # check if type == float
                        if df[col].dtypes == "float64":
                            # set column to type float
                            df[col] = df[col].astype(column_types[index])
                        # check if type == boolean
                        if df[col].dtypes == "bool":
                            # set column to type bool
                            df[col] = df[col].astype(column_types[index])
                # attempt to let the script auto sense the datatype to convert
                else:
                    # confirm the loop index exists in the range of columns
                    if index < len(df.columns):
                        # check if type == int
                        if df[col].dtypes == "int64":
                            # set column to type int
                            df[col] = df[col].astype(int)
                        # check if type == string, date, or object
                        if df[col].dtypes == "object":
                            # set column to type str
                            df[col] = df[col].astype(str)
                        # check if type == float
                        if df[col].dtypes == "float64":
                            # set column to type float
                            df[col] = df[col].astype(float)
                        # check if type == boolean
                        if df[col].dtypes == "bool":
                            # set column to type bool
                            df[col] = df[col].astype(bool)
            # return the reformatted dataframes
            return df
        # exception block - error formatting dataframe column datatypes
        except Exception as e:
            # log error when formatting dataframe column datatypes
            log.exception(f"[Error formatting dataframe column datatypes...{e}]")

    def write_df_to_excel(self, dfs, file_name, sheet_names):
        """
        Description: Create a single excel file with multiple tabs, one tab for each dataframe
        Parameters:

        dfs         - list(df), list of dfs, order matters
        file_name   - string, output filename
        sheet_names - list(string), each tab name per df, order matters, must align with list of dfs

        Return: None, write dataframes to files
        """
        # try except block
        try:
            # create instance of ExcelWrite to add sheets creted from dataframes
            writer = pd.ExcelWriter(file_name)
            # loop through list of multiple DataFrame
            # each dataframe will be its own sheet on the document
            log.info("[writing each sheet to excelfile]")
            # loop through the list of each dataframe to write to a sheet
            for index, df in enumerate(dfs):
                # log to console what sheet is being written
                log.info(f"[writing sheet to excel file: {str(sheet_names[index])}]")
                # write the individual dataframe to it's associated sheet
                df.to_excel(writer, sheet_names[index], index = False)
            # log finished looping and now writing file out
            log.info("[saving file to output location]")
            # save the file
            writer.save()
        # exception block - error writing list of dataframe to excel sheets
        except Exception as e:
            # log error when writing list of dataframe to excel sheets
            log.exception(f"[Error writing list of dataframe to excel sheets...{e}]")

    def encode_df(self, df, encoding = "unicode_escape", decoding = "utf-8"):
        """
        Description: encode strings in unicode_escape and decode back to utf-8 for processing records as utf-8
        Parameters:

        df          - pandas.DataFrame, to encode and decode as utf-8
        encoding    - string, default to unicode_escape
        decoding    - string, default to utf-8

        Return:     - pandas.DataFrame
        """
        # try except block
        try:
            # log to console, beginning encoding data in dataframe
            log.info("[encoding query results in DataFrames]")
            # return the encoded data for strings
            return df.map(lambda x : x.encode(encoding).decode(decoding) if isinstance(x, str) else x)
        # exception block - error encoding dataframe values
        except Exception as e:
            # log error when encoding dataframe values
            log.exception(f"[Error encoding dataframe values...{e}]")

    def add_sequence(self, df, group_fields, new_field, changing_fields = None, base_value = 10, increment_value = 10, sort = True, incremental_log = 1000):
        """
        Description: Create a new column that increments every time a group has
                     changes on a specific subgroup of fields, sorting matters,
                     This will sort the dataframe inplace.
        Parameters:

        df                      - DataFrame with the groups sorted
        group_fields            - python list of all fields to group the sequence by
        new_fields              - sequence field
        changing_fields         - optional, can declare what field is changing to compare against the group fields
        base_value              - starting value of the sequence
        increment_value         - incrementing value of the sequence
        sort                    - sort by group fields first then the changing field, changing_field must be declared
        incremental_log         - set how frequent the logger outputs progress

        Return: df              - DataFrame with an added column with value changing sequence
        """
        # try except block
        try:
            # sort the values of the data frame by the sort fields selected
            if sort:
                # log to console beginning sorted
                log.info("[Sorting DataFrame before generating list.]")
                # create list of fields to sort the dataframe by
                sort_fields = group_fields.append(changing_fields)
                # sort the dataframe based on the sort fields
                df.sort_values(sort_fields, inplace = True)
            # log to console what the new field is called that will hold the sequence
            log.info(f"[generating sequence for:  {new_field}]")
            # if there is no field declared how to sequence the group
            if changing_fields == None:
                # iterrate throught the dataframe row by row
                for index, row in df.iterrows():
                    # every 10,000 rows add a log output for timekeeping
                    if index % incremental_log == 0:
                        # log to console a timestamp and number of rows processed
                        log.info(f"[rows processed: {str(index)}]")
                    # if the current row's group is not the same as the previous row's group
                    if int(index) == 0 or not (df.loc[int(index) - 1, group_fields].equals(df.loc[index, group_fields])):
                        # start the sequence again based on the starting base value
                        df.loc[index, new_field] = base_value
                    # the current row's group matches the last row's group values
                    else:
                        # increment the value showing this row's group matches the previous row's group
                        df.loc[index, new_field] = df.loc[int(index) - 1, new_field] + increment_value
            # there is a declared field to sequence the group fields,
            # show what column to sequence the groups by
            else:
                # iterrate through the dataframe row by row
                for index, row in df.iterrows():
                    # if on the first row
                    if int(index) == 0:
                        # set the bast value to begin the sequence on
                        df.loc[index, new_field] = base_value
                    # any row after the first row
                    else:
                        # the current row group value is the same as the previous row, increment the sequence value
                        if (df.loc[int(index) - 1, group_fields].equals(df.loc[index, group_fields])) and not (df.loc[int(index) - 1, changing_fields].equals(df.loc[index, changing_fields])):
                            # increment the sequence value in the new column
                            df.loc[index, new_field] = df.loc[int(index) - 1, new_field] + increment_value
                        # the current row is part of a new group than the previous row
                        else:
                            # start the sequence over again on the current row
                            df.loc[index, new_field] = base_value
            # return the dataframe with the added column
            return df
        # exception block - error creating sequence column for unique members of a group
        except Exception as e:
            # log error creating sequence column for unique members of a group
            log.exception(f"[Error generating sequence for members of groups...{e}]")

    def generate_sql_list_from_df_column(self, df, column, output_file_name = None, return_line = False, output = "file"):
        """
        Description: generate a string list of values from a dataframe column to inject into a query
        Parameters:

        df                      - dataframe to generate sql list from
        column                  - string, column name to generate sql list from
        output_file_name        - string, file location to write sql list to
        return_line             - bool, start a new line for every value, or generate the list all on a single line.
        output                  - string, accepted values are: 'file', 'string'

        Return:                 - string, sql string formatted list of values
        """
        # try except block
        try:
            # begin the string list that will be return by the funciont
            sql_string = "("
            # drop duplicate from the dataframe column based on a subset before generating the SQL list
            unique_df = df.drop_duplicates(subset = [column])
            # loop through the rows of the dataframe to add values to the stirng
            for index, row in unique_df.iterrows():
                # if the string should start a new line after every
                if return_line:
                    # add new value to the sql string on a new line every entry
                    sql_string  = sql_string + "'" + str(row[column]) + "',\n"
                # generate every value of the list on a single line
                else:
                    # add new value to the sql string on the same line
                    sql_string  = sql_string + "'" + str(row[column]) + "',"
            # at the end of all the values added, add the closing parenthesis
            # remove ending new line if used
            if return_line:
                # remove the new line and add ending parenthesis
                sql_string = sql_string[:-2] + ")"
            # since all value are on same line, no need to trim extra new line in string
            else:
                # add ending parenthesis to sql string list
                sql_string = sql_string[:-1] + ")"
            # if the sql string is to be written to a file for further use
            if output == "file" and output_file_name != None:
                # log to console, attempting output sql string to a file
                log.info(f"[Converting DataFrame Column to SQL List in text file: {output_file_name }]")
                # open the file in write mode
                with open(output_file_name, "w") as file:
                    # write the sql string to the file and close once done
                    file.write(sql_string)
            # if the sql string should be returned instead of written to file
            elif output == "string":
                    # retun sql string as list
                    return sql_string
            # if don't write sql string to file, and don't return the string
            else:
                # do nothing
                return None
        # exception block - error creating sql list
        except Exception as e:
            # log error creating sql list from dataframe column
            log.exception(f"[Error generating sql list from dataframe column...{e}]")

    def now(self, ts_format="%Y-%m-%d__%H-%M-%S"):
        """
        Description: return the current time down to the second, generally for creating timestamps of actions
        Parameters:

        ts_format - default to "%Y-%m-%d__%H-%M-%S" - this is the default of salesforce

        Return:     - datetime of right now down to the second.
        """
        # try except block
        try:
            # return datetime of right now
            return datetime.fromtimestamp(time.time()).strftime(ts_format)
        # exception block - error returning a datetime string of now
        except Exception as e:
            # log error when returning a datetime string of now
            log.exception(f"[Error returning a datetime string of now...{e}]")

    def log_message_to_console(self, message="%Y-%m-%d__%H-%M-%S"):
        """
        Description: log any text message to console, use for start and end timer of scripts
        Parameters:

        message     - string
        """
        # try except block
        try:
            # log message to console
            log.info(f"[{message}]")
        # exception block - error returning a datetime string of now
        except Exception as e:
            # log error when returning a datetime string of now
            log.exception(f"[Error logging message...{e}]")
