import pandas as pd
import paramiko
from Configuration.etlconfig import *
import logging
import pytest
import os
import boto3
from io import StringIO

from conftest import connect_to_mysql_database

# Logging configuration
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)

def sales_data_from_linux_server():
    # download sales file from linux server to local via SFTP/ssh
    try:
        logger.info("Sales file from Linux server download started...")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(Linux_hostname, username=Linux_username, password=Linux_password)

        sftp = ssh_client.open_sftp()
        sftp.get(Linux_remote_file_path,Linux_local_file_path)
        sftp.close()
        ssh_client.close()
        logger.info("Sales file from Linux server download completed...")
    except Exception as e:
        logger.error("Error while downloading the sales file from Linux server.")

#parameterizing the filepath,filetype,tablename etc here

def verify_expected_as_file_to_actual_as_database(file_path, file_type, db_engine, table_name, test_case_name):
    try:
        if file_type == 'csv':
            df_expected = pd.read_csv(file_path)
        elif file_type == 'json':
            df_expected = pd.read_json(file_path)
        elif file_type == 'xml':
            df_expected = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed{file_path}")
        logger.info(f"Expected data in the file is {df_expected}")
        query_actual = f"select * from {table_name}"
        df_actual = pd.read_sql(query_actual, db_engine)
        logger.info(f"Actual data in the file is {df_actual}")

        # expected minus actual data (  extra data in expected )
        df_extra = df_expected[~df_expected.apply(tuple, axis=1).isin(df_actual.apply(tuple, axis=1))]
        df_extra.to_csv(f"Differences/extra_rows_in_expected_{test_case_name}.csv", index=False)

        # actual data minus expected (  extra data in actual )
        df_missing = df_actual[~df_actual.apply(tuple, axis=1).isin(df_expected.apply(tuple, axis=1))]
        df_missing.to_csv(f"Differences/extra_rows_in_actual_{test_case_name}.csv", index=False)

        assert df_extra.empty, (
            f"{test_case_name} : extra rows found in {df_extra} \n"
            f"check Differences/extra_rows_in_expected{test_case_name}.csv"
        )

        assert df_missing.empty, (
            f"{test_case_name} : extra rows found in {df_missing} \n"
            f"check Differences/extra_rows_in_actual_{test_case_name}.csv"
        )
    except Exception as e:
        logger.error(f"there is exception raised while check{e}")
        pytest.fail()


#parameterizing the databases
def verify_expected_as_database_to_actual_as_database(db_engine_expected,query_expected,db_engine_actual,query_actual,test_case_name):
    try:

        df_expected=pd.read_sql(query_expected,db_engine_expected)
        logger.info(f"expected data in the database is {df_expected}")
        df_actual=pd.read_sql(query_actual,db_engine_actual)
        logger.info(f"actual data in the database is {df_actual}")

        # expected minus actual data (  extra data in expected )
        df_extra = df_expected[~df_expected.apply(tuple, axis=1).isin(df_actual.apply(tuple, axis=1))]
        df_extra.to_csv(f"Differences/extra_rows_in_expected_{test_case_name}.csv", index=False)

        # actual data minus expected (  extra data in actual )
        df_missing = df_actual[~df_actual.apply(tuple, axis=1).isin(df_expected.apply(tuple, axis=1))]
        df_missing.to_csv(f"Differences/extra_rows_in_actual_{test_case_name}.csv", index=False)

        assert df_extra.empty, (
            f"{test_case_name} : extra rows found in {df_extra} \n"
            f"check Differences/extra_rows_in_expected{test_case_name}.csv"
        )

        assert df_missing.empty, (
            f"{test_case_name} : extra rows found in {df_missing} \n"
            f"check Differences/extra_rows_in_actual_{test_case_name}.csv"
        )

    except Exception as e:
        logger.error(f"there is exception raised while check{e}")
        pytest.fail()


#parameterising the duplicate check
def check_for_duplicates_across_all_the_columns(file_path, file_type):
    try:
        if file_type == 'csv':
            df_data = pd.read_csv(file_path)
        elif file_type == 'json':
            df_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed{file_path}")
        # logger.info(f"data in the file is {df_data}")

        if df_data.duplicated().any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while reading the file {file_path}")
        pytest.fail()

#parameterising for the specific column for a duplicate check
def check_for_duplicates_for_the_specific_column(file_path, file_type, column_name):
    try:
        # Read file based on type
        if file_type == 'csv':
            df_data = pd.read_csv(file_path)
        elif file_type == 'json':
            df_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Check if the column exists
        if column_name not in df_data.columns:
            raise ValueError(f"Column '{column_name}' not found in file: {file_path}")

        # ~ duplicated() gives True for unique rows, False for duplicates
        unique_mask = ~df_data[column_name].duplicated()
        # If ALL values are True → no duplicates
        # If ANY value is False → duplicates exist
        return unique_mask.all()

    except Exception as e:
        logger.error(f"Error while processing file {file_path}: {e}")
        return False


#parameterising for the specific set of columns(composite primary)in a file for a duplicate check
def check_for_duplicates_for_the_specific_columns_composite_primary(file_path, file_type, column_list):
    try:
        # Read file based on type
        if file_type == 'csv':
            df_data = pd.read_csv(file_path)
        elif file_type == 'json':
            df_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Check if all the column exists
        for col in column_list:
            if col not in df_data.columns:
                raise ValueError(f"Column '{col}' not found in file: {file_path}")

        # Check duplicates based on composite key
        duplicate_found = df_data.duplicated(subset=column_list).any()

        # Return True if NO duplicates, False if duplicates exist
        return not duplicate_found

    except Exception as e:
        logger.error(f"Error while processing file {file_path}: {e}")
        return False

#test for checking duplicate rows in a table

def check_duplicates_on_all_target_tables_across_columns(db_engine, query_execution):
    try:
        # Execute SQL and load into pandas DataFrame
        df_data = pd.read_sql(query_execution, db_engine)

        # Check for duplicates across all columns
        if df_data.duplicated().any():
            return False     # duplicates found
        else:
            return True      # no duplicates

    except Exception as e:
        logger.error(f"Error while executing query '{query_execution}': {e}")
        return False


# test for checking duplicate in a specific column in a table

def check_duplicates_on_all_target_tables_on_a_specific_columns(db_engine, query_execution, subset_columns):
    try:
        df_data = pd.read_sql(query_execution, db_engine)

        # Check duplicates on specific columns
        has_duplicates = df_data.duplicated(subset=subset_columns).any()

        return not has_duplicates  # False if duplicates exist, True if not

    except Exception as e:
        logger.error(f"Error while executing query '{query_execution}': {e}")
        return False


## parameterizing to check for null values in a source files

def check_for_null_values_in_a_file(file_path, file_type,test_case_name):
    try:
        # Treat empty strings and spaces as null
        na_vals = ["", " ", "  "]
        if file_type == 'csv':
            df_data = pd.read_csv(file_path,na_values=na_vals)

        elif file_type == 'json':
            df_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed{file_path}")
        logger.info(f"data in the file is {df_data}")

        if df_data.isnull().values.any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while reading the file {file_path}")
        pytest.fail()

# parameterizing for null values in the target tables
def check_for_null_values_in_target_tables(db_engine,query_execution,table_name,test_case_name):
    try:
        logger.info(f"checking for null values in table '{table_name}'")
        logger.info(f"test_case execution for '{test_case_name}' started")
        df_data = pd.read_sql(query_execution,db_engine)
        #check for nulls
        if df_data.isnull().values.any():
            return False
        else:
            return True

    except Exception as e:
        logger.error(f"Error while executing query '{query_execution}': {e}")

#Check for file existence

def check_file_existence(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file :{file_path} does not exist {e}")

#Check for file size

def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) !=0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file :{file_path} is zero byte file {e}")


# Check for Schema Validations

def check_schema(db_engine, table_name, expected_schema):
    try:
        # Read only one row
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", db_engine)

        # ---- SIMPLE DATE COLUMN HANDLING ----
        # Manually list your date columns here
        date_columns = ["sale_date","last_updated"]

        # Convert each date column into datetime
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="raise")

        # ---- SCHEMA CHECK ----
        for col, expected_type in expected_schema.items():

            if col not in df.columns:
                return False, f"Missing column: {col}"

            actual_type = str(df[col].dtype)

            if expected_type not in actual_type:
                return False, f"Column {col} has wrong type. Expected {expected_type}, got {actual_type}"

        return True, "Schema is correct"

    except Exception as e:
        return False, f"Error checking schema: {e}"

# check for table existence in the database

def database_tables_exist(database_engine, expected_tables_list, database_name, db_type):

# -----------------------------
# Choose query based on DB type
# -----------------------------
 if db_type == "mysql":
    query = f"""
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE table_schema = '{database_name}'
    """
 elif db_type == "oracle":
    query = f"""
        SELECT TABLE_NAME
        FROM ALL_TABLES
        WHERE OWNER = '{database_name.upper()}'
    """
 else:
     raise ValueError("db_type must be 'mysql' or 'oracle'")

# -----------------------------
# Run query
# -----------------------------
 df = pd.read_sql(query, database_engine)

# Normalize all column names to uppercase
 df.columns = df.columns.str.upper()

# Oracle returns uppercase table names
 actual_tables_list = df["TABLE_NAME"].str.upper().tolist()

 logger.info(f"Actual tables: {actual_tables_list}")

# -----------------------------
# Find missing tables (simplified)
# -----------------------------
 expected = set(t.upper() for t in expected_tables_list)
 actual = set(actual_tables_list)

 missing_tables_list = list(expected - actual)

 return missing_tables_list


# S3 conectivity


# initialize the connection
s3 = boto3.client("s3")
def read_file_from_s3(bucket_name,file_key):
    # fetch the csv file from S3
    try:
        response = s3.get_object(Bucket=bucket_name,Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        data = StringIO(csv_content)
        df = pd.read_csv(data)
        return df
    except Exception as e:
        logger.error(f"exception raised while reading from S3 {e}", exc_info=True)

def verify_expected_as_S3_to_actual_as_db(bucket_name_expected,file_key_expected,db_engine_actual,query_actual):
    df_expected = read_file_from_s3(bucket_name_expected,file_key_expected)
    df_actual = pd.read_sql(query_actual,db_engine_actual)
    assert df_actual.equals(df_expected),f"expected data {df_expected} doesn not match with actual data{df_actual}"

#parameterizing the referential integrity
def check_referential_integrity(source_conn, target_conn, source_query, target_query, key_column, csv_path):
    try:
        logger.info(f"Running source query: {source_query}")
        df_source = pd.read_sql(source_query, source_conn)
        logger.info(f"Running target query: {target_query}")
        df_target = pd.read_sql(target_query, target_conn)
        logger.info(f"Comparing key column: {key_column}")
        df_not_matched = df_target[~df_target[key_column].isin(df_source[key_column])]
        df_not_matched.to_csv(csv_path, index=False)
        return df_not_matched
    except Exception as e:
        logger.error(f"Error during referential integerity check {e}")

    #2 . test case for checking referential integrity between a source file and a target database

def check_for_referential_integrity_between_source_file_and_target_database(file_path, file_type,target_conn,target_query,key_column,csv_path):
    try:
        logger.info(f"Running source file: {file_path}")

        if file_type == 'csv':
            df_source_file_data = pd.read_csv(file_path)

        elif file_type == 'json':
            df_source_file_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_source_file_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed{file_path}")
        logger.info(f"data in the file is {df_source_file_data}")
        logger.info(f"Running target query: {target_query}")
        df_target = pd.read_sql(target_query,target_conn)
        logger.info(f"Comparing key column: {key_column}")
        df_not_matched = df_target[~df_target[key_column].isin(df_source_file_data[key_column])]
        df_not_matched.to_csv(csv_path, index=False)
        return df_not_matched

    except Exception as e:
        logger.error(f"Error during referential integerity check {e}")




