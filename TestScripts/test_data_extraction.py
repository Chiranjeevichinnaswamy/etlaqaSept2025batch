#before Parameterization
'''
import paramiko
from Configuration.etlconfig import *
import logging
import pytest
from sqlalchemy import create_engine
import pandas as pd


# Logging configuration
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


class TestDataExtraction:
    #1.test case for sales data
    def test_DE_from_sales_data_between_source_and_staging(self,connect_to_mysql_database):
        try:
            logger.info('test case execution for sales data extraction started')
            df_expected=pd.read_csv('TestData/sales_data_linux.csv')
            query="""select * from staging_sales"""
            df_actual=pd.read_sql(query,connect_to_mysql_database)
            assert df_actual.equals(df_expected),'data extraction between source and staging did not happen properly'
            logger.info('test case execution for sales data extraction finished')
        except Exception as e:
            logger.error(f'test case execution for sales data extraction failed: {e}')

    # 2.test case for product data
    def test_DE_from_product_data_between_source_and_staging(self,connect_to_mysql_database):
        try:
            logger.info('test case execution for product data extraction started')
            df_expected=pd.read_csv('TestData/product_data.csv')
            query="""select * from staging_product"""
            df_actual=pd.read_sql(query,connect_to_mysql_database)
            assert df_actual.equals(df_expected),'data extraction between source and staging did not happen properly'
            logger.info('test case execution for product data extraction finished')
        except Exception as e:
            logger.error(f'test case execution for product data extraction failed: {e}')

    # 3.test case for inventory data
    def test_DE_from_inventory_data_between_source_and_staging(self,connect_to_mysql_database):
        try:
            logger.info('test case execution for inventory data extraction started')
            df_expected=pd.read_xml('TestData/inventory_data.xml',xpath='.//item')
            query="""select * from staging_inventory"""
            df_actual=pd.read_sql(query,connect_to_mysql_database)
            assert df_actual.equals(df_expected),'data extraction between source and staging did not happen properly'
            logger.info('test case execution for inventory data extraction finished')
        except Exception as e:
            logger.error(f'test case execution for inventory data extraction failed: {e}')

    # 4.test case for supplier data
    def test_DE_from_supplier_data_between_source_and_staging(self, connect_to_mysql_database):
        try:
            logger.info('test case execution for supplier data extraction started')
            df_expected = pd.read_json('TestData/supplier_data.json')
            query = """select * from staging_supplier"""
            df_actual = pd.read_sql(query, connect_to_mysql_database)
            assert df_actual.equals(df_expected), 'data extraction between source and staging did not happen properly'
            logger.info('test case execution for supplier data extraction finished')
        except Exception as e:
            logger.error(f'test case execution for supplier data extraction failed: {e}')

    # 5.test case for stores data
    def test_DE_from_stores_data_between_source_and_staging(self,connect_to_mysql_database,connect_to_oracle_database):
        try:
            logger.info('test case execution for stores data extraction started')
            query="""select * from stores"""
            df_expected = pd.read_sql(query,connect_to_oracle_database)
            query = """select * from staging_stores"""
            df_actual = pd.read_sql(query, connect_to_mysql_database)
            assert df_actual.equals(
            df_expected), 'data extraction between source and staging did not happen properly'
            logger.info('test case execution for stores data extraction finished')
        except Exception as e:
            logger.error(f'test case execution for stores data extraction failed: {e}')

'''

# after parameterizing file_path,filetype,table_name etc inside utilities.py

import paramiko

from CommonUtilities.utilities import verify_expected_as_file_to_actual_as_database, \
    verify_expected_as_database_to_actual_as_database, \
    sales_data_from_linux_server, \
    verify_expected_as_S3_to_actual_as_db, \
     read_file_from_s3

from Configuration.etlconfig import *
import logging
import pytest
from sqlalchemy import create_engine
import pandas as pd
import inspect


# Logging configuration
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)

#file_path,filetype,db_engine,tablename
@pytest.mark.usefixtures("connect_to_mysql_database")#calling the fixture here to use it in the class level(so you dont need to call it everytime)
class TestDataExtraction:
    # 1.test case for sales data
    @pytest.mark.DataExtraction
    def test_DE_from_sales_data_between_source_and_staging(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name

            logger.info(f"test case {test_case_name} execution for sales data extraction for sales data has started...")
            sales_data_from_linux_server()
            verify_expected_as_file_to_actual_as_database('TestData/sales_data.csv','csv',connect_to_mysql_database,"staging_sales",test_case_name=test_case_name)
            logger.info(f"test case {test_case_name} execution for sales data extraction for sales data has completed...")
        except Exception as e:
            logger.error(f"test case {test_case_name} execution for sales data extraction failed: {e}")
            pytest.fail(f"Test case {test_case_name} execution for sales data extraction has failed")

    # 2.test case for product data
    @pytest.mark.skip
    @pytest.mark.DataExtraction
    def test_DE_from_product_data_between_source_and_staging(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"Test case '{test_case_name}' execution has started...")

            db_engine_actual = connect_to_mysql_database
            query_actual = """select * from staging_product"""

            bucket_name_expected="bucket-capstone-sep2025"
            file_key_expected= "TestData/product_data.csv"
            verify_expected_as_S3_to_actual_as_db(bucket_name_expected, file_key_expected, db_engine_actual,
            query_actual)

            logger.info(f"Test case '{test_case_name}' execution has completed...")

        except Exception as e:
            logger.error(f"Test case '{test_case_name}' execution has failed: {e}")
            pytest.fail(f"Test case '{test_case_name}' failed")

    # 3.test case for inventory data
    @pytest.mark.DataExtraction
    def test_DE_from_inventory_data_between_source_and_staging(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name

            logger.info(f"test case {test_case_name} execution for inventory data extraction has started...")
            verify_expected_as_file_to_actual_as_database("TestData/inventory_data.xml","xml",connect_to_mysql_database,"staging_inventory",test_case_name=test_case_name)
            logger.info(f"test case {test_case_name} execution for inventory data extraction has completed...")
        except Exception as e:
            logger.error(f'test case {test_case_name} execution for inventory data extraction failed: {e}')
            pytest.fail(f"Test case {test_case_name} execution for inventory data extraction has failed")

    # 4.test case for supplier data
    @pytest.mark.DataExtraction
    def test_DE_from_supplier_data_between_source_and_staging(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name

            logger.info(f"test case {test_case_name} execution for supplier data extraction has started...")
            verify_expected_as_file_to_actual_as_database("TestData/supplier_data.json","json",connect_to_mysql_database,"staging_supplier",test_case_name=test_case_name)
            logger.info(f"test case {test_case_name} execution for supplier data extraction has completed...")
        except Exception as e:
            logger.error(f'test case {test_case_name} execution for supplier data extraction failed: {e}')
            pytest.fail(f"Test case {test_case_name} execution for supplier data extraction has failed")

    # 5.test case for stores data
    @pytest.mark.DataExtraction
    def test_DE_from_stores_data_between_source_and_staging(self, connect_to_mysql_database,connect_to_oracle_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"test case {test_case_name}execution for stores data extraction has started...")
            query_expected="""select * from stores"""
            query_actual="""select * from staging_stores"""
            verify_expected_as_database_to_actual_as_database(connect_to_oracle_database,query_expected,connect_to_mysql_database,query_actual,test_case_name=test_case_name)
            logger.info(f"test case {test_case_name} execution for stores data extraction has completed...")
        except Exception as e:
            logger.error(f'test case {test_case_name} execution for stores data extraction failed: {e}')
            pytest.fail(f"Test case {test_case_name} execution for store data extraction has failed")



