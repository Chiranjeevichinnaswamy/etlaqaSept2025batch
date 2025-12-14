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
import inspect

# after parameterizing file_path,filetype,table_name etc inside utilities.py

import paramiko

from CommonUtilities.utilities import verify_expected_as_file_to_actual_as_database,\
    verify_expected_as_database_to_actual_as_database,\
    check_for_duplicates_across_all_the_columns, \
    check_for_duplicates_for_the_specific_column, \
    check_for_duplicates_for_the_specific_columns_composite_primary, \
    check_duplicates_on_all_target_tables_across_columns, \
    check_duplicates_on_all_target_tables_on_a_specific_columns, \
    check_for_null_values_in_a_file, \
    check_for_null_values_in_target_tables, \
    check_file_existence, \
    check_file_size, \
    check_schema, \
    database_tables_exist, \
    check_referential_integrity, \
    check_for_referential_integrity_between_source_file_and_target_database
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

#file_path,filetype,db_engine,tablename
class TestDataQuality:
    # 1.test case for Data Quality check on salesdata.csv
    @pytest.mark.skip
    def test_DQ_Sales_csv_duplicate_check(self):
        try:
            logger.info("Duplicate check for sales_data has started...")
            duplicate_status = check_for_duplicates_across_all_the_columns("TestData/sales_data_linux.csv","csv")
            print(duplicate_status)
            assert duplicate_status," there are duplicate in the sales data file"
            logger.info("Duplicate check for sales_data has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check for sales_data has failed...,{e}")
            pytest.fail()



    # 2.test case for Data Quality check on product_data.csv
    @pytest.mark.skip
    def test_DQ_Product_csv_duplicate_check(self):
        try:
            logger.info("Duplicate check for product_data has started...")
            duplicate_status = check_for_duplicates_across_all_the_columns("TestData/product_data.csv", "csv")
            print(duplicate_status)
            assert duplicate_status, " there are duplicate in the sales data file"
            logger.info("Duplicate check for sales_data has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check for sales_data has failed...,{e}")
            pytest.fail()


    # 3.test case for Data Quality check on inventory_data.xml
    @pytest.mark.skip
    def test_DQ_inventory_data_xml_duplicate_check(self):
        try:
            logger.info("Duplicate check for product_data has started...")
            duplicate_status = check_for_duplicates_across_all_the_columns("TestData/inventory_data.xml", "xml")
            print(duplicate_status)
            assert duplicate_status, " there are duplicate in the sales data file"
            logger.info("Duplicate check for sales_data has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check for sales_data has failed...,{e}")
            pytest.fail()


    # 4.test case for Data Quality check on supplier_data_json
    @pytest.mark.skip
    def test_DQ_TestData_supplier_data_json_duplicate_check(self):
        try:
            logger.info("Duplicate check for product_data has started...")
            duplicate_status = check_for_duplicates_across_all_the_columns("TestData/supplier_data.json", "json")
            print(duplicate_status)
            assert duplicate_status, " there are duplicate in the sales data file"
            logger.info("Duplicate check for sales_data has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check for sales_data has failed...,{e}")
            pytest.fail()

    # 5.test case for Data Quality check on stores data

    #test cases for checking files for a speciifc columns on
    # 1.test case for checking for a speciifc columns on salesdata.csv file
    @pytest.mark.skip
    def test_DQ_TestData_sales_data_linux_duplicate_check(self):
        try:
            logger.info("Duplicate check for sales_data_linux has started...")

            duplicate_status = check_for_duplicates_for_the_specific_column(
                "TestData/sales_data_linux.csv",
                "csv",
                "sales_id"
            )

            print("Duplicate Status:", duplicate_status)

            # Assert passes only if NO duplicates → duplicate_status = True
            assert duplicate_status, "There are duplicates in sales_data file"

            logger.info("Duplicate check for sales_data_linux has completed...")

        except Exception as e:
            logger.error(f"Duplicate check for sales_data_linux has failed: {e}")
            pytest.fail()

    # 2.test case for checking for a speciifc columns on product_data.csv file
    @pytest.mark.skip
    def test_DQ_TestData_product_data_csv_duplicate_check(self):
        try:
            logger.info("Duplicate check for product_data.csv has started...")

            duplicate_status = check_for_duplicates_for_the_specific_column(
                "TestData/product_data.csv",
                "csv",
                "product_id"
            )

            print("Duplicate Status:", duplicate_status)

            # Assert passes only if NO duplicates → duplicate_status = True
            assert duplicate_status, "There are duplicates in product_data file"

            logger.info("Duplicate check for product_data has completed...")

        except Exception as e:
            logger.error(f"Duplicate check for product_data has failed: {e}")
            pytest.fail()

# 3.test case for checking for a speciifc columns on inventory_data file
    @pytest.mark.skip
    def test_inventory_data_composite_key_duplicate_check(self):
        try:
            logger.info("Duplicate check for inventory_data started...")

            duplicate_status = check_for_duplicates_for_the_specific_columns_composite_primary(
                "TestData/inventory_data.xml",
                "xml",
                ["product_id", "store_id"]
            )

            assert duplicate_status, "Duplicates found in product_id + store_id combination"
            logger.info("Duplicate check for inventory_data completed successfully.")

        except Exception as e:
            logger.error(f"Duplicate check for inventory_data failed: {e}")
            pytest.fail()



# 4.test case for checking for a speciifc columns on supplier_data.json file
    @pytest.mark.skip
    def test_DQ_TestData_supplier_data_json_duplicate_check(self):
        try:
            logger.info("Duplicate check for supplier_data.json has started...")

            duplicate_status = check_for_duplicates_for_the_specific_column(
                "TestData/supplier_data.json",
                "json",
                "supplier_id"
            )

            print("Duplicate Status:", duplicate_status)

            # Assert passes only if NO duplicates → duplicate_status = True
            assert duplicate_status, "There are duplicates in supplier_data file"

            logger.info("Duplicate check for supplier_data has completed...")

        except Exception as e:
            logger.error(f"Duplicate check for supplier_data has failed: {e}")
            pytest.fail()


    # Checking for duplicate records in a target tables

    #1. Test for duplicates in the monthly_sales_summary in  target table
    @pytest.mark.skip
    def test_duplicates_check_monthly_sales_summary_target_table_across_columns(self,connect_to_mysql_database):
        try:
            db_engine = connect_to_mysql_database
            query_execution="select * from monthly_sales_summary"
            logger.info("Duplicate check for monthly_sales_summary target table has started...")
            duplicate_status= check_duplicates_on_all_target_tables_across_columns(db_engine,query_execution)
            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, "There are duplicates in monthly_sales_summary target table"
            logger.info("Duplicate check for monthly_sales_summary target table has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check has failed for the monthly_sales_summary tables: {e}")
            pytest.fail()


    # 2. Test for duplicates in the fact_sales in  target table
    @pytest.mark.skip
    def test_duplicates_check_fact_sales_target_table_across_columns(self, connect_to_mysql_database):
        try:
            db_engine = connect_to_mysql_database
            query_execution = "select * from fact_sales"
            logger.info("Duplicate check for fact_sales target table has started...")
            duplicate_status = check_duplicates_on_all_target_tables_across_columns(db_engine, query_execution)
            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, "There are duplicates in fact_sales target table"
            logger.info("Duplicate check for fact_sales target table has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check has failed for the fact_sales tables: {e}")
            pytest.fail()

    # 3. Test for duplicates in the fact_inventory in  target table
    @pytest.mark.skip
    def test_duplicates_check_fact_inventory_target_table_across_columns(self, connect_to_mysql_database):
        try:
            db_engine = connect_to_mysql_database
            query_execution = "select * from  fact_inventory"
            logger.info("Duplicate check for fact_inventory target table has started...")
            duplicate_status = check_duplicates_on_all_target_tables_across_columns(db_engine, query_execution)
            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, "There are duplicates in fact_inventory target table"
            logger.info("Duplicate check for fact_inventory target table has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check has failed for the fact_inventory tables: {e}")
            pytest.fail()

    # 4. Test for duplicates in the inventory_level_by_stores in  target table
    @pytest.mark.skip
    def test_duplicates_check_fact_inventory_levels_by_store_table_across_columns(self, connect_to_mysql_database):
        try:
            db_engine = connect_to_mysql_database
            query_execution = "select * from  inventory_levels_by_store"
            logger.info("Duplicate check for inventory_levels_by_store target table has started...")
            duplicate_status = check_duplicates_on_all_target_tables_across_columns(db_engine, query_execution)
            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, "There are duplicates in inventory_levels_by_store target table"
            logger.info("Duplicate check for inventory_levels_by_store target table has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check has failed for the inventory_levels_by_store tables: {e}")
            pytest.fail()

   # Test cases for checking duplicates in a specific columns present in a target table in the database
    # 1. test case for checking for duplicates in the monthly_sales_summary table
    @pytest.mark.skip
    def test_duplicates_check_monthly_sales_summary_for_specific_columns(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            db_engine = connect_to_mysql_database
            query_execution = """
                   SELECT product_id, store_id, year, month, total_monthly_sales
                   FROM monthly_sales_summary
               """

            logger.info(f"Duplicate check for {test_case_name} target table has started...")

            duplicate_status = check_duplicates_on_all_target_tables_on_a_specific_columns(
                db_engine,
                query_execution,
                subset_columns=["product_id", "store_id", "year", "month"]
            )

            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, f"There are duplicates in {test_case_name} target table"

            logger.info(f"Duplicate check for {test_case_name} target table has completed ...")

        except Exception as e:
            logger.error(f"Duplicate check has failed for {test_case_name}: {e}")
            pytest.fail()


    # 2. test case for checking for duplicates in the fact_sales table

    @pytest.mark.skip
    def test_duplicates_check_fact_sales_for_specific_columns(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            db_engine = connect_to_mysql_database
            query_execution = """
                      select sales_id, product_id, store_id, quantity, total_sales, sale_date
                      from fact_sales
                  """

            logger.info(f"Duplicate check for {test_case_name} target table has started...")

            duplicate_status = check_duplicates_on_all_target_tables_on_a_specific_columns(
                db_engine,
                query_execution,
                subset_columns=["sales_id", "product_id", "store_id"]
            )

            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, f"There are duplicates in {test_case_name} target table"

            logger.info(f"Duplicate check for {test_case_name} target table has completed ...")

        except Exception as e:
            logger.error(f"Duplicate check has failed for {test_case_name}: {e}")
            pytest.fail()

    #3. test case for checking for duplicates in the fact_inventory table
    @pytest.mark.skip
    def test_duplicates_check_fact_inventory_for_specific_columns(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            db_engine = connect_to_mysql_database
            query_execution = """
                      select product_id, store_id, quantity_on_hand, last_updated from fact_inventory
                  """

            logger.info(f"Duplicate check for {test_case_name} target table has started...")

            duplicate_status = check_duplicates_on_all_target_tables_on_a_specific_columns(
                db_engine,
                query_execution,
                subset_columns=["product_id", "store_id"]
            )

            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, f"There are duplicates in {test_case_name} target table"

            logger.info(f"Duplicate check for {test_case_name} target table has completed ...")

        except Exception as e:
            logger.error(f"Duplicate check has failed for {test_case_name}: {e}")
            pytest.fail()

    #4. test case for checking for duplicates in the inventory_levels_by_store table
    @pytest.mark.skip
    def test_duplicates_check_inventory_levels_by_store_for_specific_columns(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            db_engine = connect_to_mysql_database
            query_execution = """
                      select * from inventory_levels_by_store
                  """

            logger.info(f"Duplicate check for {test_case_name} target table has started...")

            duplicate_status = check_duplicates_on_all_target_tables_on_a_specific_columns(
                db_engine,
                query_execution,
                subset_columns=["store_id"]
            )

            print("Duplicate Status:", duplicate_status)
            assert duplicate_status, f"There are duplicates in {test_case_name} target table"

            logger.info(f"Duplicate check for {test_case_name} target table has completed ...")

        except Exception as e:
            logger.error(f"Duplicate check has failed for {test_case_name}: {e}")
            pytest.fail()

    # Test cases for null checks in a source files

    #1. Test cases for null checks in sales_data_linux.csv file
    @pytest.mark.skip
    def test_null_values_in_sales_data_linux_source_files(self):
        try:

            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"Null Status for {test_case_name} source file has started...")
            null_status=check_for_null_values_in_a_file("TestData/sales_data_linux.csv","csv",test_case_name)

            print("Null Status:", null_status)
            assert null_status,f"there are null values present in {test_case_name}"

            logger.info(f"Null check for {test_case_name} source file  has completed successfully...")
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

    # 2. Test cases for null checks in product_data.csv file
    @pytest.mark.skip
    def test_null_values_in_product_data_csv_source_files(self):
        try:

            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"Null Status for {test_case_name} source file has started...")
            null_status=check_for_null_values_in_a_file("TestData/product_data.csv","csv",test_case_name)

            print("Null Status:", null_status)
            assert null_status,f"there are null values present in {test_case_name}"

            logger.info(f"Null check for {test_case_name} source file  has completed successfully...")
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

    # 3. Test cases for null checks in inventory_data.xml file
    @pytest.mark.skip
    def test_null_values_in_inventory_data_xml_source_files(self):
        try:

            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"Null Status for {test_case_name} source file has started...")
            null_status=check_for_null_values_in_a_file("TestData/inventory_data.xml","xml",test_case_name)

            print("Null Status:", null_status)
            assert null_status,f"there are null values present in {test_case_name}"

            logger.info(f"Null check for {test_case_name} source file  has completed successfully...")
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

    # 4. Test cases for null checks in supplier_data.json file
    @pytest.mark.skip
    def test_null_values_in_supplier_data_json_source_files(self):
        try:

            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"Null Status for {test_case_name} source file has started...")
            null_status=check_for_null_values_in_a_file("TestData/supplier_data.json","json",test_case_name)

            print("Null Status:", null_status)
            assert null_status,f"there are null values present in {test_case_name}"

            logger.info(f"Null check for {test_case_name} source file  has completed successfully...")
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

   # Testcases for null values in the target table

   #1. Test case for null values in monthly_sales_summary in target table
    @pytest.mark.skip
    def test_for_null_values_in_monthly_sales_summary_table(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            table_name = "monthly_sales_summary"
            query_execution = f"select * from {table_name}"
            null_status = check_for_null_values_in_target_tables(connect_to_mysql_database, query_execution, table_name,
                                                                 test_case_name)
            print(f"Null Status:", null_status)
            assert null_status, (
                f"there are null values present in test case {test_case_name}\n"
                f"check for null values in the table {table_name}"
            )
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

   #2. Test case for null values in fact_sales
    @pytest.mark.skip
    def test_for_null_values_in_fact_sales_table(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            table_name = "fact_sales"
            query_execution = f"select * from {table_name}"
            null_status = check_for_null_values_in_target_tables(connect_to_mysql_database, query_execution, table_name,
                                                                 test_case_name)
            print(f"Null Status:", null_status)
            assert null_status, (
                f"there are null values present in test case {test_case_name}\n"
                f"check for null values in the table {table_name}"
            )
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

   #3. Test case for null values in fact_inventory
    @pytest.mark.skip
    def test_for_null_values_in_fact_inventory_table(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            table_name = "fact_inventory"
            query_execution = f"select * from {table_name}"
            null_status = check_for_null_values_in_target_tables(connect_to_mysql_database, query_execution, table_name,
                                                                 test_case_name)
            print(f"Null Status:", null_status)
            assert null_status, (
                f"there are null values present in test case {test_case_name}\n"
                f"check for null values in the table {table_name}"
            )
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

   #4. Test case for null values in inventory_level_by stores
    @pytest.mark.skip
    def test_for_null_values_in_inventory_level_by_stores_table(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            table_name = "inventory_levels_by_store"
            query_execution = f"select * from {table_name}"
            null_status = check_for_null_values_in_target_tables(connect_to_mysql_database, query_execution, table_name,
                                                                 test_case_name)
            print(f"Null Status:", null_status)
            assert null_status, (
                f"there are null values present in test case {test_case_name}\n"
                f"check for null values in the table {table_name}"
            )
        except Exception as e:
            logger.error(f"Null Status has failed for {test_case_name}: {e}")
            pytest.fail()

   # Test cases to check for the file availability

    #1. test case for Sales_csv_file_availabilty
    @pytest.mark.skip
    def test_DQ_Sales_csv_file_availabilty(self):
        try:
            logger.info("file availability check for sales_data has started...")
            assert check_file_existence("TestData/sales_data_linux.csv"), "File doe not exist in location"
            logger.info("file availability check for sales_data has completed...")
        except Exception as e:
            logger.error(f"file availability check for sales_data has failed {e}")
            pytest.fail()

#2. test case for Sales_csv_file_availabilty
    @pytest.mark.skip
    def test_DQ_Product_csv_file_availabilty(self):
        try:
            logger.info("file availability check for Product_data has started...")
            assert check_file_existence("TestData/Product_data.csv"), "File doe not exist in location"
            logger.info("file availability check for Product_data has completed...")
        except Exception as e:
            logger.error(f"file availability check for Product_data has failed {e}")
            pytest.fail()

#3. test case for inventory_data_xml_file_availabilty
    @pytest.mark.skip
    def test_DQ_inventory_data_xml_file_availabilty(self):
        try:
            logger.info("file availability check for inventory_data_xml has started...")
            assert check_file_existence("TestData/inventory_data.xml"), "File doe not exist in location"
            logger.info("file availability check for inventory_data_xml has completed...")
        except Exception as e:
            logger.error(f"file availability check for inventory_data_xml has failed {e}")
            pytest.fail()

#4. test case for supplier_data_json_file_availabilty
    @pytest.mark.skip
    def test_DQ_supplier_data_json_file_availabilty(self):
        try:
            logger.info("file availability check for supplier_data_json has started...")
            assert check_file_existence("TestData/supplier_data.json"), "File doe not exist in location"
            logger.info("file availability check for supplier_data_json has completed...")
        except Exception as e:
            logger.error(f"file availability check for supplier_data_json has failed {e}")
            pytest.fail()

#Test Cases for file size

#1. test case for sales_data_linux_csv file size
    @pytest.mark.skip
    def test_DQ_sales_data_linux_csv_file_availabilty(self):
        try:
            logger.info("file size check for sales_data_linux_csv has started...")
            assert check_file_size("TestData/sales_data_linux.csv"), "File doe not exist in location"
            logger.info("file size check for sales_data_linux_csv has completed...")
        except Exception as e:
            logger.error(f"file size check for sales_data_linux_csv has failed {e}")
            pytest.fail()

#2. test case for product_csv file size
    @pytest.mark.skip
    def test_DQ_product_data_csv_file_availabilty(self):
        try:
            logger.info("file size check for product_data_csv has started...")
            assert check_file_size("TestData/product_data.csv"), "File doe not exist in location"
            logger.info("file size check for product_data_csv has completed...")
        except Exception as e:
            logger.error(f"file size check for product_data_csv has failed {e}")
            pytest.fail()

#3. test case for inventory_data_xml file size
    @pytest.mark.skip
    def test_DQ_inventory_data_xml_file_size(self):
        try:
            logger.info("file size check for inventory_data_xml has started...")
            assert check_file_size("TestData/inventory_data.xml"), "File doe not exist in location"
            logger.info("file size check for inventory_data_xml has completed...")
        except Exception as e:
            logger.error(f"file size check for inventory_data_xml has failed {e}")
            pytest.fail()

#4. test case for supplier_data_json file size
    @pytest.mark.skip
    def test_DQ_supplier_data_json_file_size(self):
        try:
            logger.info("file size check for supplier_data_json has started...")
            assert check_file_size("TestData/supplier_data.json"), "File doe not exist in location"
            logger.info("file size check for supplier_data_json has completed...")
        except Exception as e:
            logger.error(f"file size check for supplier_data_json has failed {e}")
            pytest.fail()

# test for the schema validation in the target tables

#1. test for schema validations on fact_sales table in the target
    @pytest.mark.skip
    def test_schema_check_fact_sales(self, connect_to_mysql_database):
        expected_schema = {
            "total_sales": "float",
            "store_id": "int",
            "sales_id": "int",
            "sale_date": "date",
            "quantity": "int",
            "product_id":"int"
        }

        try:
            status, message = check_schema(
                connect_to_mysql_database,
                "fact_sales",
                expected_schema
            )

            assert status, message

        except Exception as e:
            logger.error(f"Schema test failed: {e}")
            pytest.fail(str(e))

#2. test for schema validations on monthly sales summary table in the target
    @pytest.mark.skip
    def test_schema_check_monthly_sales_summary(self, connect_to_mysql_database):
        expected_schema = {
            "product_id": "int64",
            "store_id": "int64",
            "year": "int64",
            "month": "int64",
            "total_monthly_sales": "float64"
        }

        try:
            status, message = check_schema(
                connect_to_mysql_database,
                "monthly_sales_summary",
                expected_schema
            )

            assert status, message

        except Exception as e:
            logger.error(f"Schema test failed: {e}")
            pytest.fail(str(e))

#3. test for schema validations on fact_inventory table in the target
    @pytest.mark.skip
    def test_schema_check_fact_inventory(self, connect_to_mysql_database):
        expected_schema = {
            "product_id": "int64",
            "store_id": "int64",
            "quantity_on_hand": "int64",
            "last_updated": "date",

        }

        try:
            status, message = check_schema(
                connect_to_mysql_database,
                "fact_inventory",
                expected_schema
            )

            assert status, message

        except Exception as e:
            logger.error(f"Schema test failed: {e}")
            pytest.fail(str(e))


#4. test for schema validations on inventory_levels_by_store table in the target
    @pytest.mark.skip
    def test_schema_check_inventory_levels_by_store(self, connect_to_mysql_database):
        expected_schema = {
            "store_id": "int64",
            "total_inventory": "int64"

        }

        try:
            status, message = check_schema(
                connect_to_mysql_database,
                "inventory_levels_by_store",
                expected_schema
            )

            assert status, message

        except Exception as e:
            logger.error(f"Schema test failed: {e}")
            pytest.fail(str(e))

## Test case to check tables availbility in database

    @pytest.mark.skip
    def test_mysql_tables_exist(self, connect_to_mysql_database):
        expected_table_list = ["fact_inventory", "fact_sales", "monthly_sales_summary", "inventory_levels_by_store"]

        missing_tables_list = database_tables_exist(
            database_engine=connect_to_mysql_database,
            expected_tables_list=expected_table_list,
            database_name="retaildwh",
            db_type = 'mysql'

        )
        assert len(missing_tables_list) == 0, f"Missing tables: {missing_tables_list}"


    @pytest.mark.order(2)#order is used to assign the order of execution of testcases
    def test_oracle_tables_exist(self, connect_to_oracle_database):
        expected_table_list = ["stores"]  # Oracle-only table

        missing_tables_list = database_tables_exist(
            database_engine=connect_to_oracle_database,
            expected_tables_list=expected_table_list,
            database_name="SYSTEM",
            db_type="oracle"
        )

        assert len(missing_tables_list) == 0, f"Missing tables: {missing_tables_list}"

    #test case for refertial integrity
    @pytest.mark.order(1)
    def test_referentialIntegrity_store_id_between_Oracle_stores_and_target_mysql(self, connect_to_oracle_database,
                                                                                  connect_to_mysql_database):
        source_query = """select store_id from stores order by store_id"""
        target_query = """select store_id from fact_sales order by store_id"""
        df_not_matched = check_referential_integrity(
            source_conn=connect_to_oracle_database,
            target_conn=connect_to_mysql_database,
            source_query=source_query,
            target_query=target_query,
            key_column='store_id',
            csv_path="Differences/not_matching_store_data.csv"
        )
        assert df_not_matched.empty, "There are store_id valure in the target that do not exist in the source"


   #test case 2
    pytest.mark.order(3)
    def test_referentialIntegrity_sales_id_between_sales_data_csv_and_target_mysql(self,connect_to_mysql_database):
        target_query = """select sales_id from fact_sales order by sales_id"""
        df_not_matched = check_for_referential_integrity_between_source_file_and_target_database(
            file_path = "TestData/sales_data.csv",
            file_type = 'csv',
            target_conn=connect_to_mysql_database,
            target_query=target_query,
            key_column='sales_id',
            csv_path="Differences/not_matching_sales_data.csv"
        )
        assert df_not_matched.empty, "There are sales_id valure in the target that do not exist in the source"