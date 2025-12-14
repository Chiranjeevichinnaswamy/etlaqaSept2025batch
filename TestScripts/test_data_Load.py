import paramiko

from CommonUtilities.utilities import verify_expected_as_file_to_actual_as_database, \
    verify_expected_as_database_to_actual_as_database
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
class TestDataLoad:
    # 1.test case for monthly_sales_summary


    def test_DL_Monthly_Sales_Summary(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for data loading for monthly_sales_summary has started...")
            query_expected = """select * from intermediate_monthly_sales_summary_source"""
            query_actual = """select * from monthly_sales_summary"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database, query_expected,
                                                              connect_to_mysql_database, query_actual)
            logger.info("test case execution for data loading for fact_monthly_sales_summary has completed...")
        except Exception as e:
            logger.error(f'test case execution for data loading for fact_monthly_sales_summary has failed: {e}')
            pytest.fail("Test case execution for data loading for fact_monthly_sales_summary has failed")

    # 2.test case for Fact inventory


    def test_DL_Fact_Inventory(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for data loading for fact_inventory has started...")
            query_expected = """select CAST(product_id AS signed) AS product_id,CAST(store_id AS SIGNED) AS store_id,CAST(quantity_on_hand AS SIGNED)AS quantity_on_hand,
                CAST(last_updated AS date)AS last_updated from staging_inventory order by product_id,store_id;"""
            query_actual = """select * from  fact_inventory order by product_id,store_id"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database, query_expected,
                                                              connect_to_mysql_database, query_actual)
            logger.info("test case execution for data loading for fact_inventory has completed...")
        except Exception as e:
            logger.error(f'test case execution for data loading for fact_inventory has failed: {e}')
            pytest.fail("Test case execution for data loading for fact_inventory has failed")

    # 3.test case for factsales
    def test_DL_Fact_sales(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for data loading for fact_sales has started...")
            query_expected = """select cast(sales_id as signed) as sales_id,cast(product_id as signed) as product_id,cast(store_id as signed)as store_id,
                    cast(quantity as signed)as quantity,ROUND(cast(sales_amount as decimal(10,2)), 1) as total_sales,cast(sale_date as date)as sale_date from intermediate_sales_with_details"""
            query_actual = """select sales_id, product_id, store_id, quantity,ROUND(total_sales, 1) AS total_sales, sale_date from fact_sales"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database, query_expected,
                                                              connect_to_mysql_database, query_actual)
            logger.info("test case execution for data loading for fact_sales has completed...")
        except Exception as e:
            logger.error(f'test case execution for data loading for fact_sales has failed: {e}')
            pytest.fail("Test case execution for data loading for fact_sales has failed")

    # 4.test case for Inventory_Level_by_stores


    def test_DL_Inventory_Level_by_stores(self, connect_to_mysql_database, ):
        try:
            logger.info("test case execution for data loading for Inventory_Level_by_stores has started...")
            query_expected = """SELECT CAST(store_id AS SIGNED) AS store_id,CAST(total_inventory AS SIGNED) AS total_inventory
                FROM intermediate_aggregated_inventory_level"""
            query_actual = """select * from inventory_levels_by_store"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database, query_expected,
                                                              connect_to_mysql_database, query_actual)
            logger.info("test case execution for data loading for Inventory_Level_by_stores has completed...")
        except Exception as e:
            logger.error(f'test case execution for data loading for Inventory_Level_by_stores has failed: {e}')
            pytest.fail("Test case execution for data loading for Inventory_Level_by_stores has failed")

