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
class TestDataTransformation:
    # 1.test case for sales data
    @pytest.mark.skip
    def test_DT_Filter_Sales(self, connect_to_mysql_database,):
        try:
            logger.info("test case execution for sales data transformation for sales data has started...")
            query_expected="""select * from staging_sales where sale_date>='2024-09-10'"""
            query_actual="""select * from intermediate_filtered_sales"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("test case execution for sales data transformation for sales data has completed...")
        except Exception as e:
            logger.error(f'test case execution for sales data transformation failed: {e}')
            pytest.fail("Test case execution for store data transformation has failed")

    @pytest.mark.skip
    def test_DT_Router_High_Region__Sales(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for Router_High_Region data transformation for sales data has started...")
            query_expected="""select * from intermediate_filtered_sales where region='High'"""
            query_actual="""select * from intermediate_high_sales"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("test case execution for Router_High_Region data transformation for sales data has completed...")
        except Exception as e:
            logger.error(f'test case execution for Router_High_Region data transformation failed: {e}')
            pytest.fail("Test case execution for Router_High_Region data transformation has failed")

    @pytest.mark.skip
    def test_DT_Router_Low_Region__Sales(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for Router_Low_Region data transformation for sales data has started...")
            query_expected="""select * from intermediate_filtered_sales where region='Low'"""
            query_actual="""select * from intermediate_Low_sales"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("test case execution for Router_Low_Region data transformation for sales data has completed...")
        except Exception as e:
            logger.error(f'test case execution for Router_Low_Region data transformation failed: {e}')
            pytest.fail("Test case execution for Router_Low_Region data transformation has failed")


    def test_DT_Aggreg_Monthly_sales_summary(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for Monthly_sales_summary data transformation for sales data has started...")
            query_expected="""select product_id,store_id,year(sale_date) as year,month(sale_date) as month,sum(quantity*price) as total_monthly_sales 
                      from intermediate_filtered_sales 
                       group by product_id,store_id,month(sale_date),year(sale_date)"""
            query_actual="""select * from intermediate_monthly_sales_summary_source"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("test case execution for Monthly_sales_summary data transformation for sales data has completed...")
        except Exception as e:
            logger.error(f'test case execution for Monthly_sales_summary data transformation failed: {e}')
            pytest.fail("Test case execution for Monthly_sales_summary data transformation has failed")

    @pytest.mark.skip
    def test_DT_Joiner_Sales_with_details(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for Joiner_Sales_with_details data transformation for sales data has started...")
            query_expected = """select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price as sales_amount,fs.sale_date,p.product_id,p.product_name,
                        s.store_id,s.store_name
                        from intermediate_filtered_sales as fs 
                        inner join staging_product as p on fs.product_id = p.product_id
                        inner join staging_stores as s on fs.store_id = s.store_id
                        order by fs.sales_id"""
            query_actual = """select * from intermediate_sales_with_details order by sales_id"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("test case execution for Joiner_Sales_with_details data transformation for sales data has completed...")
        except Exception as e:
            logger.error(f'test case execution for Joiner_Sales_with_details data transformation failed: {e}')
            pytest.fail(f"Test case execution for Joiner_Sales_with_details data transformation has failed :{e}")

    @pytest.mark.skip
    def test_DT_Aggreg_Inventory_level(self, connect_to_mysql_database):
        try:
            logger.info("test case execution for Aggreg_Inventory_level data transformation for sales data has started...")
            query_expected="""select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id"""
            query_actual="""select * from intermediate_aggregated_inventory_level"""
            verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
            logger.info("test case execution for Aggreg_Inventory_level data transformation for sales data has completed...")
        except Exception as e:
            logger.error(f'test case execution for Aggreg_Inventory_level data transformation failed: {e}')
            pytest.fail("Test case execution for Aggreg_Inventory_level data transformation has failed")
