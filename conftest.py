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

@pytest.fixture()
def connect_to_oracle_database():
    logger.info("Oracle database connectiuon is being established..")
    oracle_engine = create_engine(
        f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}").connect()
    logger.info("Oracle database connection has been established.")
    yield oracle_engine
    oracle_engine.close()
    logger.info("Oracle database connection has been closed.")

@pytest.fixture(scope="class") #making this fixture to work ih the class level in other test scripts(eg:test_data_extraction.py)
def connect_to_mysql_database():
    logger.info("mysql database connection is being established..")
    mysql_engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}").connect()
    logger.info("mysql database connection has been established.")
    yield mysql_engine
    mysql_engine.close()
    logger.info("mysql database connection has been closed.")