import os
import csv
from distutils import dir_util
import pytest
import pandas as pd
from sqlalchemy import orm
from sqlalchemy import create_engine

from mindsdb_sql.executioner.connection import SQLConnection


@pytest.fixture
def connection_db1():
    params = {
        'db_url': os.environ.get('DB_URL_1', 'mysql+pymysql://user:pwd@test_db1/test?charset=utf8mb4'),
        'db_echo': int(os.environ.get('DB_ECHO', '0') == '1'),
    }
    con = SQLConnection(params)
    transaction = con.connection.begin()
    yield con
    transaction.rollback()
    con.connection.close()


@pytest.fixture
def connection_db2():
    params = {
        'db_url': os.environ.get('DB_URL_2', 'mysql+pymysql://user:pwd@test_db2/test?charset=utf8mb4'),
        'db_echo': int(os.environ.get('DB_ECHO', '0') == '1'),
    }
    con = SQLConnection(params)
    transaction = con.connection.begin()
    yield con
    transaction.rollback()
    con.connection.close()


@pytest.fixture
def datadir(tmpdir, request):
    '''
    Fixture responsible for searching a folder with the same name of test
    module and, if available, moving all contents to a temporary directory so
    tests can use them freely.
    '''
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    if os.path.isdir(test_dir):
        dir_util.copy_tree(test_dir, str(tmpdir))

    return tmpdir


@pytest.fixture
def default_data_db1(connection_db1, datadir):
    googleplaystore_file = datadir.join('googleplaystore.csv')
    googleplaystore_fpath = str(googleplaystore_file)
    n_rows = 1000
    create_sql = """
        CREATE TABLE IF NOT EXISTS googleplaystore (
            App VARCHAR(300) NULL, 
            Category VARCHAR(100) NULL,
            Rating FLOAT NULL,
            Reviews VARCHAR(100) NULL,
            Size Varchar(100) NULL,
            Installs Varchar(100) NULL,
            Type Varchar(100) NULL,
            Price Varchar(100) NULL,
            `Content Rating` Varchar(100) NULL,
            Genres Varchar(100) NULL,
            LastUpdated Varchar(100) NULL,
            `Current Ver` Varchar(100) NULL,
            `Android Ver` Varchar(100) NULL
        );  
    """
    insert_sql = "INSERT INTO googleplaystore VALUES"

    df = pd.read_csv(googleplaystore_fpath)
    df = df.replace('nan', None)
    df = df.replace('NaN', None)
    df = df.convert_dtypes()
    df = df.iloc[:n_rows]
    for row in df.itertuples(index=False):
        repr_list = [(repr(x) if not pd.isnull(x) else 'NULL') for x in row]
        sql_str = f"({', '.join(repr_list)}),"
        insert_sql += '\n' + sql_str
    insert_sql = insert_sql.strip(',') + ';'

    connection_db1.query_raw('DROP TABLE IF EXISTS googleplaystore;')
    connection_db1.query_raw(create_sql)
    connection_db1.query_raw(insert_sql)


@pytest.fixture
def default_data_db2(connection_db2, datadir):
    googleplaystore_user_reviews_file = datadir.join('googleplaystore_user_reviews.csv')
    googleplaystore_user_reviews_fpath = str(googleplaystore_user_reviews_file)
    n_rows = 1000
    create_sql = """
        CREATE TABLE IF NOT EXISTS googleplaystore_user_reviews (
            App VARCHAR(300) NULL, 
            `Translated_Review` MEDIUMTEXT NULL,
            Sentiment VARCHAR(100) NULL,
            `Sentiment_Polarity` FLOAT NULL,
            `Sentiment_Subjectivity` FLOAT NULL
        );  
    """
    insert_sql = "INSERT INTO googleplaystore_user_reviews VALUES"

    df = pd.read_csv(googleplaystore_user_reviews_fpath)
    df = df.replace('nan', None)
    df = df.replace('NaN', None)
    df = df.dropna()
    df = df.convert_dtypes()
    df = df.iloc[:n_rows]
    for row in df.itertuples(index=False):
        repr_list = [(repr(x) if not pd.isnull(x) else 'NULL') for x in row]
        sql_str = f"({', '.join(repr_list)}),"
        insert_sql += '\n' + sql_str
    insert_sql = insert_sql.strip(',') + ';'

    insert_sql = insert_sql.replace('%', '%%')
    create_sql = create_sql.replace('%', '%%')
    connection_db2.query_raw('DROP TABLE IF EXISTS googleplaystore_user_reviews;')
    connection_db2.query_raw(create_sql)
    connection_db2.query_raw(insert_sql)



