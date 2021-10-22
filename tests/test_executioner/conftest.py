import os
import csv
from distutils import dir_util
import pytest
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
    n_rows = 100
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

    with open(googleplaystore_fpath, 'r') as read_obj:
        csv_reader = csv.reader(read_obj, delimiter=',')
        for i, row in enumerate(csv_reader):
            if i == 0:
                continue
            if i > n_rows:
                break
            row_list = list(row)
            row_list[2] = float(row_list[2]) if row_list[2] and row_list[2] != 'NaN' else None
            for j, item in enumerate(row_list):
                if row_list[j] is not None and isinstance(row_list[j], str):
                    if row_list[j] == 'NaN' or not row_list[j]:
                        row_list[j] = None

            repr_list = [(repr(x) if x is not None else 'NULL') for x in row_list]
            sql_str = f"({', '.join(repr_list)}),"
            insert_sql += '\n'+sql_str
    insert_sql = insert_sql.strip(',') + ';'

    connection_db1.query_raw(create_sql)
    connection_db1.query_raw(insert_sql)


@pytest.fixture
def default_data_db2(connection_db2, datadir):
    googleplaystore_user_reviews_file = datadir.join('googleplaystore_user_reviews.csv')
    googleplaystore_user_reviews_fpath = str(googleplaystore_user_reviews_file)
    n_rows = 100
    create_sql = """
        CREATE TABLE IF NOT EXISTS googleplaystore_user_reviews (
            App VARCHAR(300) NULL, 
            `Translated_Review` VARCHAR(500) NULL,
            Sentiment VARCHAR(100) NULL,
            `Sentiment_Polarity` FLOAT NULL,
            `Sentiment_Subjectivity` FLOAT NULL
        );  
    """
    insert_sql = "INSERT INTO googleplaystore_user_reviews VALUES"

    with open(googleplaystore_user_reviews_fpath, 'r') as read_obj:
        csv_reader = csv.reader(read_obj, delimiter=',')
        for i, row in enumerate(csv_reader):
            if i == 0:
                continue
            if i > n_rows:
                break
            row_list = list(row)
            row_list[3] = float(row_list[3]) if row_list[3] and row_list[3].lower() != 'nan' else None
            row_list[4] = float(row_list[4]) if row_list[4] and row_list[4].lower() != 'nan' else None
            for j, item in enumerate(row_list):
                if row_list[j] is not None and isinstance(row_list[j], str):
                    if row_list[j] == 'NaN' or not row_list[j]:
                        row_list[j] = None

            repr_list = [(repr(x) if x is not None else 'NULL') for x in row_list]
            sql_str = f"({', '.join(repr_list)}),"
            insert_sql += '\n'+sql_str
    insert_sql = insert_sql.strip(',') + ';'

    connection_db2.query_raw(create_sql)
    connection_db2.query_raw(insert_sql)



