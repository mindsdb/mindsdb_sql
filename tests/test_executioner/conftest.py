import os
import csv
from distutils import dir_util
import pytest
from sqlalchemy import orm
from sqlalchemy import create_engine

from mindsdb_sql.executioner.connection import SQLConnection


@pytest.fixture(scope="function")
def db_engine():
    db_url = os.environ.get('DB_URL', 'mysql+pymysql://user:pwd@test_db/test?charset=utf8mb4')
    db_echo = int(os.environ.get('DB_ECHO', '0') == '1')
    assert 'test' in db_url
    engine = create_engine(db_url, echo=db_echo)
    return engine


@pytest.fixture
def connection(db_engine):
    params = {
        'db_url': os.environ.get('DB_URL', 'mysql+pymysql://user:pwd@test_db/test?charset=utf8mb4'),
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
def default_data(connection, datadir):
    googleplaystore_file = datadir.join('googleplaystore.csv')
    googleplaystore_fpath = str(googleplaystore_file)
    n_rows = 100
    # connection.execute('DROP TABLE googleplaystore;')
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

    connection.query_raw(create_sql)
    connection.query_raw(insert_sql)



