import pytest
from mindsdb_sql import parse_sql

class TestSql:
    def test_ending(self):
        sql = """INSERT INTO tbl_name VALUES (1, 3)  
           ;
        """

        parse_sql(sql, dialect='mindsdb')
