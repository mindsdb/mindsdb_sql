import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestCreateFile:
    def test_create_file(self):
        sql = "CREATE TABLE files.my_table USING url='http://som.e/url'"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateFile(
            name=Identifier('files.my_table'),
            url='http://som.e/url'
        )
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
