import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb.use import Use
from mindsdb_sql.parser.ast import *


class TestUse:
    def test_use(self):
        sql = "USE my_integration"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Use(value=Identifier('my_integration'))

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


    def test_use_wrong_dialect(self):
        sql = "USE my_integration"
        for dialect in ['sqlite', 'mysql']:
            with pytest.raises(Exception):
                ast = parse_sql(sql, dialect=dialect)
