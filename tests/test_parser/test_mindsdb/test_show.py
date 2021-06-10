import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb.show import Show
from mindsdb_sql.parser.ast import *


class TestShow:
    def test_show_keyword(self):
        for keyword in ['STREAMS',
                        'PREDICTORS',
                        'INTEGRATIONS',
                        'PUBLICATIONS',
                        'ALL']:
            sql = f"SHOW {keyword}"
            ast = parse_sql(sql, dialect='mindsdb')
            expected_ast = Show(value=keyword)

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_show_tables_arg(self):
        for keyword in ['VIEWS', 'TABLES']:
            sql = f"SHOW {keyword} integration_name"
            ast = parse_sql(sql, dialect='mindsdb')
            expected_ast = Show(value=keyword, arg=Identifier("integration_name"))


            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_use_wrong_dialect(self):
        sql = "SHOW my_integration"
        for dialect in ['sqlite', 'mysql']:
            with pytest.raises(Exception):
                ast = parse_sql(sql, dialect=dialect)
