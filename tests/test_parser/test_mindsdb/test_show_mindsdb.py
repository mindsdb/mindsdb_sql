import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.show import Show
from mindsdb_sql.parser.ast import *


class TestShowMindsdb:
    def test_show_keyword(self):
        for keyword in ['STREAMS',
                        'PREDICTORS',
                        'INTEGRATIONS',
                        'PUBLICATIONS',
                        'ALL']:
            sql = f"SHOW {keyword}"
            ast = parse_sql(sql, dialect='mindsdb')
            expected_ast = Show(category=keyword, condition=None, expression=None)

            assert str(ast).lower() == sql.lower()
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_show_tables_arg(self):
        for keyword in ['VIEWS', 'TABLES']:
            sql = f"SHOW {keyword} from integration_name"
            ast = parse_sql(sql, dialect='mindsdb')
            expected_ast = Show(category=keyword, condition='from', expression=Identifier('integration_name'))

            assert str(ast).lower() == sql.lower()
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

