import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestSpecificSelects:
    def test_select_from_predictors(self):
        sql = "SELECT * FROM predictors WHERE name = 'pred_name'"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier('predictors'),
            where=BinaryOperation('=', args=[Identifier('name'), Constant('pred_name')])
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_predict_column(self):
        sql = "SELECT predict FROM mindsdb.predictors"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('predict')],
            from_table=Identifier('mindsdb.predictors'),
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
