import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.evaluate import Evaluate
from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer


class TestEvaluate:
    def test_evaluate_lexer(self):
        sql = "EVALUATE balanced_accuracy_score FROM (SELECT true, pred FROM table_1)"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'EVALUATE'
        assert tokens[1].type == 'ID'
        assert tokens[1].value == 'balanced_accuracy_score'

    def test_evaluate_full_1(self):
        sql = "EVALUATE balanced_accuracy_score FROM (SELECT true, pred FROM table_1) USING adjusted=1, param2=2;"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Evaluate(
            name=Identifier('balanced_accuracy_score'),
            query_str="SELECT true, pred FROM table_1",
            using={'adjusted': 1, 'param2': 2},
        )
        assert ' '.join(str(ast).split()).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_evaluate_full_2(self):
        query_str = """SELECT t.rental_price as ground_truth, m.rental_price as prediction FROM example_db.demo_data.home_rentals as t JOIN mindsdb.home_rentals_model as m limit 100"""  # noqa
        sql = f"""EVALUATE r2_score FROM ({query_str});"""
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Evaluate(
            name=Identifier('r2_score'),
            query_str=query_str,
        )
        assert ' '.join(str(ast).split()).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
