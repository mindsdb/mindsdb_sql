import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.utils import JoinType
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
        sql = "EVALUATE balanced_accuracy_score FROM (SELECT ground_truth, pred FROM table_1) USING adjusted=1, param2=2;"  #  noqa
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Evaluate(
            name=Identifier('balanced_accuracy_score'),
            data=Select(targets=[Identifier('ground_truth'), Identifier('pred')], from_table=Identifier('table_1')),
            using={'adjusted': 1, 'param2': 2},
        )
        assert ' '.join(str(ast).split()).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_evaluate_full_2(self):
        query_str = """SELECT t.rental_price as ground_truth, m.rental_price as prediction FROM example_db.demo_data.home_rentals as t JOIN mindsdb.home_rentals_model as m limit 100"""  # noqa
        sql = f"""EVALUATE r2_score FROM ({query_str});"""
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Evaluate(
            name=Identifier('r2_score'),
            data=Select(targets=[Identifier('t.rental_price', alias=Identifier('ground_truth')),
                                 Identifier('m.rental_price', alias=Identifier('prediction'))],
                        from_table=Join(left=Identifier('example_db.demo_data.home_rentals', alias=Identifier('t')),
                                        right=Identifier('mindsdb.home_rentals_model', alias=Identifier('m')),
                                        join_type=JoinType.JOIN),
                        limit=Constant(100))
        )
        assert ' '.join(str(ast).split()).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast).lower() == str(expected_ast).lower()
