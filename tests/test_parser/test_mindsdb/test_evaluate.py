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

    def test_evaluate_full(self):
        sql = "EVALUATE balanced_accuracy_score FROM (SELECT true, pred FROM table_1) USING adjusted=1, param2=2"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Evaluate(
            name=Identifier('balanced_accuracy_score'),
            query_str="SELECT true, pred FROM table_1",
            using={'adjusted': 1, 'param2': 2},
        )
        assert ' '.join(str(ast).split()).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
