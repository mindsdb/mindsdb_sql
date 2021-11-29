import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.retrain_predictor import RetrainPredictor


class TestRetrainPredictor:
    def test_retrain_predictor_lexer(self):
        sql = "RETRAIN mindsdb.pred"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'RETRAIN'
        assert tokens[1].type == 'ID'
        assert tokens[1].value == 'mindsdb.pred'

    def test_retrain_predictor_ok(self):
        sql = "RETRAIN mindsdb.pred"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = RetrainPredictor(name=Identifier('mindsdb.pred'))
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
