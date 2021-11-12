import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestDropPredictor:
    def test_drop_predictor_lexer(self):
        sql = "DROP PREDICTOR mindsdb.pred"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'DROP'
        assert tokens[1].type == 'PREDICTOR'
        assert tokens[2].type == 'ID'
        assert tokens[2].value == 'mindsdb.pred'

    def test_drop_predictor_ok(self):
        sql = "DROP PREDICTOR mindsdb.pred"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropPredictor(name=Identifier('mindsdb.pred'))
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_drop_predictor_table_syntax_ok(self):
        sql = "DROP TABLE mindsdb.pred"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropPredictor(name=Identifier('mindsdb.pred'))
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
