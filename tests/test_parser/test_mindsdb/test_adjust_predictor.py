import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.adjust_predictor import AdjustPredictor
from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer


class TestAdjustPredictor:
    def test_adjust_predictor_lexer(self):
        sql = "ADJUST mindsdb.pred FROM integration_name (SELECT * FROM table_1) USING a=1"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'ADJUST'
        assert tokens[1].type == 'ID'
        assert tokens[1].value == 'mindsdb.pred'

    def test_adjust_predictor_full(self):
        sql = "ADJUST mindsdb.pred FROM integration_name (SELECT * FROM table_1) USING a=1, b=null"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = AdjustPredictor(
            name=Identifier('mindsdb.pred'),
            integration_name=Identifier('integration_name'),
            query_str="SELECT * FROM table_1",
            using={'a': 1, 'b': None},
        )
        assert ' '.join(str(ast).split()).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
