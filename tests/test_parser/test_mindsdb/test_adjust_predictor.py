import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
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
