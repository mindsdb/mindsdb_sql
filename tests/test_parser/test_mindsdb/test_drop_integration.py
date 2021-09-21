import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestDropIntegration:
    def test_drop_integration_lexer(self):
        sql = "DROP INTEGRATION db"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'DROP'
        assert tokens[1].type == 'INTEGRATION'
        assert tokens[2].type == 'ID'
        assert tokens[2].value == 'db'

    def test_drop_integration_ok(self):
        sql = "DROP INTEGRATION db"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropIntegration(name='db')
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
