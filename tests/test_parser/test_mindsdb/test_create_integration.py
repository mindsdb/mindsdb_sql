import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.utils import to_single_line


class TestCreateIntegration:
    def test_create_integration_lexer(self):
        sql = "CREATE INTEGRATION db WITH ENGINE = 'mysql', PARAMETERS = {\"user\": \"admin\", \"password\": \"admin\"}"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'CREATE'
        assert tokens[1].type == 'INTEGRATION'
        assert tokens[2].type == 'ID'
        assert tokens[3].type == 'WITH'
        assert tokens[4].type == 'ENGINE'
        assert tokens[5].type == 'EQUALS'
        assert tokens[6].type == 'STRING'
        assert tokens[7].type == 'COMMA'
        assert tokens[8].type == 'PARAMETERS'
        assert tokens[9].type == 'EQUALS'
        assert tokens[10].type == 'JSON'

    def test_create_integration_ok(self):
        sql = """
            CREATE INTEGRATION db
            WITH ENGINE = 'mysql',
            PARAMETERS = {"user": "admin", "password": "admin", "host": "127.0.0.1"}
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateIntegration(name='db',
                                  engine='mysql',
                                  parameters=dict(user='admin', password='admin', host='127.0.0.1'))
        assert str(ast).lower() == to_single_line(sql.lower())
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_datasource_ok(self):
        sql = """
            CREATE DATASOURCE db
            WITH ENGINE = 'mysql',
            PARAMETERS = {"user": "admin", "password": "admin123_.,';:!@#$%^&*(){}[]", "host": "127.0.0.1"}
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateIntegration(name='db',
                                  engine='mysql',
                                  parameters=dict(user='admin', password="admin123_.,';:!@#$%^&*(){}[]", host='127.0.0.1'))
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_integration_invalid_json(self):
        sql = "CREATE INTEGRATION db WITH ENGINE = 'mysql', PARAMETERS = 'wow'"
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect='mindsdb')
