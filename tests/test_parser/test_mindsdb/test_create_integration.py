import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.ast import Identifier
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer

class TestCreateDatabase:
    def test_create_database_lexer(self):
        sql = "CREATE DATABASE IF NOT EXISTS db WITH ENGINE = 'mysql', PARAMETERS = {\"user\": \"admin\", \"password\": \"admin\"}"
        tokens = list(MindsDBLexer().tokenize(sql))
        assert tokens[0].type == 'CREATE'
        assert tokens[1].type == 'DATABASE'
        assert tokens[2].type == 'IF_NOT_EXISTS'
        assert tokens[3].type == 'ID'
        assert tokens[4].type == 'WITH'
        assert tokens[5].type == 'ENGINE'
        assert tokens[6].type == 'EQUALS'
        assert tokens[7].type == 'QUOTE_STRING'
        assert tokens[8].type == 'COMMA'
        assert tokens[9].type == 'PARAMETERS'
        assert tokens[10].type == 'EQUALS'
        # next tokens come separately, not just single JSON
        # assert tokens[10].type == 'JSON'

    def test_create_database_ok(self, ):
        sql = "CREATE DATABASE db"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateDatabase(name=Identifier('db'), engine=None, parameters=None)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        sql = "CREATE DATABASE db ENGINE 'eng'"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateDatabase(name=Identifier('db'), engine='eng', parameters=None)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # variants with or without ',' and '='
        for with_ in ('WITH', ''):
            engines = [
                "ENGINE 'mysql'",
                "ENGINE = 'mysql'",
                "ENGINE 'mysql',",
                "ENGINE = 'mysql',",
            ]
            for engine in engines:
                for equal in ('=', ''):
                    sql = """
                        CREATE DATABASE db
                        %(with)s %(engine)s
                        PARAMETERS %(equal)s {"user": "admin", "password": "admin123_.,';:!@#$%%^&*(){}[]", "host": "127.0.0.1"}
                    """ % {'equal': equal, 'engine': engine, 'with': with_}

                    ast = parse_sql(sql, dialect='mindsdb')
                    expected_ast = CreateDatabase(name=Identifier('db'),
                                                engine='mysql',
                                                parameters=dict(user='admin', password="admin123_.,';:!@#$%^&*(){}[]", host='127.0.0.1'))
                    assert str(ast) == str(expected_ast)
                    assert ast.to_tree() == expected_ast.to_tree()

        sql = """
            CREATE or REPLACE DATABASE db
                    /*
                        multiline comment
                    */
                    WITH ENGINE='mysql'
                    PARAMETERS = {"user": "admin", "password": "admin123_.,';:!@#$%^&*(){}[]", "host": "127.0.0.1"}
        """

        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateDatabase(name=Identifier('db'),
                                        engine='mysql',
                                        is_replace=True,
                                        parameters=dict(user='admin', password="admin123_.,';:!@#$%^&*(){}[]",
                                                        host='127.0.0.1'))
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # test with if not exists
        sql = """
            CREATE DATABASE IF NOT EXISTS db
            WITH ENGINE='mysql'
            """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateDatabase(name=Identifier('db'),
                                        engine='mysql',
                                        if_not_exists=True,
                                        parameters=None)
        assert str(ast) == str(expected_ast)

    def test_create_database_invalid_json(self):
        sql = "CREATE DATABASE db WITH ENGINE = 'mysql', PARAMETERS = 'wow'"
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect='mindsdb')

    def test_create_project(self):

        sql = "create PROJECT dbname"
        ast = parse_sql(sql, dialect='mindsdb')

        expected_ast = CreateDatabase(name=Identifier('dbname'), engine=None, parameters=None)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

        # test with if not exists
        sql = """
            CREATE PROJECT IF NOT EXISTS db
            """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateDatabase(name=Identifier('db'),
                                        engine=None,
                                        if_not_exists=True,
                                        parameters=None)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()


    def test_create_database_using(self):

        sql = "CREATE DATABASE db using ENGINE = 'mysql', PARAMETERS = {'A': 1}"
        ast = parse_sql(sql, dialect='mindsdb')

        expected_ast = CreateDatabase(name=Identifier('db'), engine='mysql', parameters={'A': 1})

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

