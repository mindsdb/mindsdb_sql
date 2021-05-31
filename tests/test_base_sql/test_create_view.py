import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.lexer import SQLLexer
from mindsdb_sql.ast import *


@pytest.mark.parametrize('dialect', ['sqlite',
                                     'mysql'])
class TestCreateView:
    def test_create_view_lexer(self, dialect):
        sql = "CREATE VIEW my_view FROM integration AS ( SELECT * FROM predictor )"
        tokens = list(SQLLexer().tokenize(sql))
        assert tokens[0].value == 'CREATE'
        assert tokens[0].type == 'CREATE'

        assert tokens[1].value == 'VIEW'
        assert tokens[1].type == 'VIEW'

    def test_create_view_full(self, dialect):
        sql = "CREATE VIEW my_view FROM integration AS ( SELECT * FROM predictor )"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = CreateView(name='my_view',
                                  from_table=Identifier('integration'),
                                  query=Select(targets=[Identifier('*')],
                                               from_table=Identifier('predictor')))

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_view_nofrom(self, dialect):
        sql = "CREATE VIEW my_view AS ( SELECT * FROM predictor )"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = CreateView(name='my_view',
                                  query=Select(targets=[Identifier('*')],
                                               from_table=Identifier('predictor')))

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
