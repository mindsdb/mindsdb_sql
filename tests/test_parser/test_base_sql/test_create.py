import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *

@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestCreate:
    def test_create(self, dialect):
        expected_ast = CreateTable(
            name=Identifier('int1.model_name'),
            is_replace=True,
            from_select=Select(
                targets=[Identifier('a')],
                from_table=Identifier('ddd'),
            )
        )

        # with parens
        sql = '''
         create or replace table int1.model_name (
            select a from ddd             
        )
        '''
        ast = parse_sql(sql, dialect=dialect)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

        # without parens
        sql = '''
         create or replace table int1.model_name
            select a from ddd             
        '''
        ast = parse_sql(sql, dialect=dialect)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

        expected_ast.is_replace = False

        # no replace
        sql = '''
         create table int1.model_name
            select a from ddd             
        '''
        ast = parse_sql(sql, dialect='mindsdb')

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()


