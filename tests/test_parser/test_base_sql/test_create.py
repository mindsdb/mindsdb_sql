import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *


class TestDelete:
    def test_delete(self):
        expected_ast = CreateTable(
            name=Identifier('int1.model_name'),
            replace=True,
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
        ast = parse_sql(sql, dialect='mindsdb')

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

        # without parens
        sql = '''
         create or replace table int1.model_name
            select a from ddd             
        '''
        ast = parse_sql(sql, dialect='mindsdb')

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

        expected_ast.replace = False

        # no replace
        sql = '''
         create table int1.model_name
            select a from ddd             
        '''
        ast = parse_sql(sql, dialect='mindsdb')

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()


