import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestCreate:
    def test_create_from_select(self, dialect):
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


class TestCreateMindsdb:

    def test_create(self):

        for is_replace in [True, False]:
            for if_not_exists in [True, False]:

                expected_ast = CreateTable(
                    name=Identifier('mydb.Persons'),
                    is_replace=is_replace,
                    if_not_exists=if_not_exists,
                    columns=[
                        TableColumn(name='PersonID', type='int'),
                        TableColumn(name='LastName', type='varchar', length=255),
                        TableColumn(name='FirstName', type='char', length=10),
                        TableColumn(name='Info', type='json'),
                        TableColumn(name='City', type='varchar'),
                    ]
                )
                replace_str = 'OR REPLACE' if is_replace else ''
                exist_str = 'IF NOT EXISTS' if if_not_exists else ''

                sql = f'''
                 CREATE {replace_str} TABLE {exist_str} mydb.Persons(
                    PersonID int,
                    LastName varchar(255),
                    FirstName char(10),
                    Info json,
                    City varchar
                 )
                '''
                print(sql)
                ast = parse_sql(sql)

                assert str(ast).lower() == str(expected_ast).lower()
                assert ast.to_tree() == expected_ast.to_tree()

