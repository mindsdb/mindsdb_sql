import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestDDL:

    def test_insert(self, dialect):
        sql = "INSERT INTO tbl_name(a, c) VALUES (1, 3), (4, 5)"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Insert(
            table=Identifier('tbl_name'),
            columns=[Identifier('a'), Identifier('c')],
            values=[
                [Constant(1), Constant(3)],
                [Constant(4), Constant(5)],
            ]
        )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_insert_no_columns(self, dialect):
        sql = "INSERT INTO tbl_name VALUES (1, 3), (4, 5)"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Insert(
            table=Identifier('tbl_name'),
            values=[
                [Constant(1), Constant(3)],
                [Constant(4), Constant(5)],
            ]
        )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_insert_from_select(self, dialect):
        sql = "INSERT INTO tbl_name(a, c) SELECT b, d from table2"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Insert(
            table=Identifier('tbl_name'),
            columns=[Identifier('a'), Identifier('c')],
            from_select=Select(
                targets=[
                    Identifier('b'),
                    Identifier('d'),
                ],
                from_table=Identifier('table2')
            )
        )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_insert_from_select_no_columns(self, dialect):
        sql = "INSERT INTO tbl_name SELECT b, d from table2"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Insert(
            table=Identifier('tbl_name'),
            from_select=Select(
                targets=[
                    Identifier('b'),
                    Identifier('d'),
                ],
                from_table=Identifier('table2')
            )
        )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
