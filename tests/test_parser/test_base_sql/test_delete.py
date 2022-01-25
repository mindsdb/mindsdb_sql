import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestDelete:
    def test_delete(self, dialect):
        sql = "delete from ds.table1 where field > value"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Delete(
            table=Identifier('ds.table1'),
            where=BinaryOperation(
                op='>',
                args=(
                    Identifier('field'),
                    Identifier('value'),
                )
            ),
        )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_delete_no_where(self, dialect):
        sql = "delete from ds.table1"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Delete(
            table=Identifier('ds.table1'),
            where=None
        )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()