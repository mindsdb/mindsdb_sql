import pytest

from mindsdb_sql.parser.ast.drop import DropDatabase
from mindsdb_sql import parse_sql


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestDDL:
    def test_drop_database(self, dialect):

        sql = "DROP DATABASE IF EXISTS dbname"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = DropDatabase('dbname', if_exists=True)

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

        sql = "DROP DATABASE dbname"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = DropDatabase('dbname', if_exists=False)

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

        # DROP SCHEMA is a synonym for DROP DATABASE.
        sql = "DROP SCHEMA dbname"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = DropDatabase('dbname')

        assert str(ast).lower() == 'DROP DATABASE dbname'.lower()
        assert ast.to_tree() == expected_ast.to_tree()


