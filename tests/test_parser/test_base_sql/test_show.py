import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.ast.show import Show
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestShow:
    def test_show_databases(self, dialect):
        sql = "SHOW databases"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='databases', condition=None, expression=None)

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_show_unknown_category_error(self, dialect):
        sql = "SHOW abracadabra"

        with pytest.raises(ParsingException):
            parse_sql(sql, dialect=dialect)

    def test_show_unknown_condition_error(self, dialect):
        sql = "SHOW databases WITH"
        with pytest.raises(ParsingException):
            parse_sql(sql, dialect=dialect)

    def test_show_tables_from_db(self, dialect):
        sql = "SHOW tables from db"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='tables', condition='from', expression=Identifier('db'))

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_show_function_status(self, dialect):
        sql = "show function status where Db = 'MINDSDB' AND Name LIKE '%'"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='function status', condition='where',
                            expression=BinaryOperation('and', args=[
                                BinaryOperation('=', args=[Identifier('Db'), Constant('MINDSDB')]),
                                BinaryOperation('like', args=[Identifier('Name'), Constant('%')])
                            ]),
                        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
