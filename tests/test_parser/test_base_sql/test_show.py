import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.ast.show import Show
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestShow:
    def test_show_category(self, dialect):
        categories = ['SCHEMAS',
           'DATABASES',
           'TABLES',
           'FULL TABLES',
           'VARIABLES',
           'PLUGINS',
           'SESSION VARIABLES',
           'SESSION STATUS',
           'GLOBAL VARIABLES',
           'PROCEDURE STATUS',
           'FUNCTION STATUS',
           'CREATE TABLE',
           'WARNINGS',
           'ENGINES',
           'CHARSET',
           'CHARACTER SET',
           'COLLATION',
           'TABLE STATUS',
           'STATUS']
        for cat in categories:
            sql = f"SHOW {cat}"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(category=cat, condition=None, expression=None)

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


    def test_show_character_set(self, dialect):
        sql = "show character set where charset = 'utf8mb4'"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='character set', condition='where',
                            expression=BinaryOperation('=', args=[Identifier('charset'), Constant('utf8mb4')]),
                        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


class TestShowAdapted:

    def test_show_database_adapted(self):
        statement = Select(
            targets=[Identifier(parts=["schema_name"], alias=Identifier('Database'))],
            from_table=Identifier(parts=['information_schema', 'SCHEMATA'])
        )
        sql = statement.get_string()

        statement2 = parse_sql(sql, dialect='mindsdb')

        assert statement2.to_tree() == statement.to_tree()

