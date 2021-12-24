import itertools
import pytest
from mindsdb_sql import parse_sql, get_lexer_parser
from mindsdb_sql.parser.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.utils import JoinType


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestMiscQueries:
    def test_set(self, dialect):
        lexer, parser = get_lexer_parser(dialect)

        sql = "SET NAMES some_name"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category="names", arg=Identifier('some_name'))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "set character_set_results = NULL"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(arg=BinaryOperation('=', args=[Identifier('character_set_results'), NullConstant()]))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_start_transaction(self, dialect):
        sql = "start transaction"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = StartTransaction()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_rollback(self, dialect):
        sql = "rollback"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = RollbackTransaction()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_commit(self, dialect):
        sql = "commit"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = CommitTransaction()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_explain(self, dialect):
        sql = "explain some_table"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Explain(target=Identifier('some_table'))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_alter_table_keys(self, dialect):
        sql = "alter table some_table disable keys"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = AlterTable(target=Identifier('some_table'), arg='disable keys')
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "alter table some_table enable keys"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = AlterTable(target=Identifier('some_table'), arg='enable keys')
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_autocommit(self, dialect):
        sql = "set autocommit=1"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(
            category=None,
            arg=BinaryOperation(
                op='=',
                args=(
                    Identifier('autocommit'),
                    Constant(1)
                )
            )
        )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestMiscQueriesNoSqlite:
    def test_set(self, dialect):

        sql = "set var1 = NULL, var2 = 10"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(arg=Tuple(items=[
                BinaryOperation('=', args=[Identifier('var1'), NullConstant()]),
                BinaryOperation('=', args=[Identifier('var2'), Constant(10)]),
            ])
                           )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_set_charset(self, dialect):

        sql = "SET CHARACTER SET DEFAULT"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category='CHARSET', arg=SpecialConstant('DEFAULT'))

        assert ast.to_tree() == expected_ast.to_tree()

        sql = "SET CHARSET DEFAULT"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category='CHARSET', arg=SpecialConstant('DEFAULT'))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET CHARSET 'utf8'"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category='CHARSET', arg=Constant('utf8'))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_set_transaction(self, dialect):

        sql = "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = SetTransaction(
            isolation_level='REPEATABLE READ',
            access_mode='READ WRITE',
            scope='GLOBAL')

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET SESSION TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = SetTransaction(
            isolation_level='SERIALIZABLE',
            access_mode='READ ONLY',
            scope='SESSION')

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = SetTransaction(
            isolation_level='READ UNCOMMITTED'
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET TRANSACTION READ ONLY"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = SetTransaction(
            access_mode='READ ONLY'
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)


