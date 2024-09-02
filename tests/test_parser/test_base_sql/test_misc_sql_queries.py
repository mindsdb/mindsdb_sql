import pytest
from mindsdb_sql import parse_sql, get_lexer_parser
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestMiscQueries:
    def test_set(self, dialect):
        lexer, parser = get_lexer_parser(dialect)

        sql = "SET names some_name"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category="names", value=Identifier('some_name'))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "set character_set_results = NULL"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(name=Identifier('character_set_results'), value=NullConstant())
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
            name=Identifier('autocommit'),
            value=Constant(1)
        )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestMiscQueriesNoSqlite:
    def test_set(self, dialect):

        sql = "set var1 = NULL, var2 = 10"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(
            set_list=[
                Set(name=Identifier('var1'), value=NullConstant()),
                Set(name=Identifier('var2'), value=Constant(10)),
            ]
        )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)


        sql = "SET NAMES some_name collate DEFAULT"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category="NAMES",
                           value=Constant('some_name', with_quotes=False),
                           params={'COLLATE': 'DEFAULT'})
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET names some_name collate 'utf8mb4_general_ci'"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category="names",
                           value=Constant('some_name', with_quotes=False),
                           params={'COLLATE': Constant('utf8mb4_general_ci')})
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_set_charset(self, dialect):

        sql = "SET CHARACTER SET DEFAULT"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category='CHARSET', value=Constant('DEFAULT', with_quotes=False))

        assert ast.to_tree() == expected_ast.to_tree()

        sql = "SET CHARSET DEFAULT"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category='CHARSET', value=Constant('DEFAULT', with_quotes=False))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET CHARSET 'utf8'"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(category='CHARSET', value=Constant('utf8'))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_set_transaction(self, dialect):

        sql = "SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Set(
            category='TRANSACTION',
            params={
                'isolation level': 'REPEATABLE READ',
                'access_mode': 'READ WRITE',
            },
            scope='GLOBAL'
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET SESSION TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE"

        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Set(
            category='TRANSACTION',
            params={
                'isolation level': 'SERIALIZABLE',
                'access_mode': 'READ ONLY',
            },
            scope='SESSION'
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"

        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Set(
            category='TRANSACTION',
            params={
                'isolation level': 'READ UNCOMMITTED'
            },
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SET TRANSACTION READ ONLY"

        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Set(
            category='TRANSACTION',
            params=dict(
                access_mode='READ ONLY',
            )
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_begin(self, dialect):
        sql = "begin"

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = StartTransaction()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

class TestMindsdb:
    def test_charset(self):
        sql = "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"

        ast = parse_sql(sql)
        expected_ast = Set(category="NAMES",
                           value=Constant('utf8mb4', with_quotes=False),
                           params={'COLLATE': Constant('utf8mb4_unicode_ci', with_quotes=False)})
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_set_version(self):
        sql = "SET active model_name.1"

        ast = parse_sql(sql)
        expected_ast = Set(category='active', value=Identifier(parts=['model_name', '1']))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)


    def test_interval(self):
        for value in ('1 day', "'1' day", "'1 day'"):
            sql = f"""
               select interval {value} + 1 from aaa
               where 'a' > interval "1 min"
            """

            expected_ast = Select(
                targets=[
                    BinaryOperation(op='+', args=[
                        Interval('1 day'),
                        Constant(1)
                    ])
                ],
                from_table=Identifier('aaa'),
                where=BinaryOperation(
                    op='>',
                    args=[
                        Constant('a'),
                        Interval('1 min'),
                    ]
                )
            )

            ast = parse_sql(sql)

            assert str(ast).lower() == str(expected_ast).lower()
            assert ast.to_tree() == expected_ast.to_tree()

