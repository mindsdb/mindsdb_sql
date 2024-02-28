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
           'TABLES',
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
            expected_ast = Show(category=cat)

            assert str(ast).lower() == sql.lower()
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_show_unknown_condition_error(self, dialect):
        sql = "SHOW databases WITH"
        with pytest.raises(ParsingException):
            parse_sql(sql, dialect=dialect)

    def test_show_tables_from_db(self, dialect):
        sql = "SHOW tables from db"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='tables', from_table=Identifier('db'))

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_show_function_status(self, dialect):
        sql = "show function status where Db = 'MINDSDB' AND Name LIKE '%'"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='function status',
                            where=BinaryOperation('and', args=[
                                BinaryOperation('=', args=[Identifier('Db'), Constant('MINDSDB')]),
                                BinaryOperation('like', args=[Identifier('Name'), Constant('%')])
                            ]),
                        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


    def test_show_character_set(self, dialect):
        sql = "show character set where charset = 'utf8mb4'"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(category='character set',
                            where=BinaryOperation('=', args=[Identifier('charset'), Constant('utf8mb4')]),
                        )

        # assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_from_where(self, dialect):
        sql = "SHOW FULL TABLES FROM ttt LIKE 'zzz' WHERE xxx"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(
            category='TABLES',
            modes=['FULL'],
            from_table=Identifier('ttt'),
            like='zzz',
            where=Identifier('xxx'),
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_full_columns(self, dialect):
        sql = "SHOW FULL COLUMNS FROM `concrete` FROM `files`"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(
            category='COLUMNS',
            modes=['FULL'],
            from_table=Identifier('files.concrete')
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestShowNoSqlite:
    def test_category(self, dialect):
        categories = [
            'BINARY LOGS',
            'MASTER LOGS',
            'PROCESSLIST',
            'STORAGE ENGINES',
            'PRIVILEGES',
            'MASTER STATUS',
            'PROFILES',
            'REPLICAS',
        ]

        for cat in categories:
            sql = f"SHOW {cat}"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(category=cat)

            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    @pytest.mark.parametrize('cat', [
            'CHARACTER SET',
            'CHARSET',
            'COLLATION',
            'DATABASES',
            'SCHEMAS',
            'FUNCTION STATUS',
            'PROCEDURE STATUS',
            'GLOBAL STATUS',
            'SESSION STATUS',
            'STATUS',
            'GLOBAL VARIABLES',
            'SESSION VARIABLES',
        ])
    def test_common_like_where(self, dialect, cat):

        sql = f"SHOW {cat} like 'pattern' where a=1"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Show(
            category=cat,
            like='pattern',
            where=BinaryOperation(op='=', args=[
                Identifier('a'),
                Constant(1)
            ])
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_common_like_where_from_in(self, dialect):
        categories = [
            'TABLE STATUS',
            'OPEN TABLES',
            'TRIGGERS',
        ]

        for cat in categories:
            sql = f"SHOW {cat} from tab1 in tab2 like 'pattern' where a=1"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(
                category=cat,
                like='pattern',
                from_table=Identifier('tab1'),
                in_table=Identifier('tab2'),
                where=BinaryOperation(op='=', args=[
                    Identifier('a'),
                    Constant(1)
                ])
            )

            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_common_like_where_from_in_modes(self, dialect):
        categories = [
            'TABLES',
        ]
        modes = [
            ['EXTENDED'],
            ['FULL'],
            ['EXTENDED', 'FULL'],
        ]

        for cat in categories:
            for mode in modes:

                sql = f"SHOW {' '.join(mode)} {cat} from tab1 in tab2 like 'pattern' where a=1"
                ast = parse_sql(sql, dialect=dialect)
                expected_ast = Show(
                    category=cat,
                    like='pattern',
                    from_table=Identifier('tab1'),
                    in_table=Identifier('tab2'),
                    modes=mode,
                    where=BinaryOperation(op='=', args=[
                        Identifier('a'),
                        Constant(1)
                    ])
                )

                assert str(ast) == str(expected_ast)
                assert ast.to_tree() == expected_ast.to_tree()

    def test_common_like_double_where_from_in_modes(self, dialect):
        categories = [
            'COLUMNS',
            'FIELDS',
            'INDEX',
            'INDEXES',
            'KEYS',
        ]
        modes = [
            ['EXTENDED'],
            ['FULL'],
            ['EXTENDED', 'FULL'],
        ]
        for cat in categories:
            for mode in modes:

                sql = f"SHOW {' '.join(mode)} {cat} from tab1 from db1 in tab2 in db2 like 'pattern' where a=1"
                ast = parse_sql(sql, dialect=dialect)
                expected_ast = Show(
                    category=cat,
                    like='pattern',
                    from_table=Identifier('db1.tab1'),
                    in_table=Identifier('db2.tab2'),
                    modes=mode,
                    where=BinaryOperation(op='=', args=[
                        Identifier('a'),
                        Constant(1)
                    ])
                )

                assert str(ast) == str(expected_ast)
                assert ast.to_tree() == expected_ast.to_tree()

    def test_custom(self, dialect):

        for arg in ['STATUS', 'MUTEX']:
            sql = f"SHOW ENGINE engine_name {arg}"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(
                category='ENGINE',
                name='engine_name',
                modes=[arg],
            )

            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

        for arg in ['FUNCTION', 'PROCEDURE']:
            sql = f"SHOW {arg} CODE obj_name"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(
                category=f"{arg} CODE",
                name='obj_name',
            )

            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

        for arg in ['SLAVE', 'REPLICA']:
            sql = f"SHOW {arg} STATUS FOR CHANNEL channel"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(
                category=f"REPLICA STATUS",
                name='channel',
            )

            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

            # without channel
            sql = f"SHOW {arg} STATUS"
            ast = parse_sql(sql, dialect=dialect)
            expected_ast = Show(
                category=f"REPLICA STATUS",
            )

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


class TestMindsdb:

    def test_show(self):
        sql = '''
           show full databases
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='databases',
            modes=['full']
        )

        assert statement2.to_tree() == statement.to_tree()

        # ---  show models ---
        sql = '''
           show models
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='models'
        )
        assert statement2.to_tree() == statement.to_tree()

        sql = '''
           show models FROM db_name
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='models',
            from_table=Identifier('db_name')
        )
        assert statement2.to_tree() == statement.to_tree()

        sql = '''
           show models LIKE 'pattern'
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='models',
            like='pattern',
        )
        assert statement2.to_tree() == statement.to_tree()

        sql = '''
           show models FROM db_name LIKE 'pattern' WHERE a=1
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='models',
            from_table=Identifier('db_name'),
            like='pattern',
            where=BinaryOperation(op='=', args=[
                Identifier('a'),
                Constant(1)
            ])
        )
        assert statement2.to_tree() == statement.to_tree()

        sql = '''
           show predictors
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='predictors'
        )
        assert statement2.to_tree() == statement.to_tree()

        # --- ml_engines ---
        sql = '''
            show ML_ENGINES
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='ML_ENGINES',
        )
        assert statement2.to_tree() == statement.to_tree()

        sql = '''
           show ML_ENGINES LIKE 'pattern'
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='ML_ENGINES',
            like='pattern',
        )
        assert statement2.to_tree() == statement.to_tree()

        sql = '''
           show ML_ENGINES LIKE 'pattern' WHERE  a=1
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='ML_ENGINES',
            like='pattern',
            where=BinaryOperation(op='=', args=[
                Identifier('a'),
                Constant(1)
            ])
        )
        assert statement2.to_tree() == statement.to_tree()

        # --- handlers ---
        sql = '''
            show Handlers
        '''
        statement = parse_sql(sql, dialect='mindsdb')
        statement2 = Show(
            category='HANDLERS',
        )
        assert statement2.to_tree() == statement.to_tree()



