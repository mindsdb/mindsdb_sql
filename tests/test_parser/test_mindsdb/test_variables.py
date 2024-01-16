from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.ast import Variable

class TestMDBParser:
    def test_select_variable(self):
        sql = 'SELECT @version'
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Variable('version')])
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)

        sql = 'SELECT @@version'
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Variable('version', is_system_var=True)])
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)

        sql = "set autocommit=1, global sql_mode=concat(@@sql_mode, ',STRICT_TRANS_TABLES'), NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
        ast = parse_sql(sql)
        expected_ast = Set(
            set_list=[
                Set(name=Identifier('autocommit'), value=Constant(1)),
                Set(name=Identifier('sql_mode'),
                    scope='global',
                    value=Function(op='concat', args=[
                        Variable('sql_mode', is_system_var=True),
                        Constant(',STRICT_TRANS_TABLES')
                    ])
                ),
                Set(category="NAMES",
                    value=Constant('utf8mb4', with_quotes=False),
                    params={'COLLATE': Constant('utf8mb4_unicode_ci', with_quotes=False)})
            ]
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)

