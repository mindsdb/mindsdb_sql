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

        sql = "set autocommit = 1, sql_mode = concat(@@sql_mode, ',STRICT_TRANS_TABLES')"
        ast = parse_sql(sql)
        expected_ast = Set(
            arg=Tuple([
                BinaryOperation(op='=', args=[
                    Identifier('autocommit'), Constant(1)
                ]),
                BinaryOperation(op='=', args=[
                    Identifier('sql_mode'),
                    Function(op='concat', args=[
                        Variable('sql_mode', is_system_var=True),
                        Constant(',STRICT_TRANS_TABLES')
                    ])
                ])
            ])
        )
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)

