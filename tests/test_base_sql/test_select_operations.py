import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation, NullConstant
from mindsdb_sql.ast.operation import Function, Operation, BetweenOperation
from mindsdb_sql.ast.tuple import Tuple
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.lexer import SQLLexer
from mindsdb_sql.parser import SQLParser


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql'])
class TestOperations:
    def test_select_binary_operations(self, dialect):
        for op in ['+', '-', '/', '*', '%', '=', '!=', '>', '<', '>=', '<=',
                   'is', 'is not', 'like', 'in', 'and', 'or', '||']:
            sql = f'SELECT column1 {op} column2 FROM tab'
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(
                targets=[BinaryOperation(op=op,
                                         args=(
                                             Identifier('column1'), Identifier('column2')
                                         )),
                         ],
                from_table=Identifier('tab')
            )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_operation_converts_to_lowercase(self, dialect):
        sql = f'SELECT column1 IS column2 FROM tab'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='is',
                                     args=(
                                         Identifier('column1'), Identifier('column2')
                                     )),
                     ],
            from_table=Identifier('tab')
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_sum_mult(self, dialect):
        sql = f'SELECT column1 + column2 * column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='+',
                                     args=(
                                         Identifier('column1'),
                                         BinaryOperation(op='*',
                                                         args=(
                                                             Identifier('column2'), Identifier('column3')
                                                         )),

                                     ),
                                     )
                     ]
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        sql = f'SELECT column1 * column2 + column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='+',
                                     args=(
                                         BinaryOperation(op='*',
                                                         args=(
                                                             Identifier('column1'), Identifier('column2')
                                                         )),
                                         Identifier('column3'),

                                     )
                                     )
                     ]
        )

        assert str(ast) == sql
        assert ast == expected_ast
        assert ast.to_tree() == expected_ast.to_tree()


    def test_operator_precedence_sum_mult_parentheses(self, dialect):
        sql = f'SELECT (column1 + column2) * column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='*',
                                     args=(
                                         BinaryOperation(op='+',
                                                         args=(
                                                             Identifier('column1'), Identifier('column2')
                                                         ),
                                                         parentheses=True),
                                         Identifier('column3'),

                                     ),
                                     )
                     ]
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_chained_and(self, dialect):
        sql = f"""SELECT column1 and column2 and column3"""
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[BinaryOperation(op='AND', args=(BinaryOperation(op='and', args=(
                                                                           Identifier("column1"),
                                                                           Identifier("column2"))),
                                                                            Identifier("column3"),

                                                                       ))])

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_or_and(self, dialect):
        sql = f'SELECT column1 or column2 and column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='or',
                                     args=(Identifier('column1'),
                                           BinaryOperation(op='and',
                                                           args=(
                                                               Identifier('column2'), Identifier('column3')
                                                           ))

                                           )
                                     )
                     ]
        )

        assert str(ast) == sql
        assert ast == expected_ast
        assert ast.to_tree() == expected_ast.to_tree()

        sql = f'SELECT column1 and column2 or column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='or',
                                     args=(
                                         BinaryOperation(op='and',
                                                         args=(
                                                             Identifier('column1'), Identifier('column2')
                                                         )),
                                         Identifier('column3'),

                                     )
                                     )
                     ]
        )

        assert str(ast) == sql
        assert ast == expected_ast
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_or_and_parentheses(self, dialect):
        sql = f'SELECT (column1 or column2) and column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='and',
                                     args=(
                                         BinaryOperation(op='or',
                                                         args=(
                                                             Identifier('column1'), Identifier('column2')
                                                         ),
                                                         parentheses=True),
                                         Identifier('column3'),

                                     ),
                                     )
                     ]
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_where_and_or_precedence(self, dialect):
        sql = "SELECT col1 FROM tab WHERE col1 and col2 or col3"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Identifier('col1')],
            from_table=Identifier('tab'),
            where=BinaryOperation(op='or',
                                  args=(
                                      BinaryOperation(op='and',
                                                      args=(
                                                          Identifier('col1'),
                                                          Identifier('col2'),
                                                      )),
                                      Identifier('col3'),

                                  ))
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        sql = "SELECT col1 FROM tab WHERE col1 = 1 and col2 = 1 or col3 = 1"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Identifier('col1')],
            from_table=Identifier('tab'),
            where=BinaryOperation(op='or',
                                  args=(
                                      BinaryOperation(op='and',
                                                      args=(
                                                          BinaryOperation(op='=',
                                                                          args=(
                                                                              Identifier('col1'),
                                                                              Constant(1),
                                                                          )),
                                                          BinaryOperation(op='=',
                                                                          args=(
                                                                              Identifier('col2'),
                                                                              Constant(1),
                                                                          )),
                                                      )),
                                      BinaryOperation(op='=',
                                                      args=(
                                                          Identifier('col3'),
                                                          Constant(1),
                                                      )),

                                  ))
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


    def test_select_unary_operations(self, dialect):
        for op in ['-', 'not']:
            sql = f'SELECT {op} column FROM table'
            ast = parse_sql(sql, dialect=dialect)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], UnaryOperation)
            assert ast.targets[0].op == op
            assert len(ast.targets[0].args) == 1
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].value == 'column'

            assert str(ast) == sql

    def test_select_function_no_args(self, dialect):
        sql = f'SELECT database() FROM table'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Function(op='database', args=tuple())],
            from_table=Identifier('table'),
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_function_one_arg(self, dialect):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column) FROM table'
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(
                targets=[Function(op=func, args=(Identifier('column'),))],
                from_table=Identifier('table'),
            )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_select_function_two_args(self, dialect):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column1, column2) FROM table'
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(
                targets=[Function(op=func, args=(Identifier('column1'),Identifier('column2')))],
                from_table=Identifier('table'),
            )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_select_in_operation(self, dialect):
        sql = """SELECT * FROM t1 WHERE col1 IN ("a", "b")"""

        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert ast.where

        expected_where = BinaryOperation(op='IN',
                                         args=[
                                             Identifier('col1'),
                                             Tuple(items=[Constant('a'), Constant("b")]),
                                         ])
        assert ast.where == expected_where

    def test_unary_is_special_values(self, dialect):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 is {sql_arg}"""
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(targets=[BinaryOperation(op='IS', args=(Identifier("column1"), python_obj))], )

            assert str(ast) == sql
            assert ast.to_tree() == expected_ast.to_tree()

    def test_unary_is_not_special_values(self, dialect):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 is not {sql_arg}"""
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(targets=[BinaryOperation(op='is not', args=(Identifier("column1"), python_obj))], )

            assert str(ast) == sql
            assert ast.to_tree() == expected_ast.to_tree()
            assert str(ast) == str(expected_ast)

    def test_not_in(self, dialect):
        sql = f"""SELECT column1 not   in column2"""
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[BinaryOperation(op='not in', args=(Identifier("column1"), Identifier("column2")))], )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_null(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 is NULL"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('is', args=(Identifier('col1'), NullConstant())))

        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_not_null(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 is not NULL"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('is not', args=(Identifier('col1'), NullConstant())))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_true(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 is TRUE"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('is', args=(Identifier('col1'), Constant(True))))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_false(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 is FALSE"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('is', args=(Identifier('col1'), Constant(False))))
        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_between(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 BETWEEN a AND b"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BetweenOperation(args=(Identifier('col1'), Identifier('a'), Identifier('b'))))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

