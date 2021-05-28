import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation, NullConstant
from mindsdb_sql.ast.operation import Function, Operation
from mindsdb_sql.ast.tuple import Tuple
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.lexer import SQLLexer
from mindsdb_sql.parser import SQLParser


class TestOperations:
    def test_select_binary_operations(self):
        for op in ['+', '-', '/', '*', '%', '=', '!=', '>', '<', '>=', '<=',
                   'IS', 'IS NOT', 'LIKE', 'IN', 'AND', 'OR', '||']:
            sql = f'SELECT column1 {op} column2 FROM tab'
            ast = parse_sql(sql)

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

    def test_operator_precedence_sum_mult(self):
        sql = f'SELECT column1 + column2 * column3'
        ast = parse_sql(sql)

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
        ast = parse_sql(sql)

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


    def test_operator_precedence_sum_mult_parentheses(self):
        sql = f'SELECT (column1 + column2) * column3'
        ast = parse_sql(sql)

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

    def test_operator_chained_and(self):
        sql = f"""SELECT column1 AND column2 AND column3"""
        ast = parse_sql(sql)

        expected_ast = Select(targets=[BinaryOperation(op='AND', args=(BinaryOperation(op='AND', args=(
                                                                           Identifier("column1"),
                                                                           Identifier("column2"))),
                                                                            Identifier("column3"),

                                                                       ))])

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_or_and(self):
        sql = f'SELECT column1 OR column2 AND column3'
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[BinaryOperation(op='OR',
                                     args=(Identifier('column1'),
                                           BinaryOperation(op='AND',
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

        sql = f'SELECT column1 AND column2 OR column3'
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[BinaryOperation(op='OR',
                                     args=(
                                         BinaryOperation(op='AND',
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

    def test_operator_precedence_or_and_parentheses(self):
        sql = f'SELECT (column1 OR column2) AND column3'
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[BinaryOperation(op='AND',
                                     args=(
                                         BinaryOperation(op='OR',
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

    def test_where_and_or_precedence(self):
        sql = "SELECT col1 FROM tab WHERE col1 AND col2 OR col3"
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[Identifier('col1')],
            from_table=Identifier('tab'),
            where=BinaryOperation(op='OR',
                                  args=(
                                      BinaryOperation(op='AND',
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

        sql = "SELECT col1 FROM tab WHERE col1 = 1 AND col2 = 1 OR col3 = 1"
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[Identifier('col1')],
            from_table=Identifier('tab'),
            where=BinaryOperation(op='OR',
                                  args=(
                                      BinaryOperation(op='AND',
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


    def test_select_unary_operations(self):
        for op in ['-', 'NOT']:
            sql = f'SELECT {op} column FROM table'
            ast = parse_sql(sql)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], UnaryOperation)
            assert ast.targets[0].op == op
            assert len(ast.targets[0].args) == 1
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].value == 'column'

            assert str(ast) == sql

    def test_select_function_no_args(self):
        sql = f'SELECT database() FROM table'
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[Function(op='database', args=tuple())],
            from_table=Identifier('table'),
        )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_function_one_arg(self):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column) FROM table'
            ast = parse_sql(sql)

            expected_ast = Select(
                targets=[Function(op=func, args=(Identifier('column'),))],
                from_table=Identifier('table'),
            )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_select_function_two_args(self):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column1, column2) FROM table'
            ast = parse_sql(sql)

            expected_ast = Select(
                targets=[Function(op=func, args=(Identifier('column1'),Identifier('column2')))],
                from_table=Identifier('table'),
            )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_select_in_operation(self):
        sql = """SELECT * FROM t1 WHERE col1 IN ("a", "b")"""

        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert ast.where

        expected_where = BinaryOperation(op='IN',
                                         args=[
                                             Identifier('col1'),
                                             Tuple(items=[Constant('a'), Constant("b")]),
                                         ])
        assert ast.where == expected_where

    def test_unary_is_special_values(self):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 IS {sql_arg}"""
            ast = parse_sql(sql)

            expected_ast = Select(targets=[BinaryOperation(op='IS', args=(Identifier("column1"), python_obj))], )

            assert str(ast) == sql
            assert ast.to_tree() == expected_ast.to_tree()

    def test_unary_is_not_special_values(self):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 IS NOT {sql_arg}"""
            ast = parse_sql(sql)

            expected_ast = Select(targets=[BinaryOperation(op='IS NOT', args=(Identifier("column1"), python_obj))], )

            assert str(ast) == sql
            assert ast.to_tree() == expected_ast.to_tree()
            assert str(ast) == str(expected_ast)

    def test_not_in(self):
        sql = f"""SELECT column1 NOT IN column2"""
        ast = parse_sql(sql)

        expected_ast = Select(targets=[BinaryOperation(op='NOT IN', args=(Identifier("column1"), Identifier("column2")))], )

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_null(self):
        sql = "SELECT col1 FROM t1 WHERE col1 IS NULL"
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('IS', args=(Identifier('col1'), NullConstant())))

        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_not_null(self):
        sql = "SELECT col1 FROM t1 WHERE col1 IS NOT NULL"
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('IS NOT', args=(Identifier('col1'), NullConstant())))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_true(self):
        sql = "SELECT col1 FROM t1 WHERE col1 IS TRUE"
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('IS', args=(Identifier('col1'), Constant(True))))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_false(self):
        sql = "SELECT col1 FROM t1 WHERE col1 IS FALSE"
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier("col1")], from_table=Identifier('t1'),
                              where=BinaryOperation('IS', args=(Identifier('col1'), Constant(False))))
        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)
