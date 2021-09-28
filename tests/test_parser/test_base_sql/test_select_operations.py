import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation, NullConstant
from mindsdb_sql.parser.ast import Function, BetweenOperation
from mindsdb_sql.parser.ast import Tuple


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestOperations:
    def test_select_binary_operations(self, dialect):
        for op in ['+', '-', '/', '*', '%', '=', '!=', '>', '<', '>=', '<=',
                   'is', 'IS NOT', 'like', 'in', 'and', 'or', '||']:
            sql = f'SELECT column1 {op.upper()} column2 FROM tab'
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(
                targets=[BinaryOperation(op=op,
                                         args=(
                                             Identifier.from_path_str('column1'), Identifier.from_path_str('column2')
                                         )),
                         ],
                from_table=Identifier.from_path_str('tab')
            )

            assert str(ast).lower() == sql.lower()
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_operation_converts_to_lowercase(self, dialect):
        sql = f'SELECT column1 IS column2 FROM tab'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='is',
                                     args=(
                                         Identifier.from_path_str('column1'), Identifier.from_path_str('column2')
                                     )),
                     ],
            from_table=Identifier.from_path_str('tab')
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_sum_mult(self, dialect):
        sql = f'SELECT column1 + column2 * column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='+',
                                     args=(
                                         Identifier.from_path_str('column1'),
                                         BinaryOperation(op='*',
                                                         args=(
                                                             Identifier.from_path_str('column2'), Identifier.from_path_str('column3')
                                                         )),

                                     ),
                                     )
                     ]
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        sql = f'SELECT column1 * column2 + column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='+',
                                     args=(
                                         BinaryOperation(op='*',
                                                         args=(
                                                             Identifier.from_path_str('column1'), Identifier.from_path_str('column2')
                                                         )),
                                         Identifier.from_path_str('column3'),

                                     )
                                     )
                     ]
        )

        assert str(ast).lower() == sql.lower()
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
                                                             Identifier.from_path_str('column1'), Identifier.from_path_str('column2')
                                                         ),
                                                         parentheses=True),
                                         Identifier.from_path_str('column3'),

                                     ),
                                     )
                     ]
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_chained_and(self, dialect):
        sql = f"""SELECT column1 AND column2 AND column3"""
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[BinaryOperation(op='AND', args=(BinaryOperation(op='and', args=(
                                                                           Identifier.from_path_str("column1"),
                                                                           Identifier.from_path_str("column2"))),
                                                                            Identifier.from_path_str("column3"),

                                                                       ))])

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_or_and(self, dialect):
        sql = f'SELECT column1 OR column2 AND column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='or',
                                     args=(Identifier.from_path_str('column1'),
                                           BinaryOperation(op='and',
                                                           args=(
                                                               Identifier.from_path_str('column2'), Identifier.from_path_str('column3')
                                                           ))

                                           )
                                     )
                     ]
        )

        assert str(ast).lower() == sql.lower()
        assert ast == expected_ast
        assert ast.to_tree() == expected_ast.to_tree()

        sql = f'SELECT column1 AND column2 OR column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='or',
                                     args=(
                                         BinaryOperation(op='and',
                                                         args=(
                                                             Identifier.from_path_str('column1'), Identifier.from_path_str('column2')
                                                         )),
                                         Identifier.from_path_str('column3'),

                                     )
                                     )
                     ]
        )

        assert str(ast).lower() == sql.lower()
        assert ast == expected_ast
        assert ast.to_tree() == expected_ast.to_tree()

    def test_operator_precedence_or_and_parentheses(self, dialect):
        sql = f'SELECT (column1 OR column2) AND column3'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[BinaryOperation(op='and',
                                     args=(
                                         BinaryOperation(op='or',
                                                         args=(
                                                             Identifier.from_path_str('column1'), Identifier.from_path_str('column2')
                                                         ),
                                                         parentheses=True),
                                         Identifier.from_path_str('column3'),

                                     ),
                                     )
                     ]
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_where_and_or_precedence(self, dialect):
        sql = "SELECT col1 FROM tab WHERE col1 AND col2 OR col3"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Identifier.from_path_str('col1')],
            from_table=Identifier.from_path_str('tab'),
            where=BinaryOperation(op='or',
                                  args=(
                                      BinaryOperation(op='and',
                                                      args=(
                                                          Identifier.from_path_str('col1'),
                                                          Identifier.from_path_str('col2'),
                                                      )),
                                      Identifier.from_path_str('col3'),

                                  ))
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        sql = "SELECT col1 FROM tab WHERE col1 = 1 AND col2 = 1 OR col3 = 1"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Identifier.from_path_str('col1')],
            from_table=Identifier.from_path_str('tab'),
            where=BinaryOperation(op='or',
                                  args=(
                                      BinaryOperation(op='and',
                                                      args=(
                                                          BinaryOperation(op='=',
                                                                          args=(
                                                                              Identifier.from_path_str('col1'),
                                                                              Constant(1),
                                                                          )),
                                                          BinaryOperation(op='=',
                                                                          args=(
                                                                              Identifier.from_path_str('col2'),
                                                                              Constant(1),
                                                                          )),
                                                      )),
                                      BinaryOperation(op='=',
                                                      args=(
                                                          Identifier.from_path_str('col3'),
                                                          Constant(1),
                                                      )),

                                  ))
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_unary_operations(self, dialect):
        for op in ['-', 'not']:
            sql = f'SELECT {op} column FROM tab'
            ast = parse_sql(sql, dialect=dialect)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], UnaryOperation)
            assert ast.targets[0].op == op
            assert len(ast.targets[0].args) == 1
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].parts == ['column']

            assert str(ast).lower() == sql.lower()

    def test_select_function_no_args(self, dialect):
        sql = f'SELECT database() FROM tab'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Function(op='database', args=tuple())],
            from_table=Identifier.from_path_str('tab'),
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_function_one_arg(self, dialect):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column) FROM tab'
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(
                targets=[Function(op=func, args=(Identifier.from_path_str('column'),))],
                from_table=Identifier.from_path_str('tab'),
            )

            assert str(ast).lower() == sql.lower()
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_select_function_two_args(self, dialect):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column1, column2) FROM tab'
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(
                targets=[Function(op=func, args=(Identifier.from_path_str('column1'),Identifier.from_path_str('column2')))],
                from_table=Identifier.from_path_str('tab'),
            )

            assert str(ast).lower() == sql.lower()
            assert str(ast) == str(expected_ast)
            assert ast.to_tree() == expected_ast.to_tree()

    def test_select_in_operation(self, dialect):
        sql = """SELECT * FROM t1 WHERE col1 IN ("a", "b")"""

        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert ast.where

        expected_where = BinaryOperation(op='IN',
                                         args=[
                                             Identifier.from_path_str('col1'),
                                             Tuple(items=[Constant('a'), Constant("b")]),
                                         ])
        print(ast.where.to_tree())
        print(expected_where.to_tree())
        print(ast.where)
        print(expected_where)
        assert ast.where.to_tree() == expected_where.to_tree()
        assert ast.where == expected_where

    def test_unary_is_special_values(self, dialect):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 IS {sql_arg}"""
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(targets=[BinaryOperation(op='IS', args=(Identifier.from_path_str("column1"), python_obj))], )

            assert str(ast).lower() == sql.lower()
            assert ast.to_tree() == expected_ast.to_tree()

    def test_unary_is_not_special_values(self, dialect):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 IS NOT {sql_arg}"""
            ast = parse_sql(sql, dialect=dialect)

            expected_ast = Select(targets=[BinaryOperation(op='IS NOT', args=(Identifier.from_path_str("column1"), python_obj))], )

            assert str(ast).lower() == sql.lower()
            assert ast.to_tree() == expected_ast.to_tree()
            assert str(ast) == str(expected_ast)

    def test_not_in(self, dialect):
        sql = f"""SELECT column1 NOT   IN column2"""
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[BinaryOperation(op='not in', args=(Identifier.from_path_str("column1"), Identifier.from_path_str("column2")))], )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_null(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 IS NULL"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier.from_path_str("col1")], from_table=Identifier.from_path_str('t1'),
                              where=BinaryOperation('is', args=(Identifier.from_path_str('col1'), NullConstant())))

        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_not_null(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 IS NOT NULL"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier.from_path_str("col1")], from_table=Identifier.from_path_str('t1'),
                              where=BinaryOperation('IS NOT', args=(Identifier.from_path_str('col1'), NullConstant())))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_true(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 IS TRUE"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier.from_path_str("col1")], from_table=Identifier.from_path_str('t1'),
                              where=BinaryOperation('is', args=(Identifier.from_path_str('col1'), Constant(True))))
        assert ast.to_tree() == expected_ast.to_tree()

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_is_false(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 IS FALSE"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier.from_path_str("col1")], from_table=Identifier.from_path_str('t1'),
                              where=BinaryOperation('is', args=(Identifier.from_path_str('col1'), Constant(False))))
        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_between(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col1 BETWEEN a AND b"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier.from_path_str("col1")], from_table=Identifier.from_path_str('t1'),
                              where=BetweenOperation(args=(Identifier.from_path_str('col1'), Identifier.from_path_str('a'), Identifier.from_path_str('b'))))

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_between_with_and(self, dialect):
        sql = "SELECT col1 FROM t1 WHERE col2 > 1 AND col1 BETWEEN a AND b"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier.from_path_str("col1")],
                              from_table=Identifier.from_path_str('t1'),
                              where=BinaryOperation('and', args=[
                                  BinaryOperation('>', args=[
                                      Identifier('col2'),
                                      Constant(1),
                                  ]),
                                  BetweenOperation(args=(
                                      Identifier.from_path_str('col1'), Identifier.from_path_str('a'),
                                      Identifier.from_path_str('b'))),
                              ])
                              )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)

    def test_select_functions(self, dialect):
        sqls = [
            "SELECT connection_id()",
            "SELECT database()",
            "SELECT current_user()",
            "SELECT version()",
            "SELECT user()",
        ]

        for sql in sqls:
            ast = parse_sql(sql, dialect=dialect)
            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Function)

