import pytest

from sql_parser.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation
from sql_parser.ast.operation import Function, Operation
from sql_parser.ast.tuple import Tuple
from sql_parser.exceptions import ParsingException
from sql_parser.lexer import SQLLexer
from sql_parser.parser import SQLParser


class TestOperations:
    def test_select_binary_operations(self):
        for op in ['+', '-', '/', '*', '%', '=', '!=', '>', '<', '>=', '<=',
                   'IS', 'IS NOT', 'LIKE', 'IN', 'AND', 'OR', ]:
            sql = f'SELECT column1 {op} column2 FROM table'
            tokens = SQLLexer().tokenize(sql)
            ast = SQLParser().parse(tokens)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], BinaryOperation)
            assert ast.targets[0].op == op
            assert len(ast.targets[0].args) == 2
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].value == 'column1'
            assert isinstance(ast.targets[0].args[1], Identifier)
            assert ast.targets[0].args[1].value == 'column2'

            assert str(ast) == sql

    def test_operator_precedence_sum_mult(self):
        sql = f'SELECT column1 + column2 * column3 FROM table'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], BinaryOperation)
        assert ast.targets[0].op == '+'
        assert len(ast.targets[0].args) == 2
        assert isinstance(ast.targets[0].args[0], Identifier)
        assert ast.targets[0].args[0].value == 'column1'
        inner_op = ast.targets[0].args[1]
        assert isinstance(inner_op, BinaryOperation)
        assert len(inner_op.args) == 2
        assert inner_op.op == '*'
        assert inner_op.args[0].value == 'column2'
        assert inner_op.args[1].value == 'column3'
        assert str(ast) == sql

    def test_operator_precedence_sum_mult_parentheses(self):
        sql = f'SELECT (column1 + column2) * column3 FROM table'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], BinaryOperation)
        assert ast.targets[0].op == '*'
        assert len(ast.targets[0].args) == 2
        assert isinstance(ast.targets[0].args[1], Identifier)
        assert ast.targets[0].args[1].value == 'column3'

        inner_op = ast.targets[0].args[0]
        assert isinstance(inner_op, BinaryOperation)
        assert inner_op.parentheses == True
        assert len(inner_op.args) == 2
        assert inner_op.op == '+'
        assert inner_op.args[0].value == 'column1'
        assert inner_op.args[1].value == 'column2'
        assert str(ast) == sql

    def test_operator_precedence_or_and(self):
        sql = f'SELECT column1 OR column2 AND column3 FROM table'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], BinaryOperation)
        assert ast.targets[0].op == 'OR'
        assert len(ast.targets[0].args) == 2
        assert isinstance(ast.targets[0].args[0], Identifier)
        assert ast.targets[0].args[0].value == 'column1'
        inner_op = ast.targets[0].args[1]
        assert isinstance(inner_op, BinaryOperation)
        assert len(inner_op.args) == 2
        assert inner_op.op == 'AND'
        assert inner_op.args[0].value == 'column2'
        assert inner_op.args[1].value == 'column3'
        assert str(ast) == sql

    def test_operator_precedence_or_and_parentheses(self):
        sql = f'SELECT (column1 OR column2) AND column3 FROM table'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], BinaryOperation)
        assert ast.targets[0].op == 'AND'
        assert len(ast.targets[0].args) == 2
        assert isinstance(ast.targets[0].args[1], Identifier)
        assert ast.targets[0].args[1].value == 'column3'

        inner_op = ast.targets[0].args[0]
        assert isinstance(inner_op, BinaryOperation)
        assert inner_op.parentheses == True
        assert len(inner_op.args) == 2
        assert inner_op.op == 'OR'
        assert inner_op.args[0].value == 'column1'
        assert inner_op.args[1].value == 'column2'
        assert str(ast) == sql

    def test_select_unary_operations(self):
        for op in ['-', 'NOT']:
            sql = f'SELECT {op} column FROM table'
            tokens = SQLLexer().tokenize(sql)
            ast = SQLParser().parse(tokens)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], UnaryOperation)
            assert ast.targets[0].op == op
            assert len(ast.targets[0].args) == 1
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].value == 'column'

            assert str(ast) == sql

    def test_select_function_one_arg(self):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column) FROM table'
            tokens = SQLLexer().tokenize(sql)
            ast = SQLParser().parse(tokens)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Function)
            assert ast.targets[0].op == func
            assert len(ast.targets[0].args) == 1
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].value == 'column'

    def test_select_function_two_args(self):
        funcs = ['sum', 'min', 'max', 'some_custom_function']
        for func in funcs:
            sql = f'SELECT {func}(column1, column2) FROM table'
            tokens = SQLLexer().tokenize(sql)
            ast = SQLParser().parse(tokens)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Function)
            assert ast.targets[0].op == func
            assert len(ast.targets[0].args) == 2
            assert isinstance(ast.targets[0].args[0], Identifier)
            assert ast.targets[0].args[0].value == 'column1'
            assert isinstance(ast.targets[0].args[1], Identifier)
            assert ast.targets[0].args[1].value == 'column2'

    def test_select_in_operation(self):
        sql = """SELECT * FROM t1 WHERE col1 IN ("a", "b")"""

        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert ast.where

        expected_where = BinaryOperation(op='IN',
                                         args=[
                                             Identifier('col1'),
                                             Tuple(items=[Constant('a'), Constant("b")]),
                                         ])
        assert ast.where == expected_where
