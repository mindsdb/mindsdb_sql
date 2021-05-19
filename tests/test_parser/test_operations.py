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
            sql = f'SELECT column1 {op} column2 FROM table'
            ast = parse_sql(sql)

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

    def test_operator_chained_and(self):
        sql = f"""SELECT column1 AND column2 AND column3"""
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        expected_ast = Select(targets=[BinaryOperation(op='AND', args=(Identifier("column1"),
                                                                       BinaryOperation(op='AND', args=(
                                                                           Identifier("column2"),
                                                                           Identifier("column3")))
                                                                       ))])

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)

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
            ast = parse_sql(sql)

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
            ast = parse_sql(sql)

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
            ast = parse_sql(sql)

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

    def test_unary_is_special_values(self):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 IS {sql_arg}"""
            ast = parse_sql(sql)

            expected_ast = Select(targets=[BinaryOperation(op='IS', args=(Identifier("column1"), python_obj))], )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)

    def test_unary_is_not_special_values(self):
        args = [('NULL', NullConstant()), ('TRUE', Constant(value=True)), ('FALSE', Constant(value=False))]
        for sql_arg, python_obj in args:
            sql = f"""SELECT column1 IS NOT {sql_arg}"""
            ast = parse_sql(sql)

            expected_ast = Select(targets=[BinaryOperation(op='IS NOT', args=(Identifier("column1"), python_obj))], )

            assert str(ast) == sql
            assert str(ast) == str(expected_ast)

    def test_not_in(self):
        sql = f"""SELECT column1 NOT IN column2"""
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        expected_ast = Select(targets=[BinaryOperation(op='NOT IN', args=(Identifier("column1"), Identifier("column2")))], )

        assert str(ast) == sql
        assert str(ast) == str(expected_ast)
