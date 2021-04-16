import pytest

from sql_parser.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation
from sql_parser.exceptions import ParsingException
from sql_parser.lexer import SQLLexer
from sql_parser.parser import SQLParser


class TestParser:
    def test_select_constant(self):
        for value in [1, 1.0, 'string']:
            sql = f'SELECT {value}' if not isinstance(value, str) else f"SELECT \"{value}\""
            tokens = SQLLexer().tokenize(sql)
            ast = SQLParser().parse(tokens)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Constant)
            assert ast.targets[0].value == value
            assert str(ast) == sql

    def test_select_identifier(self):
        sql = f'SELECT column'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'
        assert str(ast) == sql

    def test_select_identifier_alias(self):
        sql = f'SELECT column AS column_alias'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'
        assert ast.targets[0].alias == 'column_alias'
        assert str(ast) == sql

    def test_select_multiple_identifiers(self):
        sql = f'SELECT column1, column2'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 2
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column1'
        assert isinstance(ast.targets[1], Identifier)
        assert ast.targets[1].value == 'column2'
        assert str(ast) == sql

    def test_select_from_table(self):
        sql = f'SELECT column FROM table'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert str(ast) == sql


    def test_select_multiple_from_table(self):
        sql = f'SELECT column1, column2, 1 AS renamed_constant FROM table'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 3
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column1'
        assert ast.targets[1].value == 'column2'
        assert ast.targets[2].value == 1
        assert ast.targets[2].alias == 'renamed_constant'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert str(ast) == sql

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

    def test_select_where(self):
        sql = f'SELECT column FROM table WHERE column != 1'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == '!='

        assert str(ast) == sql

    def test_select_where_and(self):
        sql = f'SELECT column FROM table WHERE column != 1 AND column > 10'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == 'AND'

        assert isinstance(ast.where.args[0], BinaryOperation)
        assert ast.where.args[0].op == '!='
        assert isinstance(ast.where.args[1], BinaryOperation)
        assert ast.where.args[1].op == '>'
        assert str(ast) == sql

    def test_select_where_must_be_an_op(self):
        sql = f'SELECT column FROM table WHERE column'

        with pytest.raises(ParsingException) as excinfo:
            tokens = SQLLexer().tokenize(sql)
            ast = SQLParser().parse(tokens)

        assert "WHERE must contain an operation that evaluates to a boolean" in str(excinfo.value)

    def test_select_group_by(self):
        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)
        assert str(ast) == sql

        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1, column2'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == '!='

        assert isinstance(ast.group_by, list)
        assert isinstance(ast.group_by[0], Identifier)
        assert ast.group_by[0].value == 'column1'
        assert isinstance(ast.group_by[1], Identifier)
        assert ast.group_by[1].value == 'column2'

        assert str(ast) == sql

    def test_select_having(self):
        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)
        assert str(ast) == sql

        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1, column2 HAVING column1 > 10'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)

        assert isinstance(ast, Select)

        assert isinstance(ast.having, BinaryOperation)
        assert isinstance(ast.having.args[0], Identifier)
        assert ast.having.args[0].value == 'column1'
        assert ast.having.args[1].value == 10

        assert str(ast) == sql