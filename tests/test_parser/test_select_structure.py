import itertools

import pytest

from sql_parser.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation
from sql_parser.ast.operation import Function
from sql_parser.ast.order_by import OrderBy
from sql_parser.exceptions import ParsingException
from sql_parser.lexer import SQLLexer
from sql_parser.parser import SQLParser



class TestSelectStructure:
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

    def test_from_table_raises_duplicate(self):
        sql = f'SELECT column FROM table FROM table'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

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

    def test_where_raises_nofrom(self):
        sql = f'SELECT column WHERE column != 1'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_where_raises_duplicate(self):
        sql = f'SELECT column FROM table WHERE column != 1 WHERE column > 1'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_where_raises_as(self):
        sql = f'SELECT column FROM table WHERE column != 1 AS somealias'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

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

    def test_group_by_raises_duplicate(self):
        sql = f'SELECT column FROM table GROUP BY col GROUP BY col'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

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

    def test_having_raises_duplicate(self):
        sql = f'SELECT column FROM table GROUP BY col HAVING col > 1 HAVING col > 1'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_select_order_by(self):
        sql = f'SELECT column1 FROM table ORDER BY column2'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)
        assert str(ast) == sql

        assert len(ast.order_by) == 1
        assert isinstance(ast.order_by[0], OrderBy)
        assert isinstance(ast.order_by[0].field, Identifier)
        assert ast.order_by[0].field.value == 'column2'
        assert ast.order_by[0].direction == 'default'

        sql = f'SELECT column1 FROM table ORDER BY column2, column3 ASC, column4 DESC'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)
        assert str(ast) == sql

        assert len(ast.order_by) == 3

        assert isinstance(ast.order_by[0], OrderBy)
        assert isinstance(ast.order_by[0].field, Identifier)
        assert ast.order_by[0].field.value == 'column2'
        assert ast.order_by[0].direction == 'default'

        assert isinstance(ast.order_by[1], OrderBy)
        assert isinstance(ast.order_by[1].field, Identifier)
        assert ast.order_by[1].field.value == 'column3'
        assert ast.order_by[1].direction == 'ASC'

        assert isinstance(ast.order_by[2], OrderBy)
        assert isinstance(ast.order_by[2].field, Identifier)
        assert ast.order_by[2].field.value == 'column4'
        assert ast.order_by[2].direction == 'DESC'

    def test_order_by_raises_duplicate(self):
        sql = f'SELECT column FROM table ORDER BY col1 ORDER BY col1'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_select_limit_offset(self):
        sql = f'SELECT column FROM table LIMIT 5 OFFSET 3'
        tokens = SQLLexer().tokenize(sql)
        ast = SQLParser().parse(tokens)
        assert str(ast) == sql

        assert ast.limit == Constant(value=5)
        assert ast.offset == Constant(value=3)

    def test_select_limit_offset_raises_nonint(self):
        sql = f'SELECT column FROM table OFFSET 3.0'
        tokens = SQLLexer().tokenize(sql)

        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

        sql = "SELECT column FROM table LIMIT \"string\""
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_select_limit_offset_raises_wrong_order(self):
        sql = f'SELECT column FROM table OFFSET 3 LIMIT 5 '
        tokens = SQLLexer().tokenize(sql)

        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_limit_raises_duplicate(self):
        sql = f'SELECT column FROM table LIMIT 1 LIMIT 1'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_offset_raises_duplicate(self):
        sql = f'SELECT column FROM table OFFSET 1 OFFSET 1'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_limit_raises_before_order_by(self):
        sql = f'SELECT column FROM table LIMIT 1 ORDER BY column ASC'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_offset_raises_before_order_by(self):
        sql = f'SELECT column FROM table OFFSET 1 ORDER BY column ASC'
        tokens = SQLLexer().tokenize(sql)
        with pytest.raises(ParsingException):
            ast = SQLParser().parse(tokens)

    def test_select_order(self):
        components = ['FROM table',
                      'WHERE column = 1',
                      'GROUP BY column',
                      'HAVING column != 2',
                      'ORDER BY column ASC',
                      'LIMIT 1',
                      'OFFSET 1']

        good_sql = 'SELECT column ' + '\n'.join(components)
        tokens = SQLLexer().tokenize(good_sql)
        ast = SQLParser().parse(tokens)
        assert ast

        for perm in itertools.permutations(components):
            bad_sql = 'SELECT column ' + '\n'.join(perm)
            if bad_sql == good_sql:
                continue

            tokens = SQLLexer().tokenize(bad_sql)
            with pytest.raises(ParsingException) as excinfo:
                ast = SQLParser().parse(tokens)
            assert 'must go after' in str(excinfo.value) or ' requires ' in str(excinfo.value)
