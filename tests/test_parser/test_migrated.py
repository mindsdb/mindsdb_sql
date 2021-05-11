import pytest

from sql_parser.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation, OrderBy
from sql_parser.ast.operation import Function
from sql_parser.exceptions import ParsingException
from sql_parser.lexer import SQLLexer
from sql_parser.parser import SQLParser


def parse_sql(sql):
    tokens = SQLLexer().tokenize(sql)
    ast = SQLParser().parse(tokens)
    return ast


class TestMigrated:


    def test_multiple_selects(self):
        query = f"""SELECT 1; SELECT 2"""
        with pytest.raises(ParsingException):
            parse_sql(query)

    def test_unary_operations(self):
        unary_operations = ['-', 'NOT']
        for op in unary_operations:
            query = f"""SELECT {op} column1"""
            assert str(parse_sql(query)) == query
            assert str(parse_sql(query)) == str(Select(targets=[UnaryOperation(op=op, args_=(Identifier("column1"), ))],))

    def test_binary_operations(self):
        unary_operations = ['AND', 'OR', '=', '<>',  '-', '+', '*', '/', '%', '^', '<', '>', '>=', '<=',]
        for op in unary_operations:
            query = f"""SELECT column1 {op} column2"""
            assert str(parse_sql(query)) == query
            assert str(parse_sql(query)) == str(Select(targets=[BinaryOperation(op=op, args_=(Identifier("column1"), Identifier("column2")))],))

    def test_deep_binary_operation(self):
        query = f"""SELECT column1 AND column2 AND column3"""
        assert str(parse_sql(query)) == query
        assert str(parse_sql(query)) == str(
            Select(targets=[BinaryOperation(op='AND', args_=(Identifier("column1"),
                                                             BinaryOperation(op='AND', args_=(
                                                                Identifier("column2"), Identifier("column3")))
                                                             ))]))

    def test_operation_priority(self):
        query = f"""SELECT column1 AND column2 OR column3"""
        assert str(parse_sql(query)) == query
        assert str(parse_sql(query)) == str(
            Select(targets=[BinaryOperation(op='OR', args_=(BinaryOperation(op='AND', args_=(
                Identifier("column1"), Identifier("column2"))),
                                                            Identifier("column3"))
                                            )]))

        query = f"""SELECT column1 AND (column2 OR column3)"""
        assert str(parse_sql(query)) == str(
            Select(targets=[BinaryOperation(op='AND', args_=(Identifier("column1"),
                                                             BinaryOperation(op='OR', args_=(
                                                                 Identifier("column2"), Identifier("column3"))),
                                                             )
                                            )]))

    def test_unary_comparison_predicates(self):
        ops = ['IS NULL', 'IS NOT NULL', 'IS TRUE', 'IS FALSE']
        for op in ops:
            query = f"""SELECT column1 {op}"""
            assert str(parse_sql(query)) == query
            assert str(parse_sql(query)) == str(Select(targets=[ComparisonPredicate(op=op, args_=(Identifier("column1"),))],))

    def test_aggregate_functions(self):
        functions = AGGREGATE_FUNCTIONS
        for op in functions:
            query = f"""SELECT {op.name}(column1)"""
            assert str(parse_sql(query)) == query
            assert str(parse_sql(query)) == str(
                Select(targets=[Function(op=op.name, args_=(Identifier("column1"),))]))

    def test_unsupported_aggregate_function(self):
        query = f"""SELECT mode(column1)"""
        with pytest.raises(ParsingException):
            parse_sql(query)

    def test_custom_aggregate_function(self):
        query = f"""SELECT mode(column1)"""

        class CustomFunc:
            pass

        custom_functions = {
            'mode': CustomFunc
        }

        assert str(parse_sql(query, custom_functions=custom_functions)) == query

    def test_cast(self):
        query = f"""SELECT CAST(4 as int64) as result"""

        assert str(parse_sql(query)) == str(
                Select(targets=[TypeCast(type_name='int64', arg=Constant(4), alias='result')]))

    def test_lists(self):
        query = "SELECT col FROM tab WHERE col IN (1, 2)"

        assert str(parse_sql(query)) == str(Select(targets=[Identifier('col')],
                                                   from_table=[Identifier('tab')],
                                                   where=InOperation(args_=(
                                                       Identifier('col'),
                                                       List(items=[Constant(1), Constant(2)])
                                                   ))))

    def test_parse_from_with_dots(self):
        query = "SELECT 1 FROM schema.table"

        assert str(parse_sql(query)) == str(Select(
            targets=[Constant(1)],
            from_table=[Identifier('schema.table')]
        ))
