import itertools
import pytest
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.utils import JoinType


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestCommonTableExpression:

    def test_cte_select_number(self, dialect):
        sql = f'WITH one AS ( SELECT 1 ) SELECT * FROM one'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            cte=[
                CommonTableExpression(name=Identifier('one'), query=Select(targets=[Constant(1)])),
            ],
            targets=[Star()],
            from_table=Identifier('one')
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_cte_select_named_columns(self, dialect):
        sql = f'WITH cte( a, b ) AS ( SELECT 1, 2 ) SELECT a, b FROM cte'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            cte=[
                CommonTableExpression(name=Identifier('cte'),
                                      columns=[Identifier('a'), Identifier('b')],
                                      query=Select(targets=[Constant(1), Constant(2)])),
            ],
            targets=[Identifier('a'), Identifier('b')],
            from_table=Identifier('cte')
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_cte_multiple(self, dialect):
        sql = '''WITH cte_a AS ( SELECT 1 ), cte_b AS ( SELECT 2 ) SELECT * FROM cte_a, cte_b'''
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            cte=[
                CommonTableExpression(name=Identifier('cte_a'),
                                      query=Select(targets=[Constant(1)])),
                CommonTableExpression(name=Identifier('cte_b'),
                                      query=Select(targets=[Constant(2)])),
            ],
            targets=[Star()],
            from_table=Join(left=Identifier('cte_a'),
                            right=Identifier('cte_b'),
                            join_type=JoinType.INNER_JOIN,
                            implicit=True)
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_cte_nested(self, dialect):
        sql = '''WITH cte AS ( SELECT 1 ) SELECT * FROM (WITH cte_1 AS ( SELECT 2 ) SELECT * FROM cte_1 JOIN cte) AS subquery'''
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            cte=[
                CommonTableExpression(name=Identifier('cte'),
                                      query=Select(targets=[Constant(1)])),
            ],
            targets=[Star()],
            from_table=Select(
                alias=Identifier('subquery'),
                cte=[
                    CommonTableExpression(name=Identifier('cte_1'),
                                          query=Select(targets=[Constant(2)])),
                ],
                targets=[Star()],
                from_table=Join(left=Identifier('cte_1'),
                                right=Identifier('cte'),
                                join_type=JoinType.JOIN)
            ),
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
