import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.utils import JoinType


class TestSpecificSelects:
    def test_select_from_predictors(self):
        sql = "SELECT * FROM predictors WHERE name = 'pred_name'"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier('predictors'),
            where=BinaryOperation('=', args=[Identifier('name'), Constant('pred_name')])
        )

        # assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_predict_column(self):
        sql = "SELECT predict FROM mindsdb.predictors"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('predict')],
            from_table=Identifier('mindsdb.predictors'),
        )

        # assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_status_column(self):
        sql = "SELECT status FROM mindsdb.predictors"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('status')],
            from_table=Identifier('mindsdb.predictors'),
        )

        # assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_native_query(self):
        sql = """
           SELECT status 
           FROM int1 (select q from p from r) 
           group by 1
           limit 1
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('status')],
            from_table=NativeQuery(
                integration=Identifier('int1'),
                query='select q from p from r'
            ),
            limit=Constant(1),
            group_by=[Constant(1)]
        )

        # assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_using(self):
        sql = """
           SELECT status FROM tbl1
           group by 1
           using p1=1, p2='2'
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('status')],
            from_table=Identifier('tbl1'),
            group_by=[Constant(1)],
            using={
                'p1': 1,
                'p2': '2'
            }
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


    def test_join_using(self):
        sql = """
           SELECT status FROM tbl1
           join pred1
           using p1=1, p2='2'
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('status')],
            from_table=Join(
                left=Identifier('tbl1'),
                right=Identifier('pred1'),
                join_type=JoinType.JOIN
            ),
            using={
                'p1': 1,
                'p2': '2'
            }
        )

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
