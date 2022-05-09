from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.parser.utils import JoinType
from mindsdb_sql.planner.ts_utils import validate_ts_where_condition


class TestTimeSeries:
    def test_latest_in_where(self):
        sql = "SELECT time, price FROM crypto INNER JOIN pred WHERE time > LATEST"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = Select(
            targets=[Identifier('time'), Identifier('price')],
            from_table=Join(left=Identifier('crypto'),
                            right=Identifier('pred'),
                            join_type=JoinType.INNER_JOIN),
            where=BinaryOperation('>', args=[Identifier('time'), Latest()]),
        )

        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_multi_groupby(self):
        sql = """SELECT m.saledate as date, m.MA as forecast FROM mindsdb.home_sales_model as m 
                 JOIN files.HR_MA as t WHERE t.saledate > LATEST AND t.type = 'house' AND t.bedrooms = 2 LIMIT 4;"""
        ast = parse_sql(sql, dialect='mindsdb')
        allowed_cols = ['type', 'bedrooms',  # groupby
                        'saledate']          # orderby
        validate_ts_where_condition(ast.where, allowed_columns=allowed_cols)
