import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.utils import to_single_line


class TestApplyPredictor:
    def test_apply_predictor(self):
        sql = """
        apply predictor aa using (select * from sales)
         into table=integr.predicted_sales
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = ApplyPredictor(
            name='aa',
            query_str="select * from sales",
            result_table=Identifier('integr.predicted_sales')
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()


