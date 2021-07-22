import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.utils import JoinType


class TestCreateView:
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
