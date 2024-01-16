import pytest
from mindsdb_sql import parse_sql

from mindsdb_sql.parser.ast import *


class TestSql:
    def test_ending(self):
        sql = """INSERT INTO tbl_name VALUES (1, 3)  
           ;
        """

        parse_sql(sql, dialect='mindsdb')

    def test_not_equal(self):
        sql = " select * from t1 where a<>1"

        ast = parse_sql(sql, dialect='mindsdb')

        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier('t1'),
            where=BinaryOperation(
                op='<>',
                args=[
                    Identifier('a'),
                    Constant(1)
                ]
            )
        )

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_escaping(self):
        expected_ast = Select(
            targets=[Constant(value="a ' \" b")]
        )

        sql = """
          select 'a \\' \\" b'
        """

        ast = parse_sql(sql)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

        # in double quotes
        sql = """
          select "a \\' \\" b"
        """

        ast = parse_sql(sql)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()



