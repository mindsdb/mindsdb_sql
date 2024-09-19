from textwrap import dedent
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
            targets=[
                Constant(value="a ' \" b"),
                Constant(value="a ' \" b"),
                Constant(value="a \\n b"),
                Constant(value="a \\\n b"),
                Constant(value="a \\\n b"),
                Constant(value="a\nb"),
            ]
        )

        sql = dedent('''
select
'a \\' \\" b',  -- double quote
"a \\' \\" b",  -- single quote
"a \\n b",
"a \\\n b", -- double quote
'a \\\n b', -- single quote
"a
b"
        ''')

        ast = parse_sql(sql)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

    def test_quotes_escaping(self):
        sql = "select 'women''s soccer'"

        expected_ast = Select(
            targets=[
                Constant(value="women's soccer")
            ]
        )
        ast = parse_sql(sql)

        assert str(ast).lower() == str(expected_ast).lower()
        assert ast.to_tree() == expected_ast.to_tree()

