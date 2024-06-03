import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestUpdate:

    def test_update_simple(self, dialect):
        sql = "update tbl_name set a=b, c='a', d=2, e=f.g"

        expected_ast = Update(
            table=Identifier('tbl_name'),
            update_columns={
                'a': Identifier('b'),
                'c': Constant('a'),
                'd': Constant(2),
                'e': Identifier(parts=['f', 'g']),
            },
        )

        ast = parse_sql(sql, dialect=dialect)

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        sql += ' where a=b or c>1'

        expected_ast.where = BinaryOperation(op='or', args=[
            BinaryOperation(op='=', args=[
                Identifier('a'),
                Identifier('b')
            ]),
            BinaryOperation(op='>', args=[
                Identifier('c'),
                Constant(1)
            ])
        ])

        ast = parse_sql(sql, dialect=dialect)

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


@pytest.mark.parametrize('dialect', ['mindsdb'])
class TestUpdateFromSelect:

    def test_update_simple(self, dialect):
        sql = """
            update 
                table2    
            set
                predicted = df.result
            from                         
             (
               select result, prod_id from table1
               USING  aaa = "bbb"
             ) as df
            where          
                table2.prod_id = df.prod_id             
        """

        expected_ast = Update(
            table=Identifier('table2'),
            update_columns={
                'predicted': Identifier('df.result')
            },
            from_select=Select(
                targets=[Identifier('result'), Identifier('prod_id')],
                from_table=Identifier('table1'),
                using={'aaa': 'bbb'}
            ),
            from_select_alias=Identifier('df'),
            where=BinaryOperation(op='=', args=[
                Identifier('table2.prod_id'),
                Identifier('df.prod_id')
            ])
        )

        ast = parse_sql(sql, dialect=dialect)

        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()




