import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.utils import to_single_line


class TestCreatePredictor:
    def test_create_predictor_full(self):
        sql = """CREATE PREDICTOR pred
                FROM integration_name 
                WITH 'select * FROM table'
                AS ds_name
                PREDICT f1 as f1_alias, f2
                ORDER BY f_order_1 ASC, f_order_2, f_order_3 DESC
                GROUP BY f_group_1, f_group_2
                WINDOW 100
                HORIZON 7
                USING {"x": 1, "y": "a"}
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('pred'),
            integration_name=Identifier('integration_name'),
            query='select * FROM table',
            datasource_name=Identifier('ds_name'),
            targets=[Identifier('f1', alias=Identifier('f1_alias')),
                             Identifier('f2')],
            order_by=[OrderBy(Identifier('f_order_1'), direction='ASC'),
                      OrderBy(Identifier('f_order_2'), direction='default'),
                      OrderBy(Identifier('f_order_3'), direction='DESC'),
                      ],
            group_by=[Identifier('f_group_1'), Identifier('f_group_2')],
            window=100,
            horizon=7,
            using=dict(x=1, y="a"),
        )
        assert str(ast).lower() == to_single_line(sql.lower())
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_predictor_minimal(self):
        sql = """CREATE PREDICTOR pred
                FROM integration_name 
                WITH 'select * FROM table'
                AS ds_name
                PREDICT f1 as f1_alias, f2
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('pred'),
            integration_name=Identifier('integration_name'),
            query='select * FROM table',
            datasource_name=Identifier('ds_name'),
            targets=[Identifier('f1', alias=Identifier('f1_alias')),
                             Identifier('f2')],
        )
        assert str(ast).lower() == to_single_line(sql.lower())
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_predictor_quotes(self):
        sql = """CREATE PREDICTOR xxx 
                 FROM `yyy` 
                  WITH 'SELECT * FROM zzz'
                  AS x 
                  PREDICT sss
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('xxx'),
            integration_name=Identifier('yyy'),
            query='SELECT * FROM zzz',
            datasource_name=Identifier('x'),
            targets=[Identifier('sss')],
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_predictor_invalid_json(self):
        sql = """CREATE PREDICTOR pred
                FROM integration_name 
                WITH 'select * FROM table'
                AS ds_name
                PREDICT f1 as f1_alias, f2
                ORDER BY f_order_1 ASC, f_order_2, f_order_3 DESC
                GROUP BY f_group_1, f_group_2
                WINDOW 100
                HORIZON 7
                USING 'not_really_json'"""

        with pytest.raises(ParsingException):
            parse_sql(sql, dialect='mindsdb')
