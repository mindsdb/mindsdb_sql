import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.utils import to_single_line


class TestCreatePredictor:
    def test_create_predictor_full(self):
        sql = """CREATE predictor pred
                FROM integration_name 
                (selct * FROM not some actually ( {'t': [1,2.1,[], {}, False, true, null]} ) not sql (name))
                PREDICT f1 as f1_alias, f2
                ORDER BY f_order_1 ASC, f_order_2, f_order_3 DESC
                GROUP BY f_group_1, f_group_2
                WINDOW 100
                HORIZON 7
                USING 
                    a=null, b=true, c=false,
                    x.`part 2`.part3=1, 
                    y= "a", 
                    z=0.7,
                    j={'t': [1,2.1,[], {}, False, true, null]},
                    q=Filter(x=null, y=true, z=false, a='c', b=2, j={"ar": [1], 'j': {"d": "d"}})
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('pred'),
            integration_name=Identifier('integration_name'),
            query_str="selct * FROM not some actually ( {'t': [1,2.1,[], {}, False, true, null]} ) not sql (name)",
            targets=[Identifier('f1', alias=Identifier('f1_alias')),
                             Identifier('f2')],
            order_by=[OrderBy(Identifier('f_order_1'), direction='ASC'),
                      OrderBy(Identifier('f_order_2'), direction='default'),
                      OrderBy(Identifier('f_order_3'), direction='DESC'),
                      ],
            group_by=[Identifier('f_group_1'), Identifier('f_group_2')],
            window=100,
            horizon=7,
            using={
                'a': None, 'b': True, 'c': False,
                'x.part 2.part3': 1,
                'y': "a",
                'z': 0.7,
                'j': {'t': [1,2.1,[], {}, False, True, None]},
                'q': Object(type='Filter', params={
                    'x': None, 'y': True, 'z': False,
                    'a': 'c',
                    'b': 2,
                    'j': {"ar": [1], 'j': {"d": "d"}}
                })
            },
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

        # convert to string and parse again
        ast2 = parse_sql(str(ast), dialect='mindsdb')
        assert ast.to_tree() == ast2.to_tree()

    def test_create_predictor_minimal(self):
        sql = """CREATE PREDICTOR IF NOT EXISTS pred
                FROM integration_name 
                (select * FROM table_name)
                PREDICT f1 as f1_alias, f2
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('pred'),
            if_not_exists=True,
            integration_name=Identifier('integration_name'),
            query_str="select * FROM table_name",
            targets=[Identifier('f1', alias=Identifier('f1_alias')),
                             Identifier('f2')],
        )
        assert str(ast).lower() == to_single_line(sql.lower())
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_predictor_no_with(self):
        sql = """CREATE PREDICTOR pred
                FROM integration_name 
                (select * FROM table_name)
                PREDICT f1 as f1_alias, f2
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('pred'),
            integration_name=Identifier('integration_name'),
            query_str="select * FROM table_name",
            targets=[Identifier('f1', alias=Identifier('f1_alias')),
                             Identifier('f2')],
        )
        assert ast.to_tree() == expected_ast.to_tree()

        # test create model
        sql = """CREATE model pred
                FROM integration_name 
                (select * FROM table_name)
                PREDICT f1 as f1_alias, f2
                """
        ast = parse_sql(sql, dialect='mindsdb')

        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_predictor_quotes(self):
        sql = """CREATE PREDICTOR xxx 
                 FROM `yyy` 
                  (SELECT * FROM zzz)
                  PREDICT sss
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('xxx'),
            integration_name=Identifier('yyy'),
            query_str="SELECT * FROM zzz",
            targets=[Identifier('sss')],
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

        # # or replace
        # sql = """CREATE or REPLACE PREDICTOR xxx
        #                  FROM `yyy`
        #                   (SELECT * FROM zzz)
        #                   AS x
        #                   PREDICT sss
        #                 """
        # ast = parse_sql(sql, dialect='mindsdb')
        # expected_ast.is_replace = True
        # assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        # assert ast.to_tree() == expected_ast.to_tree()

    def test_create_predictor_invalid_json(self):
        sql = """CREATE PREDICTOR pred
                FROM integration_name 
                (select * FROM table)
                AS ds_name
                PREDICT f1 as f1_alias, f2
                ORDER BY f_order_1 ASC, f_order_2, f_order_3 DESC
                GROUP BY f_group_1, f_group_2
                WINDOW 100
                HORIZON 7
                USING 'not_really_json'"""

        with pytest.raises(ParsingException):
            parse_sql(sql, dialect='mindsdb')

    def test_create_predictor_empty_fields(self):
        sql = """CREATE PREDICTOR xxx 
                PREDICT sss
                """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreatePredictor(
            name=Identifier('xxx'),
            integration_name=None,
            query_str=None,
            targets=[Identifier('sss')],
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()

    def test_create_anomaly_detection_model(self):
        for predict_clause in ["", " PREDICT alert "]:
            create_clause = """CREATE ANOMALY DETECTION MODEL alert_model """
            rest_clause = """
            FROM integration_name (select * FROM table)
            USING
                confidence=0.5
            """
            sql = create_clause + predict_clause + rest_clause
            ast = parse_sql(sql, dialect='mindsdb')

            expected_ast = CreateAnomalyDetectionModel(
                name=Identifier('alert_model'),
                integration_name=Identifier('integration_name'),
                query_str='select * FROM table',
                targets=[Identifier('alert')] if predict_clause else None,
                using={
                    'confidence': 0.5
                }
            )

            assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
            assert ast.to_tree() == expected_ast.to_tree()
