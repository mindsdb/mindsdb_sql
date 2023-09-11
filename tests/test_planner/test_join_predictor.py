import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query
from mindsdb_sql.planner.query_plan import QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep, FilterStep,
                                       LimitOffsetStep, GroupByStep, SubSelectStep, ApplyPredictorRowStep)
from mindsdb_sql.parser.utils import JoinType
from mindsdb_sql import parse_sql


class TestPlanJoinPredictor:
    def test_join_predictor_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab1.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        

    def test_predictor_namespace_is_case_insensitive(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab1.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='MINDSDB', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    def test_join_predictor_plan_aliases(self):
        query = Select(targets=[Identifier('ta.column1'), Identifier('tb.predicted')],
                       from_table=Join(left=Identifier('int.tab1', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.pred', alias=Identifier('tb')),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1', alias=Identifier('ta'))),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred', alias=Identifier('tb'))),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('ta.column1'), Identifier('tb.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    def test_join_predictor_plan_where(self):
        query = Select(targets=[Identifier('tab.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('tab.product_id'), Constant('x')]),
                           BetweenOperation(args=[Identifier('tab.time'), Constant('2021-01-01'), Constant('2021-01-31')]),
                       ])
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab'),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('=',
                                                                    args=[Identifier('tab.product_id'), Constant('x')]),
                                                    BetweenOperation(
                                                        args=[Identifier('tab.time'),
                                                              Constant('2021-01-01'),
                                                              Constant('2021-01-31')]),
                                                ])
                                                ),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    # def test_join_predictor_error_when_filtering_on_predictions(self):
    #     """
    #     Query:
    #     SELECT rental_price_confidence
    #     FROM postgres_90.test_data.home_rentals AS ta
    #     JOIN mindsdb.hrp3 AS tb
    #     WHERE ta.sqft > 1000 AND tb.rental_price_confidence > 0.5
    #     LIMIT 5;
    #     """
    #
    #     query = Select(targets=[Identifier('rental_price_confidence')],
    #                    from_table=Join(left=Identifier('postgres_90.test_data.home_rentals', alias=Identifier('ta')),
    #                                    right=Identifier('mindsdb.hrp3', alias=Identifier('tb')),
    #                                    join_type=JoinType.INNER_JOIN,
    #                                    implicit=True),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('>', args=[Identifier('ta.sqft'), Constant(1000)]),
    #                        BinaryOperation('>', args=[Identifier('tb.rental_price_confidence'), Constant(0.5)]),
    #                    ]),
    #                    limit=5
    #                    )
    #
    #     with pytest.raises(PlanningException):
    #         plan_query(query, integrations=['postgres_90'], predictor_namespace='mindsdb', predictor_metadata={'hrp3': {}})

    def test_join_predictor_plan_group_by(self):
        query = Select(targets=[Identifier('tab.asset'), Identifier('tab.time'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True),
                       group_by=[Identifier('tab.asset')],
                       having=BinaryOperation('=', args=[Identifier('tab.asset'), Constant('bitcoin')])
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab'),
                                                group_by=[Identifier('tab.asset')],
                                                having=BinaryOperation('=', args=[Identifier('tab.asset'),
                                                                                  Constant('bitcoin')])
                                                ),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab.asset'), Identifier('tab.time'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    def test_join_predictor_plan_limit_offset(self):
        query = Select(targets=[Identifier('tab.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True),
                       where=BinaryOperation('=', args=[Identifier('tab.product_id'), Constant('x')]),
                       limit=Constant(10),
                       offset=Constant(15),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab'),
                                                where=BinaryOperation('=', args=[Identifier('tab.product_id'), Constant('x')]),
                                                limit=Constant(10),
                                                offset=Constant(15),
                                                ),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    def test_join_predictor_plan_order_by(self):
        query = Select(targets=[Identifier('tab.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True),
                       where=BinaryOperation('=', args=[Identifier('tab.product_id'), Constant('x')]),
                       limit=Constant(10),
                       offset=Constant(15),
                       order_by=[OrderBy(field=Identifier('tab.column1'))]
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab'),
                                                where=BinaryOperation('=', args=[Identifier('tab.product_id'), Constant('x')]),
                                                limit=Constant(10),
                                                offset=Constant(15),
                                                order_by=[OrderBy(field=Identifier('tab.column1'))],
                                                ),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    def test_join_predictor_plan_predictor_alias(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred_alias.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred', alias=Identifier('pred_alias')),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('pred', alias=Identifier('pred_alias')), dataframe=Result(0)),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab1.column1'), Identifier('pred_alias.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        assert plan.steps == expected_plan.steps
        

    def test_no_predictor_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('pred'),
                                       join_type=None,
                                       implicit=True)
                       )

        with pytest.raises(PlanningException):
            plan = plan_query(query, integrations=['int'], predictor_metadata={'pred': {}})

    def test_join_predictor_plan_default_namespace_integration(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            default_namespace='int',
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab1.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', default_namespace='int', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_join_predictor_plan_default_namespace_predictor(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            default_namespace='mindsdb',
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('tab1.column1'), Identifier('pred.predicted')]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', default_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_nested_select(self):
        # for tableau

        sql = f'''
            SELECT time
            FROM ( 
               select * from int.covid 
               join mindsdb.pred 
               limit 10
            ) `Custom SQL Query`
            limit 1
         '''

        query = parse_sql(sql, dialect='mindsdb')

        expected_plan = QueryPlan(
            default_namespace='mindsdb',
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select * from covid limit 10')),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.JOIN)),
                SubSelectStep(dataframe=Result(2), query=parse_sql('SELECT time limit 1'), table_name='Custom SQL Query'),
                LimitOffsetStep(dataframe=Result(3), limit=1)
            ],
        )

        plan = plan_query(
            query,
            integrations=['int'],
            predictor_namespace='mindsdb',
            default_namespace='mindsdb',
            predictor_metadata={'pred': {}}
        )
        for i in range(len(plan.steps)):

            assert plan.steps[i] == expected_plan.steps[i]

        sql = f'''
                 SELECT `time`
                 FROM (
                   select * from int.covid
                   join mindsdb.pred
                 ) `Custom SQL Query`
                GROUP BY 1
            '''

        query = parse_sql(sql, dialect='mindsdb')

        expected_plan = QueryPlan(
            default_namespace='mindsdb',
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select * from covid')),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.JOIN)),
                SubSelectStep(dataframe=Result(2),
                              query=Select(targets=[Identifier('time')], group_by=[Constant(1)]),
                              table_name='Custom SQL Query'),
            ],
        )

        plan = plan_query(
            query,
            integrations=['int'],
            predictor_namespace='mindsdb',
            default_namespace='mindsdb',
            predictor_metadata={'pred': {}}
        )
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_subselect(self):

        # nested limit is greater
        sql = f'''
               SELECT *
               FROM ( 
                  select col from int.covid 
                  limit 10
               ) as t
               join mindsdb.pred 
               limit 5
            '''

        query = parse_sql(sql, dialect='mindsdb')

        expected_plan = QueryPlan(
            default_namespace='mindsdb',
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select covid.col as col from covid limit 10')),
                SubSelectStep(query=Select(targets=[Star()]), dataframe=Result(0), table_name='t'),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(1), predictor=Identifier('pred')),
                JoinStep(left=Result(1), right=Result(2),
                         query=Join(left=Identifier('tab1'),
                                    right=Identifier('tab2'),
                                    join_type=JoinType.JOIN)),
                LimitOffsetStep(dataframe=Result(3), limit=5),
                ProjectStep(dataframe=Result(4), columns=[Star()])
            ],
        )

        plan = plan_query(
            query,
            integrations=['int'],
            predictor_namespace='mindsdb',
            default_namespace='mindsdb',
            predictor_metadata={'pred': {}}
        )
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


        # only nested select with limit
        sql = f'''
               SELECT *
               FROM ( 
                  select * from int.covid
                  join int.info
                  limit 5
               ) as t
               join mindsdb.pred 
            '''

        query = parse_sql(sql, dialect='mindsdb')

        expected_plan = QueryPlan(
            default_namespace='mindsdb',
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select * from covid join info limit 5')),
                SubSelectStep(query=Select(targets=[Star()]), dataframe=Result(0), table_name='t'),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(1), predictor=Identifier('pred')),
                JoinStep(left=Result(1), right=Result(2),
                         query=Join(left=Identifier('tab1'),
                                    right=Identifier('tab2'),
                                    join_type=JoinType.JOIN)),
                ProjectStep(dataframe=Result(3), columns=[Star()])
            ],
        )

        plan = plan_query(
            query,
            integrations=['int'],
            predictor_namespace='mindsdb',
            default_namespace='mindsdb',
            predictor_metadata={'pred': {}}
        )
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]



class TestPredictorWithUsing:
    def test_using_join(self):

        sql = '''
            select * from int.tab1
            join mindsdb.pred
            using a=1
        '''

        query = parse_sql(sql, dialect='mindsdb')
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select * from tab1', dialect='mindsdb')),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0),
                                   predictor=Identifier('pred'), params={'a': 1}),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # with native query

        sql = '''
                    select * from int (select * from tab1) t
                    join mindsdb.pred
                    using a=1
                '''

        query = parse_sql(sql, dialect='mindsdb')
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int', raw_query='select * from tab1'),
                SubSelectStep(step_num=1, references=[], query=Select(targets=[Star()]),
                              dataframe=Result(0), table_name='t'),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(1),
                                   predictor=Identifier('pred'), params={'a': 1}),
                JoinStep(left=Result(1), right=Result(2),
                         query=Join(left=Identifier('tab1'),
                                    right=Identifier('tab2'),
                                    join_type=JoinType.JOIN)),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


    def test_using_one_line(self):

        sql = '''
            select * from mindsdb.pred where x=2 using a=1
        '''

        query = parse_sql(sql, dialect='mindsdb')
        expected_plan = QueryPlan(
            steps=[
                ApplyPredictorRowStep(namespace='mindsdb', predictor=Identifier('pred'),
                                      row_dict={'x': 2}, params={'a': 1}),
                ProjectStep(dataframe=Result(0), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


class TestPredictorVersion:
    def test_using_join(self):

        sql = '''
            select * from int.tab1
            join proj.pred.1
            using a=1
        '''

        query = parse_sql(sql, dialect='mindsdb')
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select * from tab1', dialect='mindsdb')),
                ApplyPredictorStep(namespace='proj', dataframe=Result(0),
                                   predictor=Identifier('pred.1'), params={'a': 1}),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Star()]),
            ],
        )

        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # default namespace

        sql = '''
            select * from int.tab1
            join pred.1
            using a=1
        '''
        query = parse_sql(sql, dialect='mindsdb')

        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb',
                          default_namespace='proj', predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_where_using(self):

        sql = '''
            select * from int.tab1 a
            join proj.pred.1 p
            where a.x=1 and p.x=1 and a.y=3 and p.y=''
        '''

        query = parse_sql(sql, dialect='mindsdb')
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=parse_sql('select * from tab1 as a where a.x=1 and a.y=3', dialect='mindsdb')),
                ApplyPredictorStep(namespace='proj', dataframe=Result(0),
                                   predictor=Identifier('pred.1', alias=Identifier('p'))),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.JOIN)),
                FilterStep(dataframe=Result(2), query=BinaryOperation(op='and', args=[
                    BinaryOperation(op='=', args=[
                        Identifier(parts=['p', 'x']),
                        Constant(1)
                    ]),
                    BinaryOperation(op='=', args=[
                        Identifier(parts=['p', 'y']),
                        Constant('')
                    ]),
                ])),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
        )

        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_using_one_line(self):

        sql = '''
            select * from proj.pred.1 where x=2 using a=1
        '''

        query = parse_sql(sql, dialect='mindsdb')
        expected_plan = QueryPlan(
            steps=[
                ApplyPredictorRowStep(namespace='proj', predictor=Identifier('pred.1'),
                                      row_dict={'x': 2}, params={'a': 1}),
                ProjectStep(dataframe=Result(0), columns=[Star()]),
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # default namespace

        sql = '''
             select * from pred.1 where x=2 using a=1
        '''
        query = parse_sql(sql, dialect='mindsdb')

        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb',
                          default_namespace='proj', predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

