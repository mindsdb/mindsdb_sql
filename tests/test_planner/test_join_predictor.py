import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep, UnionStep)
from mindsdb_sql.utils import JoinType


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
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor='pred'),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='tab1'),
                                    right=Identifier('result_1', alias='pred'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['tab1.column1', 'pred.predicted']),
            ],
            results=[0, 1, 2],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_plan_aliases(self):
        query = Select(targets=[Identifier('ta.column1'), Identifier('tb.predicted')],
                       from_table=Join(left=Identifier('int.tab1', alias='ta'),
                                       right=Identifier('mindsdb.pred', alias='tb'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1', alias='ta')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), alias='tb', predictor='pred'),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='ta'),
                                    right=Identifier('result_1', alias='tb'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['ta.column1', 'tb.predicted']),
            ],
            results=[0, 1, 2],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

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
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor='pred'),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='tab'),
                                    right=Identifier('result_1', alias='pred'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['tab.column1', 'pred.predicted']),
            ],
            results=[0, 1, 2],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_error_when_filtering_on_predictions(self):
        """
        Query:
        SELECT rental_price_confidence
        FROM postgres_90.test_data.home_rentals AS ta
        JOIN mindsdb.hrp3 AS tb
        WHERE ta.sqft > 1000 AND tb.rental_price_confidence > 0.5
        LIMIT 5;
        """

        query = Select(targets=[Identifier('rental_price_confidence')],
                       from_table=Join(left=Identifier('postgres_90.test_data.home_rentals', alias='ta'),
                                       right=Identifier('mindsdb.hrp3', alias='tb'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('>', args=[Identifier('ta.sqft'), Constant(1000)]),
                           BinaryOperation('>', args=[Identifier('tb.rental_price_confidence'), Constant(0.5)]),
                       ]),
                       limit=5
                       )

        with pytest.raises(PlanningException):
            plan_query(query, integrations=['postgres_90'], predictor_namespace='mindsdb')

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
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor='pred'),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='tab'),
                                    right=Identifier('result_1', alias='pred'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['tab.asset', 'tab.time', 'pred.predicted']),
            ],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

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
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor='pred'),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='tab'),
                                    right=Identifier('result_1', alias='pred'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['tab.column1', 'pred.predicted']),
            ],
            results=[0, 1, 2],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

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
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor='pred'),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='tab'),
                                    right=Identifier('result_1', alias='pred'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['tab.column1', 'pred.predicted']),
            ],
            results=[0, 1, 2],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_plan_predictor_alias(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred_alias.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred', alias='pred_alias'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )
        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor='pred', alias='pred_alias', dataframe=Result(0)),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0', alias='tab1'),
                                    right=Identifier('result_1', alias='pred_alias'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=['tab1.column1', 'pred_alias.predicted']),
            ],
            results=[0, 1, 2],
            result_refs={0: [1, 2], 1: [2], 2: [3]},
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_no_predictor_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('pred'),
                                       join_type=None,
                                       implicit=True)
                       )

        with pytest.raises(PlanningException):
            plan = plan_query(query, integrations=['int'])

    def test_join_predictor_timeseries_latest(self):
        predictor_window = 5

        query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),
                           BinaryOperation('=', args=[Identifier('tab1.asset'), Constant('bitcoin')]),
                       ]),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1'),
                                                where=BinaryOperation('=', args=[Identifier('tab1.asset'), Constant('bitcoin')]),
                                                order_by=[OrderBy(Identifier('tab1.time'), direction='DESC')],
                                                limit=Constant(predictor_window)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor='pred', dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=['pred.time', 'pred.price']),
            ],
            result_refs={0: [1], 1: [2]},
        )

        plan = plan_query(query,
                          integrations=['int'],
                          predictor_namespace='mindsdb',
                          predictor_metadata={
                              'pred': {'timeseries': True,
                                       'order_by_column': 'time',
                                       'group_by_column': 'asset',
                                       'window': predictor_window}
                          })

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_timeseries_between(self):
        predictor_window = 5

        query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BetweenOperation(args=[Identifier('tab1.time'), Constant(100), Constant(300)]),
                           BinaryOperation('=', args=[Identifier('tab1.asset'), Constant('bitcoin')]),
                       ]),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1'),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('<', args=[Identifier('tab1.time'),
                                                                               Constant(100)]),
                                                    BinaryOperation('=', args=[Identifier('tab1.asset'),
                                                                               Constant('bitcoin')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('tab1.time'), direction='DESC')],
                                                limit=Constant(predictor_window)
                                                )
                                   ),
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1'),
                                                where=BinaryOperation('and', args=[
                                                    BetweenOperation(args=[Identifier('tab1.time'), Constant(100), Constant(300)]),
                                                    BinaryOperation('=', args=[Identifier('tab1.asset'),
                                                                               Constant('bitcoin')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('tab1.time'), direction='DESC')],
                                                limit=Constant(predictor_window)
                                                )
                                   ),
                UnionStep(left=Result(0), right=Result(1)),
                ApplyPredictorStep(namespace='mindsdb', predictor='pred', dataframe=Result(2)),
                ProjectStep(dataframe=Result(3), columns=['pred.time', 'pred.price']),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4]},
        )

        plan = plan_query(query,
                          integrations=['int'],
                          predictor_namespace='mindsdb',
                          predictor_metadata={
                              'pred': {'timeseries': True,
                                       'order_by_column': 'time',
                                       'group_by_column': 'asset',
                                       'window': predictor_window}
                          })

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_timeseries_error_on_nested_where(self):
        query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('and', args=[BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]), BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),]),
                           BinaryOperation('=', args=[Identifier('tab1.asset'), Constant('bitcoin')]),
                       ]),
                       )

        with pytest.raises(PlanningException):
            plan_query(query,
                       integrations=['int'],
                       predictor_namespace='mindsdb',
                       predictor_metadata={
                           'pred': {'timeseries': True,
                                    'order_by_column': 'time',
                                    'group_by_column': 'asset',
                                    'window': 5}
                       })

    def test_join_predictor_timeseries_error_on_invalid_column_in_where(self):
        query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),
                           BinaryOperation('=', args=[Identifier('tab1.whatver'), Constant(0)]),
                       ]),
                       )

        with pytest.raises(PlanningException):
            plan_query(query,
                       integrations=['int'],
                       predictor_namespace='mindsdb',
                       predictor_metadata={
                           'pred': {'timeseries': True,
                                    'order_by_column': 'time',
                                    'group_by_column': 'asset',
                                    'window': 5}
                       })

    def test_join_predictor_timeseries_real_world_query(self):
        predictor_window = 10
        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour > LATEST"
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias='ta'),
                                       right=Identifier('mindsdb.tp3', alias='tb'),
                                       join_type='join'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                           BinaryOperation('>', args=[Identifier('ta.pickup_hour'), Latest()]),
                       ]),
                       )

        assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias='ta'),
                                                where=BinaryOperation('=', args=[Identifier('ta.vendor_id'),
                                                                                 Constant(1)]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                limit=Constant(10)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor='tp3', alias='tb', dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=['*']),
            ],
            result_refs={0: [1], 1: [2]},
        )

        plan = plan_query(query,
                          integrations=['mysql'],
                          predictor_namespace='mindsdb',
                          predictor_metadata={
                              'tp3': {'timeseries': True,
                                       'order_by_column': 'pickup_hour',
                                       'group_by_column': 'vendor_id',
                                       'window': predictor_window}
                          })

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

