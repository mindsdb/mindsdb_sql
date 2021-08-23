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


class TestJoinTimeseriesPredictor:
    def test_join_predictor_timeseries(self):
        predictor_window = 10
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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

    def test_join_predictor_timeseries_filter_by_group_by_column(self):
        predictor_window = 10
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('pred'), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Identifier('pred.time'), Identifier('pred.price')]),
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
                                                order_by=[OrderBy(Identifier('tab1.time'), direction='DESC')]
                                                )
                                   ),
                UnionStep(left=Result(0), right=Result(1)),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('pred'), dataframe=Result(2)),
                ProjectStep(dataframe=Result(3), columns=[Identifier('pred.time'), Identifier('pred.price')]),
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

    def test_join_predictor_timeseries_between_real_query(self):
        predictor_window = 5

        sql = """
            SELECT tb.trip_duration, tb.pickup_datetime 
            FROM singlestore2.data.ny_taxy_train AS ta 
            JOIN mindsdb.ny_p_2 AS tb 
            WHERE ta.vendor_id IN ('1', '2') AND ta.pickup_datetime BETWEEN '2016-06-30 21:59:10' AND '2016-06-30 23:59:10'
        """

        expected_query = Select(targets=[Identifier('tb.trip_duration'), Identifier('tb.pickup_datetime')],
                       from_table=Join(left=Identifier('singlestore2.data.ny_taxy_train', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.ny_p_2', alias=Identifier('tb')),
                                       join_type=JoinType.JOIN,
                                       implicit=False),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('in', args=[Identifier('ta.vendor_id'),  Tuple(items=[Constant('1'), Constant('2')])]),
                           BetweenOperation(args=[Identifier('ta.pickup_datetime'), Constant('2016-06-30 21:59:10'), Constant('2016-06-30 23:59:10')]),
                       ]),
                       )
        query = parse_sql(sql, dialect='mindsdb')
        assert query.to_tree() == expected_query.to_tree()

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='singlestore2',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_taxy_train', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('in', args=[Identifier('ta.vendor_id'),
                                                                                Tuple(items=[Constant('1'),
                                                                                             Constant('2')])]),
                                                    BinaryOperation('<', args=[Identifier('ta.pickup_datetime'),
                                                                               Constant('2016-06-30 21:59:10')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('ta.pickup_datetime'), direction='DESC')],
                                                limit=Constant(predictor_window)
                                                )
                                   ),
                FetchDataframeStep(integration='singlestore2',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_taxy_train', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('in', args=[Identifier('ta.vendor_id'),
                                                                                Tuple(items=[Constant('1'),
                                                                                             Constant('2'),
                                                                                             ]
                                                                                      )
                                                                                ]),
                                                    BetweenOperation(args=[Identifier('ta.pickup_datetime'),
                                                                           Constant('2016-06-30 21:59:10'),
                                                                           Constant('2016-06-30 23:59:10'),
                                                                           ]),
                                                ]),
                                                order_by=[OrderBy(Identifier('ta.pickup_datetime'), direction='DESC')],
                                                )
                                   ),
                UnionStep(left=Result(0), right=Result(1)),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('ny_p_2', alias=Identifier('tb')), dataframe=Result(2)),
                ProjectStep(dataframe=Result(3), columns=[Identifier('tb.trip_duration'), Identifier('tb.pickup_datetime')]),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4]},
        )

        plan = plan_query(query,
                          integrations=['singlestore2'],
                          predictor_namespace='mindsdb',
                          predictor_metadata={
                              'ny_p_2': {'timeseries': True,
                                       'order_by_column': 'pickup_datetime',
                                       'group_by_column': 'vendor_id',
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

    def test_join_predictor_timeseries_real_world_query_latest(self):
        predictor_window = 10
        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour > LATEST"
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
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
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('=', args=[Identifier('ta.vendor_id'),
                                                                                 Constant(1)]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                limit=Constant(10)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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

    def test_join_predictor_timeseries_concrete_date_greater(self):
        predictor_window = 10
        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour > '2016-06-30 21:59:10'"
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                           BinaryOperation('>', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
                       ]),
                       )

        assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('=', args=[Identifier('ta.vendor_id'),
                                                                               Constant(1)]),
                                                    BinaryOperation('<=', args=[Identifier('ta.pickup_hour'),
                                                                               Constant('2016-06-30 21:59:10')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                limit=Constant(predictor_window)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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

    def test_join_predictor_timeseries_concrete_date_greater_or_equal(self):
        predictor_window = 10
        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour >= '2016-06-30 21:59:10'"
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                           BinaryOperation('>=', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
                       ]),
                       )

        assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('=', args=[Identifier('ta.vendor_id'),
                                                                               Constant(1)]),
                                                    BinaryOperation('<', args=[Identifier('ta.pickup_hour'),
                                                                               Constant('2016-06-30 21:59:10')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                limit=Constant(predictor_window)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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

    def test_join_predictor_timeseries_concrete_date_less(self):
        predictor_window = 10
        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour < '2016-06-30 21:59:10'"
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                           BinaryOperation('<', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
                       ]),
                       )

        assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('=', args=[Identifier('ta.vendor_id'),
                                                                               Constant(1)]),
                                                    BinaryOperation('<', args=[Identifier('ta.pickup_hour'),
                                                                               Constant('2016-06-30 21:59:10')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                # limit=Constant(predictor_window)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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

    def test_join_predictor_timeseries_concrete_date_less_or_equal(self):
        predictor_window = 10
        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour <= '2016-06-30 21:59:10'"
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                           BinaryOperation('<=', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
                       ]),
                       )

        assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BinaryOperation('=', args=[Identifier('ta.vendor_id'),
                                                                               Constant(1)]),
                                                    BinaryOperation('<=', args=[Identifier('ta.pickup_hour'),
                                                                               Constant('2016-06-30 21:59:10')]),
                                                ]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                # limit=Constant(predictor_window)
                                                )
                                   ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
                ProjectStep(dataframe=Result(1), columns=[Star()]),
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