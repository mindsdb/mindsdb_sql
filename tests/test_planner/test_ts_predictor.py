import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, ApplyPredictorStep,
                                       LimitOffsetStep, MapReduceStep, MultipleSteps, JoinStep)
from mindsdb_sql.utils import JoinType


class TestJoinTimeseriesPredictor:
    def test_join_predictor_timeseries(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Identifier(parts=['ta', group_by_column], alias=Identifier(group_by_column))],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                distinct=True,
                                                )
                                   ),
                MapReduceStep(values=Result(0),
                              reduce='union',
                              step=FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant('$var')]),
                                                order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
                                                )
                                   ),
                              ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(1)),
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                ),
                                   ),
                JoinStep(left=Result(2),
                         right=Result(3),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_3', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(4), columns=[Star()]),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4], 4: [5]},
        )

        plan = plan_query(query,
                          integrations=['mysql'],
                          predictor_namespace='mindsdb',
                          predictor_metadata={
                              'tp3': {'timeseries': True,
                                       'order_by_column': 'pickup_hour',
                                       'group_by_column': group_by_column,
                                       'window': predictor_window}
                          })

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_timeseries_query_with_limit(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       limit=Constant(1000),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[
                                       Identifier(parts=['ta', group_by_column], alias=Identifier(group_by_column))],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                distinct=True,
                                                )
                                   ),
                MapReduceStep(values=Result(0),
                              reduce='union',
                              step=FetchDataframeStep(integration='mysql',
                                                      query=Select(targets=[Star()],
                                                                   from_table=Identifier('data.ny_output',
                                                                                         alias=Identifier('ta')),
                                                                   where=BinaryOperation('=', args=[
                                                                       Identifier('ta.vendor_id'), Constant('$var')]),
                                                                   order_by=[OrderBy(Identifier('ta.pickup_hour'),
                                                                                     direction='DESC')],
                                                                   )
                                                      ),
                              ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')),
                                   dataframe=Result(1)),
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                ),
                                   ),
                JoinStep(left=Result(2),
                         right=Result(3),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_3', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                LimitOffsetStep(dataframe=Result(4), limit=query.limit),
                ProjectStep(dataframe=Result(5), columns=[Star()]),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4], 4: [5], 5: [6]},
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

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_timeseries_filter_by_group_by_column(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='join'),
                       where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[
                                       Identifier(parts=['ta', group_by_column], alias=Identifier(group_by_column))],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                                                distinct=True,
                                                )
                                   ),
                MapReduceStep(values=Result(0),
                              reduce='union',
                              step=FetchDataframeStep(integration='mysql',
                                                      query=Select(targets=[Star()],
                                                                   from_table=Identifier('data.ny_output',
                                                                                         alias=Identifier('ta')),
                                                                   where=BinaryOperation('and', args=[
                                                                       BinaryOperation('=',
                                                                                       args=[Identifier('ta.vendor_id'),
                                                                                             Constant(1)]),
                                                                       BinaryOperation('=', args=[
                                                                           Identifier('ta.vendor_id'),
                                                                           Constant('$var')]),
                                                                   ]),
                                                                   order_by=[OrderBy(Identifier('ta.pickup_hour'),
                                                                                     direction='DESC')],
                                                                   )
                                                      ),
                              ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')),
                                   dataframe=Result(1)),
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                                                ),
                                   ),
                JoinStep(left=Result(2),
                         right=Result(3),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_3', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(4), columns=[Star()]),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4], 4: [5]},
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

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_timeseries_latest(self):
        predictor_window = 5
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('>', args=[Identifier('ta.pickup_hour'), Latest()]),
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                       ]),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[
                                       Identifier(parts=['ta', group_by_column], alias=Identifier(group_by_column))],
                                       from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                       where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                                       distinct=True,
                                   )
                                   ),
                MapReduceStep(values=Result(0),
                              reduce='union',
                              step=FetchDataframeStep(integration='mysql',
                                                      query=Select(targets=[Star()],
                                                                   from_table=Identifier('data.ny_output',
                                                                                         alias=Identifier('ta')),
                                                                   where=BinaryOperation('and', args=[
                                                                       BinaryOperation('=',
                                                                                       args=[Identifier('ta.vendor_id'),
                                                                                             Constant(1)]),
                                                                       BinaryOperation('=', args=[
                                                                           Identifier('ta.vendor_id'),
                                                                           Constant('$var')]),
                                                                   ]),
                                                                   order_by=[OrderBy(Identifier('ta.pickup_hour'),
                                                                                     direction='DESC')],
                                                                   limit=Constant(predictor_window),
                                                                   )
                                                      ),
                              ),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')),
                                   dataframe=Result(1)),
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('=',
                                                                      args=[Identifier('ta.vendor_id'), Constant(1)]),
                                                ),
                                   ),
                JoinStep(left=Result(2),
                         right=Result(3),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_3', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(4), columns=[Star()]),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4], 4: [5]},
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

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        assert plan.result_refs == expected_plan.result_refs

    def test_join_predictor_timeseries_between(self):
        predictor_window = 5
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BetweenOperation(args=[Identifier('ta.pickup_hour'), Constant(1), Constant(10)]),
                           BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                       ]),
                       )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[
                                       Identifier(parts=['ta', group_by_column], alias=Identifier(group_by_column))],
                                       from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                       where=BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
                                       distinct=True,
                                   )
                                   ),
                MapReduceStep(values=Result(0),
                              reduce='union',
                              step=MultipleSteps(
                                  reduce='union',
                                  steps=[
                                      FetchDataframeStep(integration='mysql',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('data.ny_output',
                                                                                            alias=Identifier('ta')),
                                                                      where=BinaryOperation('and', args=[
                                                                          BinaryOperation('and', args=[
                                                                              BinaryOperation('<',
                                                                                              args=[Identifier(
                                                                                                  'ta.pickup_hour'),
                                                                                                  Constant(1)]),
                                                                              BinaryOperation('=',
                                                                                              args=[Identifier(
                                                                                                  'ta.vendor_id'),
                                                                                                  Constant(1)]),
                                                                          ]),
                                                                          BinaryOperation('=', args=[
                                                                              Identifier('ta.vendor_id'),
                                                                              Constant('$var')])
                                                                      ]),
                                                                      order_by=[OrderBy(Identifier('ta.pickup_hour'),
                                                                                        direction='DESC')],
                                                                      limit=Constant(predictor_window),
                                                                      ),
                                                         ),
                                      FetchDataframeStep(integration='mysql',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('data.ny_output',
                                                                                            alias=Identifier('ta')),
                                                                      where=BinaryOperation('and', args=[
                                                                          BinaryOperation('and', args=[
                                                                              BetweenOperation(
                                                                                  args=[Identifier('ta.pickup_hour'),
                                                                                        Constant(1), Constant(10)]),
                                                                              BinaryOperation('=',
                                                                                              args=[Identifier(
                                                                                                  'ta.vendor_id'),
                                                                                                  Constant(1)]),
                                                                          ]),

                                                                          BinaryOperation('=', args=[
                                                                              Identifier('ta.vendor_id'),
                                                                              Constant('$var')])
                                                                      ]),
                                                                      order_by=[OrderBy(Identifier('ta.pickup_hour'),
                                                                                        direction='DESC')],
                                                                      ),
                                                         ),

                                  ]
                              )),
                ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')),
                                   dataframe=Result(1)),
                FetchDataframeStep(integration='mysql',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                                                where=BinaryOperation('and', args=[
                                                    BetweenOperation(
                                                        args=[Identifier('ta.pickup_hour'), Constant(1), Constant(10)]),
                                                    BinaryOperation('=',
                                                                    args=[Identifier('ta.vendor_id'), Constant(1)]),
                                                ]),
                                                ),
                                   ),
                JoinStep(left=Result(2),
                         right=Result(3),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_3', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(4), columns=[Star()]),
            ],
            result_refs={0: [1], 1: [2], 2: [3], 3: [4], 4: [5]},
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

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        assert plan.result_refs == expected_plan.result_refs

    #
    # def test_join_predictor_timeseries_error_on_nested_where(self):
    #     query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
    #                    from_table=Join(left=Identifier('int.tab1'),
    #                                    right=Identifier('mindsdb.pred'),
    #                                    join_type=None,
    #                                    implicit=True),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('and', args=[BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]), BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),]),
    #                        BinaryOperation('=', args=[Identifier('tab1.asset'), Constant('bitcoin')]),
    #                    ]),
    #                    )
    #
    #     with pytest.raises(PlanningException):
    #         plan_query(query,
    #                    integrations=['int'],
    #                    predictor_namespace='mindsdb',
    #                    predictor_metadata={
    #                        'pred': {'timeseries': True,
    #                                 'order_by_column': 'time',
    #                                 'group_by_column': 'asset',
    #                                 'window': 5}
    #                    })
    #
    # def test_join_predictor_timeseries_error_on_invalid_column_in_where(self):
    #     query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
    #                    from_table=Join(left=Identifier('int.tab1'),
    #                                    right=Identifier('mindsdb.pred'),
    #                                    join_type=None,
    #                                    implicit=True),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),
    #                        BinaryOperation('=', args=[Identifier('tab1.whatver'), Constant(0)]),
    #                    ]),
    #                    )
    #
    #     with pytest.raises(PlanningException):
    #         plan_query(query,
    #                    integrations=['int'],
    #                    predictor_namespace='mindsdb',
    #                    predictor_metadata={
    #                        'pred': {'timeseries': True,
    #                                 'order_by_column': 'time',
    #                                 'group_by_column': 'asset',
    #                                 'window': 5}
    #                    })
    #

    #
    # def test_join_predictor_timeseries_concrete_date_greater(self):
    #     predictor_window = 10
    #     sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour > '2016-06-30 21:59:10'"
    #     query = Select(targets=[Star()],
    #                    from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
    #                                    right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
    #                                    join_type='join'),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
    #                        BinaryOperation('>', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
    #                    ]),
    #                    )
    #
    #     assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()
    #
    #     expected_plan = QueryPlan(
    #         steps=[
    #             FetchDataframeStep(integration='mysql',
    #                                query=Select(targets=[Star()],
    #                                             from_table=Identifier('data.ny_output', alias=Identifier('ta')),
    #                                             where=BinaryOperation('and', args=[
    #                                                 BinaryOperation('=', args=[Identifier('ta.vendor_id'),
    #                                                                            Constant(1)]),
    #                                                 BinaryOperation('<=', args=[Identifier('ta.pickup_hour'),
    #                                                                            Constant('2016-06-30 21:59:10')]),
    #                                             ]),
    #                                             order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
    #                                             limit=Constant(predictor_window)
    #                                             )
    #                                ),
    #             ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
    #             ProjectStep(dataframe=Result(1), columns=[Star()]),
    #         ],
    #         result_refs={0: [1], 1: [2]},
    #     )
    #
    #     plan = plan_query(query,
    #                       integrations=['mysql'],
    #                       predictor_namespace='mindsdb',
    #                       predictor_metadata={
    #                           'tp3': {'timeseries': True,
    #                                    'order_by_column': 'pickup_hour',
    #                                    'group_by_column': 'vendor_id',
    #                                    'window': predictor_window}
    #                       })
    #
    #     assert plan.steps == expected_plan.steps
    #     assert plan.result_refs == expected_plan.result_refs
    #
    # def test_join_predictor_timeseries_concrete_date_greater_or_equal(self):
    #     predictor_window = 10
    #     sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour >= '2016-06-30 21:59:10'"
    #     query = Select(targets=[Star()],
    #                    from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
    #                                    right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
    #                                    join_type='join'),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
    #                        BinaryOperation('>=', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
    #                    ]),
    #                    )
    #
    #     assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()
    #
    #     expected_plan = QueryPlan(
    #         steps=[
    #             FetchDataframeStep(integration='mysql',
    #                                query=Select(targets=[Star()],
    #                                             from_table=Identifier('data.ny_output', alias=Identifier('ta')),
    #                                             where=BinaryOperation('and', args=[
    #                                                 BinaryOperation('=', args=[Identifier('ta.vendor_id'),
    #                                                                            Constant(1)]),
    #                                                 BinaryOperation('<', args=[Identifier('ta.pickup_hour'),
    #                                                                            Constant('2016-06-30 21:59:10')]),
    #                                             ]),
    #                                             order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
    #                                             limit=Constant(predictor_window)
    #                                             )
    #                                ),
    #             ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
    #             ProjectStep(dataframe=Result(1), columns=[Star()]),
    #         ],
    #         result_refs={0: [1], 1: [2]},
    #     )
    #
    #     plan = plan_query(query,
    #                       integrations=['mysql'],
    #                       predictor_namespace='mindsdb',
    #                       predictor_metadata={
    #                           'tp3': {'timeseries': True,
    #                                    'order_by_column': 'pickup_hour',
    #                                    'group_by_column': 'vendor_id',
    #                                    'window': predictor_window}
    #                       })
    #
    #     assert plan.steps == expected_plan.steps
    #     assert plan.result_refs == expected_plan.result_refs
    #
    # def test_join_predictor_timeseries_concrete_date_less(self):
    #     predictor_window = 10
    #     sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour < '2016-06-30 21:59:10'"
    #     query = Select(targets=[Star()],
    #                    from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
    #                                    right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
    #                                    join_type='join'),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
    #                        BinaryOperation('<', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
    #                    ]),
    #                    )
    #
    #     assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()
    #
    #     expected_plan = QueryPlan(
    #         steps=[
    #             FetchDataframeStep(integration='mysql',
    #                                query=Select(targets=[Star()],
    #                                             from_table=Identifier('data.ny_output', alias=Identifier('ta')),
    #                                             where=BinaryOperation('and', args=[
    #                                                 BinaryOperation('=', args=[Identifier('ta.vendor_id'),
    #                                                                            Constant(1)]),
    #                                                 BinaryOperation('<', args=[Identifier('ta.pickup_hour'),
    #                                                                            Constant('2016-06-30 21:59:10')]),
    #                                             ]),
    #                                             order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
    #                                             )
    #                                ),
    #             ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
    #             ProjectStep(dataframe=Result(1), columns=[Star()]),
    #         ],
    #         result_refs={0: [1], 1: [2]},
    #     )
    #
    #     plan = plan_query(query,
    #                       integrations=['mysql'],
    #                       predictor_namespace='mindsdb',
    #                       predictor_metadata={
    #                           'tp3': {'timeseries': True,
    #                                    'order_by_column': 'pickup_hour',
    #                                    'group_by_column': 'vendor_id',
    #                                    'window': predictor_window}
    #                       })
    #
    #     assert plan.steps == expected_plan.steps
    #     assert plan.result_refs == expected_plan.result_refs
    #
    # def test_join_predictor_timeseries_concrete_date_less_or_equal(self):
    #     predictor_window = 10
    #     sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.vendor_id = 1 and ta.pickup_hour <= '2016-06-30 21:59:10'"
    #     query = Select(targets=[Star()],
    #                    from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
    #                                    right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
    #                                    join_type='join'),
    #                    where=BinaryOperation('and', args=[
    #                        BinaryOperation('=', args=[Identifier('ta.vendor_id'), Constant(1)]),
    #                        BinaryOperation('<=', args=[Identifier('ta.pickup_hour'), Constant('2016-06-30 21:59:10')]),
    #                    ]),
    #                    )
    #
    #     assert parse_sql(sql, dialect='mindsdb').to_tree() == query.to_tree()
    #
    #     expected_plan = QueryPlan(
    #         steps=[
    #             FetchDataframeStep(integration='mysql',
    #                                query=Select(targets=[Star()],
    #                                             from_table=Identifier('data.ny_output', alias=Identifier('ta')),
    #                                             where=BinaryOperation('and', args=[
    #                                                 BinaryOperation('=', args=[Identifier('ta.vendor_id'),
    #                                                                            Constant(1)]),
    #                                                 BinaryOperation('<=', args=[Identifier('ta.pickup_hour'),
    #                                                                            Constant('2016-06-30 21:59:10')]),
    #                                             ]),
    #                                             order_by=[OrderBy(Identifier('ta.pickup_hour'), direction='DESC')],
    #                                             )
    #                                ),
    #             ApplyPredictorStep(namespace='mindsdb', predictor=Identifier('tp3', alias=Identifier('tb')), dataframe=Result(0)),
    #             ProjectStep(dataframe=Result(1), columns=[Star()]),
    #         ],
    #         result_refs={0: [1], 1: [2]},
    #     )
    #
    #     plan = plan_query(query,
    #                       integrations=['mysql'],
    #                       predictor_namespace='mindsdb',
    #                       predictor_metadata={
    #                           'tp3': {'timeseries': True,
    #                                    'order_by_column': 'pickup_hour',
    #                                    'group_by_column': 'vendor_id',
    #                                    'window': predictor_window}
    #                       })
    #
    #     assert plan.steps == expected_plan.steps
    #     assert plan.result_refs == expected_plan.result_refs
    #
