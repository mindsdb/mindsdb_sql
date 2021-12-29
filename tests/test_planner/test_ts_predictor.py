import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, ApplyTimeseriesPredictorStep,
                                       LimitOffsetStep, MapReduceStep, MultipleSteps, JoinStep)
from mindsdb_sql.utils import JoinType


class TestJoinTimeseriesPredictor:
    def test_join_predictor_timeseries(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='left join'),
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
                ApplyTimeseriesPredictorStep(namespace='mindsdb',
                                             predictor=Identifier('tp3', alias=Identifier('tb')),
                                             dataframe=Result(1)),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_select_table_columns(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Identifier('ta.target', alias=Identifier('y_true')),
                                Identifier('tb.target', alias=Identifier('y_pred'))],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='left join'),
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
                ApplyTimeseriesPredictorStep(namespace='mindsdb',
                                             predictor=Identifier('tp3', alias=Identifier('tb')),
                                             dataframe=Result(1)),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Identifier('ta.target', alias=Identifier('y_true')), Identifier('tb.target', alias=Identifier('y_pred'))]),
            ],
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

    def test_join_predictor_timeseries_query_with_limit(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='left join'),
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
                ApplyTimeseriesPredictorStep(namespace='mindsdb',
                                             predictor=Identifier('tp3', alias=Identifier('tb')),
                                             dataframe=Result(1)),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                LimitOffsetStep(dataframe=Result(3), limit=query.limit),
                ProjectStep(dataframe=Result(4), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_filter_by_group_by_column(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type='left join'),
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
                ApplyTimeseriesPredictorStep(namespace='mindsdb',
                                             predictor=Identifier('tp3', alias=Identifier('tb')),
                                             dataframe=Result(1)),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_latest(self):
        predictor_window = 5
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type=JoinType.LEFT_JOIN,
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
                ApplyTimeseriesPredictorStep(
                    output_time_filter=BinaryOperation('>', args=[Identifier('ta.pickup_hour'), Latest()]),
                    namespace='mindsdb',
                    predictor=Identifier('tp3', alias=Identifier('tb')),
                    dataframe=Result(1),
                ),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_between(self):
        predictor_window = 5
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type=JoinType.LEFT_JOIN,
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
                ApplyTimeseriesPredictorStep(
                    output_time_filter=BetweenOperation(
                        args=[Identifier('ta.pickup_hour'), Constant(1), Constant(10)],
                    ),
                    namespace='mindsdb',
                    predictor=Identifier('tp3', alias=Identifier('tb')),
                    dataframe=Result(1),
                ),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_concrete_date_greater(self):
        predictor_window = 10
        group_by_column = 'vendor_id'

        sql = "select * from mysql.data.ny_output as ta left join mindsdb.tp3 as tb where ta.pickup_hour > 10 and ta.vendor_id = 1"

        query = parse_sql(sql, dialect='mindsdb')

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
                                                                              BinaryOperation('<=',
                                                                                              args=[Identifier(
                                                                                                  'ta.pickup_hour'),
                                                                                                  Constant(10)]),
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
                                                                              BinaryOperation('>',
                                                                                              args=[Identifier(
                                                                                                  'ta.pickup_hour'),
                                                                                                  Constant(10)]),
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
                ApplyTimeseriesPredictorStep(output_time_filter=BinaryOperation('>', args=[Identifier('ta.pickup_hour'), Constant(10)]),
                                             namespace='mindsdb',
                                             predictor=Identifier('tp3', alias=Identifier('tb')),
                                             dataframe=Result(1)),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_concrete_date_greater_or_equal(self):
        predictor_window = 10
        group_by_column = 'vendor_id'

        sql = "select * from mysql.data.ny_output as ta left join mindsdb.tp3 as tb where ta.pickup_hour >= 10 and ta.vendor_id = 1"

        query = parse_sql(sql, dialect='mindsdb')

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
                                                                                                  Constant(10)]),
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
                                                                              BinaryOperation('>=',
                                                                                              args=[Identifier(
                                                                                                  'ta.pickup_hour'),
                                                                                                  Constant(10)]),
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
                ApplyTimeseriesPredictorStep(output_time_filter=BinaryOperation('>=', args=[Identifier('ta.pickup_hour'), Constant(10)]),
                                             namespace='mindsdb',
                                             predictor=Identifier('tp3', alias=Identifier('tb')),
                                             dataframe=Result(1)),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_concrete_date_less(self):
        predictor_window = 10
        group_by_column = 'vendor_id'

        sql = "select * from mysql.data.ny_output as ta join mindsdb.tp3 as tb where ta.pickup_hour < 10 and ta.vendor_id = 1"

        query = parse_sql(sql, dialect='mindsdb')

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
                                                                          BinaryOperation('and', args=[
                                                                              BinaryOperation('<',
                                                                                              args=[Identifier(
                                                                                                  'ta.pickup_hour'),
                                                                                                  Constant(10)]),
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
                              ),
                ApplyTimeseriesPredictorStep(
                    output_time_filter=BinaryOperation('<', args=[Identifier('ta.pickup_hour'), Constant(10)]),
                    namespace='mindsdb',
                    predictor=Identifier('tp3', alias=Identifier('tb')),
                    dataframe=Result(1),
                ),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             right=Identifier('result_2', alias=Identifier('tb')),
                             left=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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

    def test_join_predictor_timeseries_concrete_date_less_or_equal(self):
        predictor_window = 10
        group_by_column = 'vendor_id'

        sql = "select * from  mindsdb.tp3 as tb left join mysql.data.ny_output as ta where ta.pickup_hour <= 10 and ta.vendor_id = 1"

        query = parse_sql(sql, dialect='mindsdb')

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
                                                                          BinaryOperation('and', args=[
                                                                              BinaryOperation('<=',
                                                                                              args=[Identifier(
                                                                                                  'ta.pickup_hour'),
                                                                                                  Constant(10)]),
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
                              ),
                ApplyTimeseriesPredictorStep(
                    output_time_filter=BinaryOperation('<=', args=[Identifier('ta.pickup_hour'), Constant(10)]),
                    namespace='mindsdb',
                    predictor=Identifier('tp3', alias=Identifier('tb')),
                    dataframe=Result(1),
                ),
                JoinStep(left=Result(2),
                         right=Result(1),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
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
        

    def test_join_predictor_timeseries_error_on_nested_where(self):
        query = Select(targets=[Identifier('pred.time'), Identifier('pred.price')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=None,
                                       implicit=True),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('and', args=[BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),
                                                        BinaryOperation('>', args=[Identifier('tab1.time'), Latest()]),]),
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

    def test_join_predictor_timeseries_default_namespace_predictor(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('tp3', alias=Identifier('tb')),
                                       right=Identifier('mysql.data.ny_output', alias=Identifier('ta')),
                                       join_type=JoinType.LEFT_JOIN),
                       )

        expected_plan = QueryPlan(
            default_namespace='mindsdb',
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
                ApplyTimeseriesPredictorStep(
                    namespace='mindsdb',
                    predictor=Identifier('tp3', alias=Identifier('tb')),
                    dataframe=Result(1),
                ),
                JoinStep(left=Result(2),
                         right=Result(1),
                         query=Join(
                             left=Identifier('result_2', alias=Identifier('tb')),
                             right=Identifier('result_1', alias=Identifier('ta')),
                             join_type=JoinType.LEFT_JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
        )

        plan = plan_query(query,
                          integrations=['mysql'],
                          predictor_namespace='mindsdb',
                          default_namespace='mindsdb',
                          predictor_metadata={
                              'tp3': {'timeseries': True,
                                       'order_by_column': 'pickup_hour',
                                       'group_by_column': group_by_column,
                                       'window': predictor_window}
                          })

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_join_predictor_timeseries_default_namespace_integration(self):
        predictor_window = 10
        group_by_column = 'vendor_id'
        query = Select(targets=[Star()],
                       from_table=Join(left=Identifier('data.ny_output', alias=Identifier('ta')),
                                       right=Identifier('mindsdb.tp3', alias=Identifier('tb')),
                                       join_type=JoinType.JOIN),
                       )

        expected_plan = QueryPlan(
            default_namespace='mysql',
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
                ApplyTimeseriesPredictorStep(
                    namespace='mindsdb',
                    predictor=Identifier('tp3', alias=Identifier('tb')),
                    dataframe=Result(1),
                ),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(
                             left=Identifier('result_1', alias=Identifier('ta')),
                             right=Identifier('result_2', alias=Identifier('tb')),
                             join_type=JoinType.JOIN)
                         ),
                ProjectStep(dataframe=Result(3), columns=[Star()]),
            ],
        )

        plan = plan_query(query,
                          integrations=['mysql'],
                          predictor_namespace='mindsdb',
                          default_namespace='mysql',
                          predictor_metadata={
                              'tp3': {'timeseries': True,
                                       'order_by_column': 'pickup_hour',
                                       'group_by_column': group_by_column,
                                       'window': predictor_window}
                          })

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_timeseries_not_change_query(self):
        sql = "select * from ds.data as ta left join mindsdb.pr as tb where ta.f2 in ('a') and ta.f1 > LATEST"
        query = parse_sql(sql,  dialect='mindsdb')

        query_tree = query.to_tree()

        plan_query(
            query,
            integrations=['ds', 'int'],
            predictor_namespace='mindsdb',
            predictor_metadata={
                'pr': {'timeseries': True, 'window': 3, 'order_by_column': 'f1', 'group_by_column': 'f2'}},
            default_namespace='mindsdb'
        )

        assert query.to_tree() == query_tree

    def test_timeseries_without_group(self):
        sql = "select * from ds.data.ny_output as ta join mindsdb.pr as tb where ta.f1 > LATEST"
        query = parse_sql(sql,  dialect='mindsdb')

        predictor_window = 3
        expected_plan = QueryPlan(
            default_namespace='ds',
            steps=[
                FetchDataframeStep(
                    integration='ds',
                    query=Select(
                       targets=[Star()],
                       from_table=Identifier('data.ny_output', alias=Identifier('ta')),
                       order_by=[OrderBy(Identifier('ta.f1'), direction='DESC')],
                       limit=Constant(predictor_window),
                    )
                ),
                ApplyTimeseriesPredictorStep(
                    namespace='mindsdb',
                    predictor=Identifier('pr', alias=Identifier('tb')),
                    dataframe=Result(0),
                    output_time_filter=BinaryOperation('>', args=[Identifier('ta.f1'), Latest()]),
                ),
                JoinStep(left=Result(0),
                         right=Result(1),
                         query=Join(
                             left=Identifier('result_0', alias=Identifier('ta')),
                             right=Identifier('result_1', alias=Identifier('tb')),
                             join_type=JoinType.JOIN)
                         ),
                ProjectStep(dataframe=Result(2), columns=[Star()]),
            ],
        )

        plan = plan_query(
            query,
            integrations=['ds', 'int'],
            predictor_namespace='mindsdb',
            predictor_metadata={
                'pr': {'timeseries': True, 'window': predictor_window, 'order_by_column': 'f1', 'group_by_column': None}},
            default_namespace='mindsdb'
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


