import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep, LimitOffsetStep, OrderByStep)
from mindsdb_sql.utils import JoinType


class TestPlanJoinTables:
    def test_join_tables_plan(self):
        queries = [
            # No integrations in condition
            Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                   from_table=Join(left=Identifier('int.tab1'),
                                   right=Identifier('int.tab2'),
                                   condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                           Identifier('tab2.column1')]),
                                   join_type=JoinType.INNER_JOIN
                                   )
                   ),
            # Integrations in condition
            Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                   from_table=Join(left=Identifier('int.tab1'),
                                   right=Identifier('int.tab2'),
                                   condition=BinaryOperation(op='=', args=[Identifier('int.tab1.column1'),
                                                                           Identifier('int.tab2.column1')]),
                                   join_type=JoinType.INNER_JOIN
                                   ),
                   ),
        ]
        for query in queries:
            plan = plan_query(query, integrations=['int'])
            expected_plan = QueryPlan(integrations=['int'],
                                      steps = [
                                          FetchDataframeStep(integration='int',
                                                             query=Select(
                                                                 targets=[Star()],
                                                                 from_table=Identifier('tab1')),
                                                             ),
                                          FetchDataframeStep(integration='int',
                                                             query=Select(targets=[Star()],
                                                                          from_table=Identifier('tab2')),
                                                             ),
                                          JoinStep(left=Result(0), right=Result(1),
                                                   query=Join(left=Identifier('tab1'),
                                                              right=Identifier('tab2'),
                                                              condition=BinaryOperation(op='=',
                                                                                        args=[Identifier('tab1.column1'),
                                                                                              Identifier('tab2.column1')]),
                                                              join_type=JoinType.INNER_JOIN
                                                              )),
                                          ProjectStep(dataframe=Result(2),
                                                      columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                      ],
            )
            assert plan.steps == expected_plan.steps

    def test_join_tables_plan_databases_specified(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                   from_table=Join(left=Identifier('int.db.tab1'),
                                   right=Identifier('int.db.tab2'),
                                   condition=BinaryOperation(op='=', args=[Identifier('int.db.tab1.column1'),
                                                                           Identifier('int.db.tab2.column1')]),
                                   join_type=JoinType.INNER_JOIN
                                   ),
                   )

        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('db.tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('db.tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('db.tab1'),
                                                          right=Identifier('db.tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('db.tab1.column1'),
                                                                                          Identifier('db.tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
        )

        assert plan.steps == expected_plan.steps

    def test_join_tables_plan_alias(self):
        query = Select(targets=[Identifier('t1.column1'), Identifier('t2.column1'), Identifier('t2.column2')],
                   from_table=Join(left=Identifier('int.db.tab1', alias=Identifier('t1')),
                                   right=Identifier('int.db.tab2', alias=Identifier('t2')),
                                   condition=BinaryOperation(op='=', args=[Identifier('t1.column1'),
                                                                           Identifier('t2.column1')]),
                                   join_type=JoinType.INNER_JOIN
                                   ),
                   )

        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('db.tab1', alias=Identifier('t1'))),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('db.tab2', alias=Identifier('t2'))),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('db.tab1', alias=Identifier('t1')),
                                                          right=Identifier('db.tab2', alias=Identifier('t2')),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('t1.column1'),
                                                                                          Identifier('t2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('column1', alias=Identifier('t1.column1')),
                                                           Identifier('column1', alias=Identifier('t2.column1')),
                                                           Identifier('column2', alias=Identifier('t2.column2'))]),
                                  ],
                                  )

        for step, expected_step in zip(plan.steps, expected_plan.steps):
            assert step == expected_step

    def test_join_tables_where_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                               Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       ),
                       where=BinaryOperation('and',
                                             args=[
                                                 BinaryOperation('and',
                                                                 args=[
                                                                     BinaryOperation('=',
                                                                                     args=[Identifier('tab1.column1'),
                                                                                           Constant(1)]),
                                                                     BinaryOperation('=',
                                                                                     args=[Identifier('tab2.column1'),
                                                                                           Constant(0)]),

                                                                 ]
                                                                 ),
                                                 BinaryOperation('=',
                                                                 args=[Identifier('tab1.column3'),
                                                                       Identifier('tab2.column3')]),
                                             ]
                                             )
                       )

        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1'),
                                                         ),

                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2'),
                                                                      ),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      FilterStep(dataframe=Result(2),
                                                 query=BinaryOperation('and',
                                                                       args=[
                                                                           BinaryOperation('and',
                                                                                           args=[
                                                                                               BinaryOperation('=',
                                                                                                               args=[
                                                                                                                   Identifier(
                                                                                                                       'tab1.column1'),
                                                                                                                   Constant(
                                                                                                                       1)]),
                                                                                               BinaryOperation('=',
                                                                                                               args=[
                                                                                                                   Identifier(
                                                                                                                       'tab2.column1'),
                                                                                                                   Constant(
                                                                                                                       0)]),

                                                                                           ]
                                                                                           ),
                                                                           BinaryOperation('=',
                                                                                           args=[Identifier(
                                                                                               'tab1.column3'),
                                                                                                 Identifier(
                                                                                                     'tab2.column3')]),
                                                                       ]
                                                                       )),
                                      ProjectStep(dataframe=Result(3),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        assert plan.steps == expected_plan.steps
        

    def test_join_tables_plan_groupby(self):
        query = Select(targets=[
            Identifier('tab1.column1'),
            Identifier('tab2.column1'),
            Function('sum', args=[Identifier('tab2.column2')], alias=Identifier('total'))],
            from_table=Join(left=Identifier('int.tab1'),
                            right=Identifier('int.tab2'),
                            condition=BinaryOperation(op='=',
                                                      args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                            join_type=JoinType.INNER_JOIN
                            ),
            group_by=[Identifier('tab1.column1'), Identifier('tab2.column1')],
            having=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Constant(0)])
        )
        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      GroupByStep(dataframe=Result(2),
                                                  targets=[Identifier('tab1.column1'),
                                                            Identifier('tab2.column1'),
                                                            Function('sum', args=[Identifier('tab2.column2')], alias=Identifier('total'))],
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                      FilterStep(dataframe=Result(3), query=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Constant(0)])),
                                      ProjectStep(dataframe=Result(4),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'),
                                                           Identifier(parts=['sum(tab2.column2)'], alias=Identifier('total'))]),
                                  ],
                                  )
        for step, expected_step in zip(plan.steps, expected_plan.steps):
            assert step == expected_step


    def test_join_tables_plan_limit_offset(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       ),
                       limit=Constant(10),
                       offset=Constant(15),
                )
        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      LimitOffsetStep(dataframe=Result(2), limit=10, offset=15),
                                      ProjectStep(dataframe=Result(3),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        assert plan.steps == expected_plan.steps
        

    def test_join_tables_plan_order_by(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       ),
                       limit=Constant(10),
                       offset=Constant(15),
                       order_by=[OrderBy(field=Identifier('tab1.column1'))],
                )
        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      OrderByStep(dataframe=Result(2), order_by=[OrderBy(field=Identifier('tab1.column1'))]),
                                      LimitOffsetStep(dataframe=Result(3), limit=10, offset=15),
                                      ProjectStep(dataframe=Result(4),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        assert plan.steps == expected_plan.steps
        

    # def test_join_tables_where_ambigous_column_error(self):
    #     query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
    #                    from_table=Join(left=Identifier('int.tab1'),
    #                                    right=Identifier('int.tab2'),
    #                                    condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
    #                                                                            Identifier('tab2.column1')]),
    #                                    join_type=JoinType.INNER_JOIN
    #                                    ),
    #                    where=BinaryOperation('and',
    #                                          args=[
    #                                              BinaryOperation('and',
    #                                                              args=[
    #                                                                  BinaryOperation('=',
    #                                                                                  args=[Identifier('tab1.column1'),
    #                                                                                        Constant(1)]),
    #                                                                  BinaryOperation('=',
    #                                                                                  args=[Identifier('tab2.column1'),
    #                                                                                        Constant(0)]),
    #
    #                                                              ]
    #                                                              ),
    #                                              BinaryOperation('=',
    #                                                              args=[Identifier('column3'),
    #                                                                    Constant(0)]),
    #                                              # Ambigous column: no idea what table column3 comes from
    #                                          ]
    #                                          )
    #                    )
    #
    #     with pytest.raises(PlanningException) as e:
    #         plan_query(query, integrations=['int'])

    def test_join_tables_disambiguate_identifiers_in_condition(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('int.tab1.column1'), # integration name included
                                                                               Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       )
                       )
        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'), # integration name gets stripped out
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        assert plan.steps == expected_plan.steps

    def test_join_tables_error_on_wrong_table_in_condition(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                               Identifier('tab3.column1')]), #Wrong table name
                                       join_type=JoinType.INNER_JOIN
                                       ))
        with pytest.raises(PlanningException) as e:
            plan_query(query, integrations=['int'])

    def test_join_tables_plan_default_namespace(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('tab1'),
                                       right=Identifier('tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       )
                )
        expected_plan = QueryPlan(integrations=['int'],
                                  default_namespace='int',
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
        )
        plan = plan_query(query, integrations=['int'], default_namespace='int')

        assert plan.steps == expected_plan.steps

    def test_join_tables_in_subquery(self):
        query = Select(targets=[Identifier('subquery.column1')],
                       from_table=Select(targets=[Identifier('column1')],
                                         from_table=Join(left=Identifier('tab1'),
                                                         right=Identifier('tab2'),
                                                         condition=BinaryOperation(op='=',
                                                                                   args=[Identifier('tab1.column1'),
                                                                                         Identifier('tab2.column1')]),
                                                         join_type=JoinType.INNER_JOIN
                                                         ),
                                         alias=Identifier('subquery')))
        expected_plan = QueryPlan(integrations=['int'],
                                  default_namespace='int',
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1'),
                                                         ),
                                                         ),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab2'),
                                                         ),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('subquery.column1')]),
                                  ])
        with pytest.raises(NotImplementedError):
            plan = plan_query(query, integrations=['int'], default_namespace='int')

        # assert plan.steps == expected_plan.steps

