import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep, LimitOffsetStep)
from mindsdb_sql.utils import JoinType


class TestPlanJoinTables:
    def test_join_tables_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       )
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
                                      ProjectStep(dataframe=Result(2),
                                                  columns=['tab1.column1', 'tab2.column1', 'tab2.column2']),
                                  ],
                                  result_refs={0: [2], 1: [2], 2: [3]})

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

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
                                                  columns=['tab1.column1', 'tab2.column1', 'tab2.column2']),
                                  ],
                                  result_refs={0: [2], 1: [2], 2: [3], 3: [4]})

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_tables_plan_groupby(self):
        query = Select(targets=[
            Identifier('tab1.column1'),
            Identifier('tab2.column1'),
            Function('sum', args=[Identifier('tab2.column2')], alias='total')],
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
                                                            Function('sum', args=[Identifier('tab2.column2')])],
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                      FilterStep(dataframe=Result(3), query=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Constant(0)])),
                                      ProjectStep(dataframe=Result(4), columns=['tab1.column1', 'tab2.column1', 'sum(tab2.column2)'], aliases={'sum(tab2.column2)': 'total'}),
                                  ],
                                  result_refs={0: [2], 1: [2], 2: [3], 3: [4], 4: [5]})
        assert plan == expected_plan

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
                                                  columns=['tab1.column1', 'tab2.column1', 'tab2.column2']),
                                  ],
                                  result_refs={0: [2], 1: [2], 2: [3], 3: [4]})

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_tables_where_ambigous_column_error(self):
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
                                                                 args=[Identifier('column3'),
                                                                       Constant(0)]),
                                                 # Ambigous column: no idea what table column3 comes from
                                             ]
                                             )
                       )

        with pytest.raises(PlanningException) as e:
            plan_query(query, integrations=['int'])

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
                                                  columns=['tab1.column1', 'tab2.column1', 'tab2.column2']),
                                  ],
                                  result_refs={0: [2], 1: [2], 2: [3]})

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_join_tables_error_on_unspecified_table_in_condition(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                               Identifier('column1')]), #Table name omitted
                                       join_type=JoinType.INNER_JOIN
                                       ))
        with pytest.raises(PlanningException):
            plan_query(query, integrations=['int'])

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
