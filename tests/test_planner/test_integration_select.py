import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep)


class TestPlanIntegrationSelect:
    def test_integration_select_plan(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Identifier('int.tab'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                           BinaryOperation('>', args=[Identifier('column3'), Constant(0)]),
                       ]))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Identifier('tab.column1', alias='column1')],
                                                                      from_table=Identifier('tab'),
                                                                      where=BinaryOperation('and', args=[
                                                                              BinaryOperation('=',
                                                                                              args=[Identifier('tab.column1'),
                                                                                                    Identifier('tab.column2')]),
                                                                              BinaryOperation('>',
                                                                                              args=[Identifier('tab.column3'),
                                                                                                    Constant(0)]),
                                                                          ])
                                                                      )),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_limit_offset(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Identifier('int.tab'),
                       where=BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                       limit=Constant(10),
                       offset=Constant(15),
                       )
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('tab.column1', alias='column1')],
                                                             from_table=Identifier('tab'),
                                                             where=BinaryOperation('=', args=[Identifier('tab.column1'),
                                                                                              Identifier(
                                                                                                  'tab.column2')]),
                                                             limit=Constant(10),
                                                             offset=Constant(15),
                                                             ),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_order_by(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Identifier('int.tab'),
                       where=BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                       limit=Constant(10),
                       offset=Constant(15),
                       order_by=[OrderBy(field=Identifier('column1'))],
                       )
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('tab.column1', alias='column1')],
                                                             from_table=Identifier('tab'),
                                                             where=BinaryOperation('=', args=[Identifier('tab.column1'),
                                                                                              Identifier(
                                                                                                  'tab.column2')]),
                                                             limit=Constant(10),
                                                             offset=Constant(15),
                                                             order_by=[OrderBy(field=Identifier('tab.column1'))],
                                                             ),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_plan_star(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int', query=Select(targets=[Star()], from_table=Identifier('tab'))),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_plan_complex_path(self):
        query = Select(targets=[Identifier(parts=['int', 'tab', 'a column with spaces'])],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('tab.`a column with spaces`', alias='int.tab.`a column with spaces`')],
                                                             from_table=Identifier('tab')),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_table_alias(self):
        query = Select(targets=[Identifier('col1')],
                       from_table=Identifier('int.tab', alias='alias'))

        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier(parts=['alias','col1'],
                                                                                 alias='col1')],
                                                             from_table=Identifier(parts=['tab'], alias='alias')),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_column_alias(self):
        query = Select(targets=[Identifier('col1', alias='column_alias')],
                       from_table=Identifier('int.tab'))

        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier(parts=['tab', 'col1'], alias='column_alias')],
                                                             from_table=Identifier(parts=['tab'])),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_integration_select_plan_group_by(self):
        query = Select(targets=[Identifier('column1'),
                                Identifier("column2"),
                                Function(op="sum",
                                 args=[Identifier(parts=["column3"])],
                                 alias='total'),
                                ],
                       from_table=Identifier('int.tab'),
                       group_by=[Identifier("column1"), Identifier("column2")],
                       having=BinaryOperation('=', args=[Identifier("column1"), Constant(0)])
                       )
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[
                                                             Identifier('tab.column1', alias='column1'),
                                                             Identifier('tab.column2', alias='column2'),
                                                             Function(op="sum",
                                                                      args=[Identifier(parts=['tab', 'column3'])],
                                                                      alias='total'),

                                                         ],
                                                             from_table=Identifier('tab'),
                                                             group_by=[Identifier('tab.column1'), Identifier('tab.column2')],
                                                             having=BinaryOperation('=', args=[Identifier('tab.column1'),
                                                                                               Constant(0)])
                                                         )),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_no_integration_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Identifier('int.tab'))
        with pytest.raises(PlanningException):
            plan = plan_query(query, integrations=[], predictor_namespace='mindsdb')

    def test_integration_select_subquery_in_target(self):
        query = Select(targets=[Identifier('column1'), Select(targets=[Identifier('column2')],
                                                              from_table=Identifier('int.tab'),
                                                              limit=Constant(1),
                                                              alias='subquery')],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Identifier('tab.column1', alias='column1'),
                                                                               Select(targets=[Identifier('tab.column2', alias='column2')],
                                                                                      from_table=Identifier('tab'),
                                                                                      limit=Constant(1),
                                                                                      alias='subquery')
                                                                               ],
                                                                      from_table=Identifier('tab'),
                                                                      )),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

