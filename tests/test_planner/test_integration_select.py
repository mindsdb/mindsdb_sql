import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.steps import FetchDataframeStep


class TestPlanIntegrationSelect:
    def test_integration_select_plan(self):
        query = Select(targets=[Identifier('column1'), Constant(1), Function('database', args=[])],
                       from_table=Identifier('int.tab'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                           BinaryOperation('>', args=[Identifier('column3'), Constant(0)]),
                       ]))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Identifier('tab.column1', alias=Identifier('column1')),
                                                                               Constant(1),
                                                                               Function('database', args=[]),
                                                                               ],
                                                                      from_table=Identifier('tab'),
                                                                      where=BinaryOperation('and', args=[
                                                                              BinaryOperation('=',
                                                                                              args=[Identifier('tab.column1'),
                                                                                                    Identifier('tab.column2')]),
                                                                              BinaryOperation('>',
                                                                                              args=[Identifier('tab.column3'),
                                                                                                    Constant(0)]),
                                                                          ])
                                                                      ),
                                                         step_num=0,
                                                         references=None,
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_integration_name_is_case_insensitive(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Identifier('int.tab'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                           BinaryOperation('>', args=[Identifier('column3'), Constant(0)]),
                       ]))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Identifier('tab.column1', alias=Identifier('column1'))],
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

        plan = plan_query(query, integrations=['INT'])

        assert plan.steps == expected_plan.steps
        

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
                                                             targets=[Identifier('tab.column1', alias=Identifier('column1'))],
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
                                                             targets=[Identifier('tab.column1', alias=Identifier('column1'))],
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
        

    def test_integration_select_plan_star(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int', query=Select(targets=[Star()], from_table=Identifier('tab'))),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_integration_select_plan_complex_path(self):
        query = Select(targets=[Identifier(parts=['int', 'tab', 'a column with spaces'])],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('tab.`a column with spaces`', alias=Identifier(parts=['int', 'tab', 'a column with spaces']))],
                                                             from_table=Identifier('tab')),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_integration_select_table_alias(self):
        query = Select(targets=[Identifier('col1')],
                       from_table=Identifier('int.tab', alias=Identifier('alias')))

        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier(parts=['alias','col1'],
                                                                                 alias=Identifier('col1'))],
                                                             from_table=Identifier(parts=['tab'], alias=Identifier('alias'))),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_integration_select_column_alias(self):
        query = Select(targets=[Identifier('col1', alias=Identifier('column_alias'))],
                       from_table=Identifier('int.tab'))

        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier(parts=['tab', 'col1'], alias=Identifier('column_alias'))],
                                                             from_table=Identifier(parts=['tab'])),
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_integration_select_plan_group_by(self):
        query = Select(targets=[Identifier('column1'),
                                Identifier("column2"),
                                Function(op="sum",
                                 args=[Identifier(parts=["column3"])],
                                 alias=Identifier('total')),
                                ],
                       from_table=Identifier('int.tab'),
                       group_by=[Identifier("column1"), Identifier("column2")],
                       having=BinaryOperation('=', args=[Identifier("column1"), Constant(0)])
                       )
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[
                                                             Identifier('tab.column1', alias=Identifier('column1')),
                                                             Identifier('tab.column2', alias=Identifier('column2')),
                                                             Function(op="sum",
                                                                      args=[Identifier(parts=['tab', 'column3'])],
                                                                      alias=Identifier('total')),

                                                         ],
                                                             from_table=Identifier('tab'),
                                                             group_by=[Identifier('tab.column1'), Identifier('tab.column2')],
                                                             having=BinaryOperation('=', args=[Identifier('tab.column1'),
                                                                                               Constant(0)])
                                                         )),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_no_integration_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Identifier('int.tab'))
        with pytest.raises(PlanningException):
            plan = plan_query(query, integrations=[], predictor_namespace='mindsdb')

    def test_integration_select_subquery_in_target(self):
        query = Select(targets=[Identifier('column1'), Select(targets=[Identifier('column2')],
                                                              from_table=Identifier('int.tab'),
                                                              limit=Constant(1),
                                                              alias=Identifier('subquery'))],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Identifier('tab.column1', alias=Identifier('column1')),
                                                                               Select(targets=[Identifier('tab.column2', alias=Identifier('column2'))],
                                                                                      from_table=Identifier('tab'),
                                                                                      limit=Constant(1),
                                                                                      alias=Identifier('subquery'))
                                                                               ],
                                                                      from_table=Identifier('tab'),
                                                                      )),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_integration_select_subquery_in_from(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Select(targets=[Identifier('column1')],
                                         from_table=Identifier('int.tab'),
                                         alias=Identifier('subquery')))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('column1')],
                                                             from_table=Select(
                                                                 targets=[Identifier('tab.column1', alias=Identifier('column1'))],
                                                                 from_table=Identifier('tab'),
                                                                 alias=Identifier('subquery')),
                                                             )),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        

    def test_integration_select_subquery_in_where(self):
        query = Select(targets=[Star()],
                          from_table=Identifier('int.tab1'),
                          where=BinaryOperation(op='in',
                                                args=(
                                                    Identifier(parts=['column1']),
                                                    Select(targets=[Identifier('column2')],
                                                           from_table=Identifier('int.tab2'),
                                                           parentheses=True)
                                                )))

        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab1'),
                                                                      where=BinaryOperation(op='in',
                                                                                            args=[
                                                                                                Identifier('tab1.column1'),
                                                                                                Select(targets=[
                                                                                                    Identifier('tab2.column2', alias=Identifier('column2'))],
                                                                                                       from_table=Identifier('tab2'),
                                                                                                       parentheses=True)]
                                                                                            ))),
                                  ])

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        
    def test_integration_select_default_namespace(self):
        query = Select(targets=[Identifier('column1'), Constant(1), Function('database', args=[])],
                       from_table=Identifier('tab'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                           BinaryOperation('>', args=[Identifier('column3'), Constant(0)]),
                       ]))

        expected_plan = QueryPlan(integrations=['int'],
                                  default_namespace='int',
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Identifier('tab.column1', alias=Identifier('column1')),
                                                                               Constant(1),
                                                                               Function('database', args=[]),
                                                                               ],
                                                                      from_table=Identifier('tab'),
                                                                      where=BinaryOperation('and', args=[
                                                                              BinaryOperation('=',
                                                                                              args=[Identifier('tab.column1'),
                                                                                                    Identifier('tab.column2')]),
                                                                              BinaryOperation('>',
                                                                                              args=[Identifier('tab.column3'),
                                                                                                    Constant(0)]),
                                                                          ])
                                                                      ),
                                                         step_num=0,
                                                         references=None,
                                                         ),
                                  ])

        plan = plan_query(query, integrations=['int'], default_namespace='int')

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_integration_select_default_namespace_subquery_in_from(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Select(targets=[Identifier('column1')],
                                         from_table=Identifier('tab'),
                                         alias=Identifier('subquery')))
        expected_plan = QueryPlan(integrations=['int'],
                                  default_namespace='int',
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('column1')],
                                                             from_table=Select(
                                                                 targets=[Identifier('tab.column1', alias=Identifier('column1'))],
                                                                 from_table=Identifier('tab'),
                                                                 alias=Identifier('subquery')),
                                                             )),
                                  ])

        plan = plan_query(query, integrations=['int'], default_namespace='int')

        assert plan.steps == expected_plan.steps
