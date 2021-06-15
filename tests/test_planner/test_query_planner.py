import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep
from mindsdb_sql.utils import JoinType


class TestQueryPlanner:
    def test_pure_select_plan(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int', query=Select(targets=[Identifier('tab.column1', alias='column1')], from_table=Identifier('tab')), save=True),
                                      ProjectStep(dataframe=Result(0), columns=['column1']),
                                  ], result_refs={0: [1]})

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_pure_select_plan_alias(self):
        query = Select(targets=[Identifier('column1', alias='alias')],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('tab.column1', alias='alias')],
                                                             from_table=Identifier('tab')),
                                                         save=True),
                                      ProjectStep(dataframe=Result(0), columns=['alias']),
                                  ], result_refs={0: [1]})

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_pure_select_plan_complex_path(self):
        query = Select(targets=[Identifier(parts=['int', 'tab', 'a column with spaces'])],
                       from_table=Identifier('int.tab'))
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier('tab.`a column with spaces`', alias='int.tab.`a column with spaces`')],
                                                             from_table=Identifier('tab')),
                                                         save=True),
                                      ProjectStep(dataframe=Result(0), columns=['int.tab.`a column with spaces`']),
                                  ], result_refs={0: [1]})

        plan = plan_query(query, integrations=['int'])

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_pure_select_table_alias(self):
        query = Select(targets=[Identifier('col1')],
                       from_table=Identifier('int.tab', alias='alias'))

        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Identifier(parts=['alias','col1'],
                                                                                 alias='col1')],
                                                             from_table=Identifier(parts=['tab'], alias='alias')),
                                                         save=True),
                                      ProjectStep(dataframe=Result(0), columns=['col1']),
                                  ], result_refs={0: [1]})

        plan = plan_query(query, integrations=['int'])
        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs


    def test_no_integration_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Identifier('int.tab')
                       )
        with pytest.raises(PlanningException):
            plan = plan_query(query, integrations=[], predictors=['pred'])

    def test_join_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('integr.tab1'),
                                       right=Identifier('integr.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       )
                )
        plan = plan_query(query, integrations=['integr'])

        assert plan == [
            FetchDataframeStep(integration='integr', table_path='tab1',
                               query=Select(targets=[Identifier('column1', alias='tab1.column1')],
                                            from_table=Identifier('tab1')), save=True),
            FetchDataframeStep(integration='integr', table_path='tab2',
                               query=Select(targets=[Identifier('column1', alias='tab2.column1'), Identifier('column2', alias='tab2.column2')],
                                            from_table=Identifier('tab2')), save=True),
            JoinStep(dataframe_left=Result(0), dataframe_right=Result(1), condition=query.from_table.condition,
                     join_type=query.from_table.join_type),
            ProjectStep(dataframe=Result(2), columns=['tab1.column1', 'tab2.column1', 'tab2.column2']),
        ]

    def test_join_predictor_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('integr.tab1'),
                                       right=Identifier('pred'))
                )
        plan = plan_query(query, integrations=['integr'], predictors=['pred'])

        assert plan == [
            FetchDataframeStep(integration='integr', table_path='tab1',
                               query=Select(targets=[Star()],
                                            from_table=Identifier('tab1')),
                               save=True),
            ApplyPredictorStep(dataframe=Result(0), predictor='pred', save=True),
            JoinStep(dataframe_left=Result(0), dataframe_right=Result(1)),
            ProjectStep(dataframe=Result(2), columns=['tab1.column1', 'pred.predicted']),
        ]



    def test_no_predictor_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('integr.tab1'),
                                       right=Identifier('pred'),
                                       join_type=None,
                                       implicit=True)
                       )

        with pytest.raises(PlanningException):
            plan = plan_query(query, integrations=['integr'])
