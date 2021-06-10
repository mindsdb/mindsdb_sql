import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import QueryPlanner
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep
from mindsdb_sql.utils import JoinType


class TestQueryPlanner:
    def test_basic_plan(self):
        query = Select(targets=[Identifier('column1')],
                       from_table=Identifier('integr.tab'))
        planner = QueryPlanner(integrations=['integr'])

        plan = planner.plan(query)

        assert plan == [
            FetchDataframeStep(integration='integr', table='tab', query=query, save=True),
            ProjectStep(dataframe=Result(0), columns=['column1']),
        ]

    def test_join_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('integr.tab1'),
                                       right=Identifier('integr.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       )
                )
        planner = QueryPlanner(integrations=['integr'])
        plan = planner.plan(query)

        assert plan == [
            FetchDataframeStep(integration='integr', table='tab1',
                               query=Select(targets=[Identifier('column1', alias='tab1.column1')],
                                            from_table=Identifier('tab1')), save=True),
            FetchDataframeStep(integration='integr', table='tab2',
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
        planner = QueryPlanner(integrations=['integr'], predictors=['pred'])
        plan = planner.plan(query)

        assert plan == [
            FetchDataframeStep(integration='integr', table='tab1',
                               query=Select(targets=[Identifier('*')],
                                            from_table=Identifier('tab1')),
                               save=True),
            ApplyPredictorStep(dataframe=Result(0), predictor='pred', save=True),
            JoinStep(dataframe_left=Result(0), dataframe_right=Result(1)),
            ProjectStep(dataframe=Result(2), columns=['tab1.column1', 'pred.predicted']),
        ]

    def test_no_integration_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('integr.tab1'),
                                       right=Identifier('pred'))
                       )
        planner = QueryPlanner(integrations=[], predictors=['pred'])
        with pytest.raises(PlanningException):
            plan = planner.plan(query)

    def test_no_predictor_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('integr.tab1'),
                                       right=Identifier('pred'))
                       )
        planner = QueryPlanner(integrations=['integr'], predictors=[])
        with pytest.raises(PlanningException):
            plan = planner.plan(query)
