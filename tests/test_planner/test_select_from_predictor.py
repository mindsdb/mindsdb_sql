import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep)
from mindsdb_sql.utils import JoinType


class TestPlanSelectFromPredictor:
    def test_select_from_predictor_plan(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('x1'), Constant(1)]),
                                                   BinaryOperation(op='=', args=[Identifier('x2'), Constant('2')])],
                                             ))
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor='pred',
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=['*']),
                                  ],
                                  result_refs={0: [1]}
                                  )

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs

    def test_select_from_predictor_aliases_in_project(self):
        query = Select(targets=[Identifier('tb.x1', alias='col1'),
                                Identifier('tb.x2', alias='col2'),
                                Identifier('tb.y', alias='predicted')],
                       from_table=Identifier('mindsdb.pred', alias='tb'),
                       where=BinaryOperation(op='and',
                                             args=[
                                                 BinaryOperation(op='=', args=[Identifier('tb.x1'), Constant(1)]),
                                                 BinaryOperation(op='=', args=[Identifier('tb.x2'), Constant('2')]),
                                             ],
                                             )
                       )
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb',
                                                            predictor='pred',
                                                            alias='tb',
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0),
                                                  columns=['tb.x1', 'tb.x2', 'tb.y'],
                                                  aliases={'tb.x1': 'col1', 'tb.x2': 'col2', 'tb.y': 'predicted'}),
                                  ],
                                  result_refs={0: [1]}
                                  )

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs


    def test_select_from_predictor_plan_predictor_alias(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred', alias='pred_alias'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('pred_alias.x1'), Constant(1)]),
                                                   BinaryOperation(op='=',
                                                                   args=[Identifier('pred_alias.x2'), Constant('2')])],
                                             ))
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor='pred',
                                                            alias = 'pred_alias',
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=['*']),
                                  ],
                                  result_refs={0: [1]})

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs


    def test_select_from_predictor_plan_verbose_col_names(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('pred.x1'), Constant(1)]),
                                                   BinaryOperation(op='=', args=[Identifier('pred.x2'), Constant('2')])],
                                             ))
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor='pred',
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=['*']),
                                  ],
                                  result_refs={0: [1]})

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        assert plan.result_refs == expected_plan.result_refs



    def test_select_from_predictor_plan_group_by_error(self):
        query = Select(targets=[Identifier('x1'), Identifier('x2'), Identifier('pred.y')],
                       from_table=Identifier('mindsdb.pred'),
                       group_by=[Identifier('x1')]
                       )
        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb')


    def test_select_from_predictor_wrong_where_op_error(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='>', args=[Identifier('x1'), Constant(1)]),
                                                   BinaryOperation(op='=', args=[Identifier('x2'), Constant('2')])],
                                             ))

        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb')


    def test_select_from_predictor_multiple_values_error(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('x1'), Constant(1)]),
                                                   BinaryOperation(op='=', args=[Identifier('x1'), Constant('2')])],
                                             ))

        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb')


    def test_select_from_predictor_no_where_error(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred'))

        with pytest.raises(PlanningException):
            plan_query(query, predictor_namespace='mindsdb')
