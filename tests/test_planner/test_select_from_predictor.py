import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep, GetPredictorColumns)
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
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor=Identifier('pred'),
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=[Star()]),
                                  ],

                                  )

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        

    def test_select_from_predictor_aliases_in_project(self):
        query = Select(targets=[Identifier('tb.x1', alias=Identifier('col1')),
                                Identifier('tb.x2', alias=Identifier('col2')),
                                Identifier('tb.y', alias=Identifier('predicted'))],
                       from_table=Identifier('mindsdb.pred', alias=Identifier('tb')),
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
                                                            predictor=Identifier('pred', alias=Identifier('tb')),
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0),
                                                  columns=[Identifier('tb.x1', alias=Identifier('col1')),
                                                           Identifier('tb.x2', alias=Identifier('col2')),
                                                           Identifier('tb.y', alias=Identifier('predicted'))]),
                                  ],

                                  )

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        

    def test_select_from_predictor_plan_predictor_alias(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred', alias=Identifier('pred_alias')),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('pred_alias.x1'), Constant(1)]),
                                                   BinaryOperation(op='=',
                                                                   args=[Identifier('pred_alias.x2'), Constant('2')])],
                                             ))
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor=Identifier('pred', alias=Identifier('pred_alias')),
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=[Star()]),
                                  ],
                                  )

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        

    def test_select_from_predictor_plan_verbose_col_names(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('mindsdb.pred'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('pred.x1'), Constant(1)]),
                                                   BinaryOperation(op='=', args=[Identifier('pred.x2'), Constant('2')])],
                                             ))
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor=Identifier('pred'),
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=[Star()]),
                                  ],
                                  )

        plan = plan_query(query, predictor_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
        



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

    def test_select_from_predictor_default_namespace(self):
        query = Select(targets=[Star()],
                       from_table=Identifier('pred'),
                       where=BinaryOperation(op='and',
                                             args=[BinaryOperation(op='=', args=[Identifier('x1'), Constant(1)]),
                                                   BinaryOperation(op='=', args=[Identifier('x2'), Constant('2')])],
                                             ))
        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  default_namespace='mindsdb',
                                  steps=[
                                      ApplyPredictorRowStep(namespace='mindsdb', predictor=Identifier('pred'),
                                                            row_dict={'x1': 1, 'x2': '2'}),
                                      ProjectStep(dataframe=Result(0), columns=[Star()]),
                                  ],
                                  )

        plan = plan_query(query, predictor_namespace='mindsdb', default_namespace='mindsdb')

        assert plan.steps == expected_plan.steps

    def test_select_from_predictor_get_columns(self):
        sql = f'SELECT GDP_per_capita_USD FROM hdi_predictor_external WHERE 1 = 0'
        query = parse_sql(sql, dialect='mindsdb')

        expected_query = Select(targets=[Identifier('GDP_per_capita_USD')],
                                       from_table=Identifier('hdi_predictor_external'),
                                       where=BinaryOperation(op="=",
                                                             args=[Constant(1), Constant(0)]))
        assert query.to_tree() == expected_query.to_tree()

        expected_plan = QueryPlan(predictor_namespace='mindsdb',
                                  default_namespace='mindsdb',
                                  steps=[
                                      GetPredictorColumns(namespace='mindsdb',
                                                            predictor=Identifier('hdi_predictor_external')),
                                      ProjectStep(dataframe=Result(0), columns=[Identifier('GDP_per_capita_USD')]),
                                  ],
                                  )

        plan = plan_query(query, predictor_namespace='mindsdb', default_namespace='mindsdb')

        assert plan.steps == expected_plan.steps
