import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query, QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, FilterStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, GroupByStep, UnionStep)
from mindsdb_sql.utils import JoinType


class TestPlanUnion:
    def test_plan_union_queries(self):
        query1 = Select(targets=[Identifier('column1'), Constant(None, alias=Identifier('pred.predicted'))],
                       from_table=Identifier('int.tab'),
                       where=BinaryOperation('and', args=[
                           BinaryOperation('=', args=[Identifier('column1'), Identifier('column2')]),
                           BinaryOperation('>', args=[Identifier('column3'), Constant(0)]),
                       ]))

        query2 = Select(targets=[Identifier('tab1.column1'), Identifier('pred.predicted')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('mindsdb.pred'),
                                       join_type=JoinType.INNER_JOIN,
                                       implicit=True)
                       )

        query = Union(left=query1, right=query2, unique=False)
        expected_plan = QueryPlan(
            steps=[
                # Query 1
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Identifier('tab.column1', alias=Identifier('column1')),
                                                         Constant(None, alias=Identifier('pred.predicted'))],
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
                # Query 2
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1')),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(1), predictor=Identifier('pred')),
                JoinStep(left=Result(1), right=Result(2),
                         query=Join(left=Identifier('result_1', alias=Identifier('tab1')),
                                    right=Identifier('result_2', alias=Identifier('pred')),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(3), columns=[Identifier('tab1.column1'), Identifier('pred.predicted')]),

                # Union
                UnionStep(left=Result(0), right=Result(4), unique=False),

            ],
        )

        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb')

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        
