from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mindsdb import Evaluate

from mindsdb_sql.planner import plan_query
from mindsdb_sql.planner.query_plan import QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep, EvaluateStep)
from mindsdb_sql.parser.utils import JoinType


class TestPlanEvaluate:
    def test_plan_evaluate(self):
        query = Evaluate(
            name=Identifier('r2_score'),
            using=None,
            data=Select(
                targets=[Identifier('t.column1'), Identifier('m.predicted')],
                from_table=Join(
                    left=Identifier('int.tab1'),
                    right=Identifier('mindsdb.pred'),
                    join_type=JoinType.INNER_JOIN,
                ),
                limit=Constant(100),
            )
            # """SELECT t.column1, m.predicted FROM int.tab1 AS t JOIN mindsdb.pred AS m LIMIT 100"""
        )

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int',
                                   query=Select(targets=[Star()],
                                                from_table=Identifier('tab1'),
                                                limit=Constant(100)),
                                   ),
                ApplyPredictorStep(namespace='mindsdb', dataframe=Result(0), predictor=Identifier('pred')),
                JoinStep(left=Result(0), right=Result(1),
                         query=Join(left=Identifier('result_0'),
                                    right=Identifier('result_1'),
                                    join_type=JoinType.INNER_JOIN)),
                ProjectStep(dataframe=Result(2), columns=[Identifier('t.column1'), Identifier('m.predicted')]),
                EvaluateStep(dataframe=Result(3), metric=Identifier('r2_score'))
            ],
        )
        plan = plan_query(query, integrations=['int'], predictor_namespace='mindsdb', predictor_metadata={'pred': {}})

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
