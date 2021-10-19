import pandas as pd
from mindsdb_sql.planner.steps import FetchDataframeStep


class QueryExecutioner:
    def __init__(self, integration_connections):
        self.integration_connections = integration_connections
        self.step_results = {}

    def execute_plan_step(self, step):
        return step.execute(self)

    def execute_plan(self, query_plan):
        result = None
        for i, step in enumerate(query_plan.steps):
            result = self.execute_plan_step(step)
            self.step_results[i] = result
        return result

