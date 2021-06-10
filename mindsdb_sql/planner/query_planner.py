from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import Select


class QueryPlanner:
    def __init__(self, integrations=None, predictors=None):
        self.integrations = integrations or []
        self.predictors = predictors or []

    def plan_select(self, query):
        return []

    def plan(self, query):
        if isinstance(query, Select):
            steps = self.plan_select(query)
        else:
            raise PlanningException(f'Unsupported query type {type(query)}')

        return steps
