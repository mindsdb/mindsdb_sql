from .query_plan import QueryPlanner, QueryPlan


def plan_query(query, *args, **kwargs):
    return QueryPlanner(query, *args, **kwargs).from_query()

