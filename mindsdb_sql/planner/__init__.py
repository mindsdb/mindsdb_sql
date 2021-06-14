from .query_plan import QueryPlan


def plan_query(query, *args, **kwargs):
    return QueryPlan(*args, **kwargs).from_query(query)

