from .query_executioner import QueryExecutioner


def execute_plan(query_plan, *args, **kwargs):
    return QueryExecutioner(*args, **kwargs).execute_plan(query_plan)

