class QueryExecutioner:
    def __init__(self, connection):
        self.connection = connection
        self.step_results = {}

    def execute_plan_step(self, step):
        pass

    def execute_plan(self, query_plan):
        for i, step in enumerate(query_plan):
            result = self.execute_plan_step(step)
            self.step_results[i] = result
        return result

