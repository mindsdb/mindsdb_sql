from collections import defaultdict

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import Select, Identifier, Join
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import FetchDataframeStep, ProjectStep


class QueryPlan:
    def __init__(self, integrations=None, predictors=None, steps=None, results=None, result_refs=None):
        self.integrations = integrations or []
        self.predictors = predictors or []
        self.steps = steps or []
        self.results = results or []

        # key: step index
        # value: list of steps that reference the result from step key
        self.result_refs = result_refs or defaultdict(list)

    def __eq__(self, other):
        if isinstance(other, QueryPlan):
            return self.steps == other.steps and self.result_refs == other.result_refs
        return False

    @property
    def last_step_index(self):
        return len(self.steps) - 1

    def add_step(self, step):
        self.steps.append(step)
        if step.save:
            self.results.append(self.last_step_index)

    def add_result_reference(self, current_step, ref_step_index):
        if ref_step_index in self.results:
            self.result_refs[ref_step_index].append(current_step)
            return Result(ref_step_index)
        else:
            raise PlanningException(f'Can\'t obtain Result for plan step {ref_step_index}. Probably step has `save=False`.')

    def get_identifier_integration_table_or_error(self, identifier):
        path = identifier.value

        parts = path.split('.')
        if len(parts) == 1:
            raise PlanningException(f'No integration specified for table: {path}')
        elif len(parts) > 4:
            raise PlanningException(f'Too many parts (dots) in table identifier: {path}')

        integration_name = parts[0]
        if not integration_name in self.integrations:
            raise PlanningException(f'Unknown integration {integration_name} for table {path}')

        table_path = '.'.join(parts[1:])
        return integration_name, table_path

    # def step_from_identifier(self, identifier):
    #     path = identifier.value
    #
    #     parts = path.split('.')
    #     if len(parts) == 1:
    #         raise PlanningException(f'No integration specified for table: {path}')
    #     elif len(parts) > 4:
    #         raise PlanningException(f'Too many parts (dots) in table identifier: {path}')
    #
    #     integration_name = parts[0]
    #     if not integration_name in self.integrations:
    #         raise PlanningException(f'Unknown integration {integration_name} for table {path}')
    #
    #     fetch_df_query = Select(targets=[Star()], from_table=Identifier(parts[1:]), save=True)
    #     return FetchDataframeStep(integration=integration_name, table_path=parts[1:], query=fetch_df_query)
    #
    # def plan_join(self, join):
    #     pass
    #
    # def plan_from_table(self, from_table):
    #     if isinstance(from_table, Identifier):
    #         return [self.step_from_identifier(from_table)]
    #     elif isinstance(from_table, Join):
    #         return self.plan_join(from_table)
    #     else:
    #         raise PlanningException(f'Unsupported from_table: {type(from_table)}')

    def plan_project(self, dataframe, columns):
        return self.add_step(ProjectStep(dataframe=dataframe, columns=columns))

    def plan_pure_select(self, integration_name, table_path, select):
        """Plan for a select query that can be fully executed in an integration"""
        new_query_targets = []
        for target in select.targets:
            if isinstance(target, Identifier):
                initial_value = target.value
                if target.value.startswith(f'{integration_name}.'):
                    target.value = target.value.replace(f'{integration_name}.', '')

                if not target.value.startswith(f'{table_path}.'):
                    target.value = f'{table_path}.{str(target)}'
                new_query_targets.append(Identifier(target.value, alias=target.alias or initial_value))
            else:
                raise PlanningException(f'Unknown select target {type(target)}')

        fetch_df_query = Select(targets=new_query_targets, from_table=Identifier(table_path))
        self.add_step(FetchDataframeStep(integration=integration_name, query=fetch_df_query, save=True))

    def plan_select(self, query):
        target_columns = query.targets
        from_table = query.from_table

        if isinstance(from_table, Identifier):
            integration_name, table_path = self.get_identifier_integration_table_or_error(from_table)
            self.plan_pure_select(integration_name, table_path, query)

        from_table_result = self.add_result_reference(current_step=self.last_step_index+1,
                                                      ref_step_index=self.last_step_index)
        self.plan_project(dataframe=from_table_result, columns=[target.alias or target.value for target in target_columns])

    def from_query(self, query):
        if isinstance(query, Select):
            self.plan_select(query)
        else:
            raise PlanningException(f'Unsupported query type {type(query)}')

        return self
