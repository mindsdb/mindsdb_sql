import copy
from collections import defaultdict
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import Select, Identifier, Join, Star, BinaryOperation, Constant, Operation, OrderBy
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, FilterStep, GroupByStep, LimitOffsetStep, OrderByStep)
from mindsdb_sql.planner.utils import (get_integration_path_from_identifier,
                                       get_predictor_namespace_and_name_from_identifier,
                                       disambiguate_integration_column_identifier,
                                       disambiguate_predictor_column_identifier, recursively_disambiguate_identifiers,
                                       recursively_disambiguate_identifiers_in_op, disambiguate_select_targets,
                                       recursively_extract_column_values, recursively_check_join_identifiers_for_ambiguity)


class QueryPlan:
    def __init__(self, integrations=None, predictor_namespace=None, steps=None, results=None, result_refs=None):
        self.integrations = integrations or []
        self.predictor_namespace = predictor_namespace or 'mindsdb'
        self.steps = steps or []
        self.results = results or []

        # key: step index
        # value: list of steps that reference the result from step key
        self.result_refs = result_refs or defaultdict(list)

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        if len(self.steps) != len(other.steps):
            return False

        for step, other_step in zip(self.steps, other.steps):
            if step != other_step:
                return False

        if self.result_refs != other.result_refs:
            return False
        return True

    @property
    def last_step_index(self):
        return len(self.steps) - 1

    def add_step(self, step):
        self.steps.append(step)
        self.results.append(self.last_step_index)

    def add_result_reference(self, current_step, ref_step_index):
        if ref_step_index in self.results:
            self.result_refs[ref_step_index].append(current_step)
            return Result(ref_step_index)
        else:
            raise PlanningException(f'Can\'t obtain Result for plan step {ref_step_index}.')

    def add_last_result_reference(self):
        return self.add_result_reference(current_step=self.last_step_index+1,
                                        ref_step_index=self.last_step_index)

    def is_predictor(self, identifier):
        parts = identifier.parts
        if parts[0] == self.predictor_namespace:
            return True
        return False

    def is_integration_table(self, identifier):
        parts = identifier.parts
        if parts[0] in self.integrations:
            return True
        return False

    def get_integration_path_from_identifier_or_error(self, identifier):
        integration_name, table_path, table_alias = get_integration_path_from_identifier(identifier)
        if not integration_name in self.integrations:
            raise PlanningException(f'Unknown integration {integration_name} for table {str(identifier)}')
        return integration_name, table_path, table_alias

    def plan_integration_select(self, select):
        """Plan for a select query that can be fully executed in an integration"""
        integration_name, table_path, table_alias = self.get_integration_path_from_identifier_or_error(select.from_table)

        fetch_df_select = copy.deepcopy(select)
        recursively_disambiguate_identifiers(fetch_df_select, integration_name, table_path, table_alias)

        self.add_step(FetchDataframeStep(integration=integration_name, query=fetch_df_select))

    def plan_select_from_predictor(self, select):
        predictor_namespace, predictor_name, predictor_alias = get_predictor_namespace_and_name_from_identifier(select.from_table)
        new_query_targets = []
        for target in select.targets:
            if isinstance(target, Identifier):
                new_query_targets.append(
                    disambiguate_predictor_column_identifier(target, predictor_name, predictor_alias))
            elif isinstance(target, Star):
                new_query_targets.append(target)
            else:
                raise PlanningException(f'Unknown select target {type(target)}')

        if select.group_by or select.having:
            raise PlanningException(f'Unsupported operation when querying predictor. Only WHERE is allowed and required.')

        row_dict = {}
        where_clause = select.where
        if not where_clause:
            raise PlanningException(f'WHERE clause required when selecting from predictor')

        recursively_extract_column_values(where_clause, row_dict, predictor_name, predictor_alias)

        self.add_step(ApplyPredictorRowStep(namespace=predictor_namespace,
                                            predictor=predictor_name,
                                            row_dict=row_dict,
                                            alias=predictor_alias))
        self.plan_project(select)

    def plan_join_table_and_predictor(self, query, table, predictor_namespace, predictor_name, predictor_alias):
        join = query.from_table
        self.plan_integration_select(Select(targets=[Star()],
                                            from_table=table,
                                            where=query.where,
                                            group_by=query.group_by,
                                            having=query.having,
                                            order_by=query.order_by,
                                            limit=query.limit,
                                            offset=query.offset,
                                            ))
        fetch_table_result = self.add_last_result_reference()
        self.add_step(ApplyPredictorStep(namespace=predictor_namespace,
                                         dataframe=fetch_table_result,
                                         predictor=predictor_name,
                                         alias=predictor_alias))
        fetch_predictor_output_result = self.add_last_result_reference()

        self.add_result_reference(current_step=self.last_step_index + 1,
                                  ref_step_index=fetch_table_result.step_num)

        integration_name, table_path, table_alias = self.get_integration_path_from_identifier_or_error(table)
        new_join = Join(left=Identifier(fetch_table_result.ref_name, alias=table.alias or table_path),
                        right=Identifier(fetch_predictor_output_result.ref_name, alias=predictor_alias or predictor_name),
                        join_type=join.join_type)
        self.add_step(JoinStep(left=fetch_table_result, right=fetch_predictor_output_result, query=new_join))

    def plan_join_two_tables(self, join):

        self.plan_integration_select(Select(targets=[Star()], from_table=join.left))
        self.plan_integration_select(Select(targets=[Star()], from_table=join.right))
        fetch_left_result = self.add_result_reference(current_step=self.last_step_index + 1,
                                                      ref_step_index=self.last_step_index - 1)
        fetch_right_result = self.add_result_reference(current_step=self.last_step_index + 1,
                                                       ref_step_index=self.last_step_index)

        left_integration_name, left_table_path, left_table_alias = self.get_integration_path_from_identifier_or_error(
            join.left)
        right_integration_name, right_table_path, right_table_alias = self.get_integration_path_from_identifier_or_error(
            join.right)

        new_condition_args = []
        for arg in join.condition.args:
            if isinstance(arg, Identifier):
                if left_table_path in arg.parts:
                    new_condition_args.append(
                        disambiguate_integration_column_identifier(arg, left_integration_name, left_table_path,
                                                            left_table_alias, initial_path_as_alias=False))
                elif right_table_path in arg.parts:
                    new_condition_args.append(
                        disambiguate_integration_column_identifier(arg, right_integration_name, right_table_path,
                                                            right_table_alias, initial_path_as_alias=False))
                else:
                    raise PlanningException(
                        f'Wrong table or no source table in join condition for column: {str(arg)}')
            else:
                new_condition_args.append(arg)
        new_join = copy.deepcopy(join)
        new_join.condition.args = new_condition_args
        new_join.left = Identifier(left_table_path, alias=left_table_alias)
        new_join.right = Identifier(right_table_path, alias=right_table_alias)
        self.add_step(JoinStep(left=fetch_left_result, right=fetch_right_result, query=new_join))

    def plan_project(self, query):
        last_step_result = self.add_last_result_reference()
        out_aliases = {}
        out_columns = []
        for target in query.targets:
            target_no_alias_copy = copy.deepcopy(target)
            target_no_alias_copy.alias = None
            if target.alias:
                out_aliases[str(target_no_alias_copy)] = target.alias
            out_columns.append(str(target_no_alias_copy))
        self.add_step(ProjectStep(dataframe=last_step_result, columns=out_columns, aliases=out_aliases))

    def plan_join(self, query):
        join = query.from_table

        recursively_check_join_identifiers_for_ambiguity(query.where)
        recursively_check_join_identifiers_for_ambiguity(query.group_by)
        recursively_check_join_identifiers_for_ambiguity(query.having)
        recursively_check_join_identifiers_for_ambiguity(query.order_by)

        if isinstance(join.left, Identifier) and isinstance(join.right, Identifier):
            if self.is_predictor(join.left) and self.is_predictor(join.right):
                raise PlanningException(f'Can\'t join two predictors {str(join.left.parts[0])} and {str(join.left.parts[1])}')

            predictor_namespace = None
            predictor_name = None
            predictor_alias = None
            table = None
            if self.is_predictor(join.left):
                predictor_namespace, predictor_name, predictor_alias = get_predictor_namespace_and_name_from_identifier(join.left)
            else:
                table = join.left

            if self.is_predictor(join.right):
                predictor_namespace, predictor_name, predictor_alias = get_predictor_namespace_and_name_from_identifier(join.right)
            else:
                table = join.right

            if predictor_name:
                # One argument is a table, another is a predictor
                # Apply mindsdb model to result of last dataframe fetch
                # Then join results of applying mindsdb with table
                self.plan_join_table_and_predictor(query, table, predictor_namespace, predictor_name, predictor_alias)
            else:
                # Both arguments are tables, join results of 2 dataframe fetches

                self.plan_join_two_tables(join)

                if query.where:
                    last_result = self.add_last_result_reference()
                    self.add_step(FilterStep(dataframe=last_result, query=query.where))

                if query.group_by:
                    last_result = self.add_last_result_reference()
                    group_by_targets = []
                    for t in query.targets:
                        target_copy = copy.deepcopy(t)
                        target_copy.alias = None
                        group_by_targets.append(target_copy)
                    self.add_step(GroupByStep(dataframe=last_result, columns=query.group_by, targets=group_by_targets))

                if query.having:
                    last_result = self.add_last_result_reference()
                    self.add_step(FilterStep(dataframe=last_result, query=query.having))

                if query.order_by:
                    last_result = self.add_last_result_reference()
                    self.add_step(OrderByStep(dataframe=last_result, order_by=query.order_by))

                if query.limit is not None or query.offset is not None:
                    last_result = self.add_last_result_reference()
                    limit = query.limit.value if query.limit is not None else None
                    offset = query.offset.value if query.offset is not None else None
                    self.add_step(LimitOffsetStep(dataframe=last_result, limit=limit, offset=offset))

        else:
            raise PlanningException(f'Join of unsupported objects, currently only tables and predictors can be joined.')
        self.plan_project(query)

    def plan_select(self, query):
        from_table = query.from_table

        if isinstance(from_table, Identifier):
            if self.is_predictor(from_table):
                self.plan_select_from_predictor(query)
            else:
                self.plan_integration_select(query)
        elif isinstance(from_table, Join):
            self.plan_join(query)
        else:
            raise PlanningException(f'Unsupported from_table {type(from_table)}')

    def from_query(self, query):
        if isinstance(query, Select):
            self.plan_select(query)
        else:
            raise PlanningException(f'Unsupported query type {type(query)}')

        return self
