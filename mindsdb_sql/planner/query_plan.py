import copy
from collections import defaultdict
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import Select, Identifier, Join, Star, BinaryOperation, Constant, Operation
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep, \
    ApplyPredictorRowStep, FilterStep


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
        if isinstance(other, QueryPlan):
            return self.steps == other.steps and self.result_refs == other.result_refs
        return False

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

    def get_integration_table_or_error_from_identifier(self, identifier):
        parts = identifier.parts

        if len(parts) == 1:
            raise PlanningException(f'No integration specified for table: {str(identifier)}')
        elif len(parts) > 4:
            raise PlanningException(f'Too many parts (dots) in table identifier: {str(identifier)}')

        integration_name = parts[0]
        if not integration_name in self.integrations:
            raise PlanningException(f'Unknown integration {integration_name} for table {str(identifier)}')

        table_path = '.'.join(parts[1:])
        table_alias = identifier.alias
        return integration_name, table_path, table_alias

    def get_predictor_namespace_and_name_from_identifier(select, identifier):
        parts = identifier.parts

        if len(parts) == 1:
            raise PlanningException(f'No predictor name specified for predictor: {str(identifier)}')
        elif len(parts) > 4:
            raise PlanningException(f'Too many parts (dots) in predictor identifier: {str(identifier)}')

        namespace = parts[0]

        predictor_path = '.'.join(parts[1:])
        predictor_alias = identifier.alias
        return namespace, predictor_path, predictor_alias

    def disambiguate_integration_column_identifier(self, identifier, integration_name, table_path, table_alias,
                                       initial_path_as_alias=False):
        """Removes integration name from column if it's present, adds table path if it's absent"""
        column_table_ref = table_alias or table_path
        initial_path_str = identifier.parts_to_str()
        parts = list(identifier.parts)
        if parts[0] == integration_name:
            parts = parts[1:]

        if not parts[0] == column_table_ref:
            parts.insert(0, column_table_ref)

        new_identifier = Identifier(parts=parts)
        if identifier.alias:
            new_identifier.alias = identifier.alias
        elif initial_path_as_alias:
            new_identifier.alias = initial_path_str

        return new_identifier

    def disambiguate_predictor_column_identifier(self, identifier, predictor_name, predictor_alias):
        """Removes integration name from column if it's present, adds table path if it's absent"""
        table_ref = predictor_alias or predictor_name
        parts = list(identifier.parts)
        if parts[0] == table_ref:
            parts = parts[1:]

        new_identifier = Identifier(parts=parts)
        return new_identifier

    def recursively_disambiguate_identifiers_in_op(self, op, integration_name, table_path, table_alias):
        for arg in op.args:
            if isinstance(arg, Identifier):
                new_identifier = self.disambiguate_integration_column_identifier(arg, integration_name, table_path,
                                                                                 table_alias,
                                                                                 initial_path_as_alias=False)
                arg.parts = new_identifier.parts
                arg.alias = new_identifier.alias
            elif isinstance(arg, Operation):
                self.recursively_disambiguate_identifiers_in_op(arg, integration_name, table_path, table_alias)

    def plan_integration_select(self, select):
        """Plan for a select query that can be fully executed in an integration"""
        integration_name, table_path, table_alias = self.get_integration_table_or_error_from_identifier(select.from_table)

        new_query_targets = []
        for target in select.targets:
            if isinstance(target, Identifier):
                new_query_targets.append(self.disambiguate_integration_column_identifier(target, integration_name, table_path, table_alias,
                                                                                         initial_path_as_alias=True))
            elif isinstance(target, Star):
                new_query_targets.append(target)
            elif isinstance(target, Operation):
                new_op = copy.deepcopy(target)
                self.recursively_disambiguate_identifiers_in_op(new_op, integration_name, table_path, table_alias)
                new_query_targets.append(new_op)
            else:
                raise PlanningException(f'Unknown select target {type(target)}')

        where = None
        if select.where:
            if not isinstance(select.where, BinaryOperation):
                raise PlanningException(f'Unsupported where clause {type(select.where)}, only BinaryOperation is supported now.')

            where = copy.deepcopy(select.where)
            self.recursively_disambiguate_identifiers_in_op(where, integration_name, table_path, table_alias)

        group_by = None
        if select.group_by:
            group_by = [self.disambiguate_integration_column_identifier(id, integration_name, table_path, table_alias, initial_path_as_alias=False)
                        for id in select.group_by]

        having = None
        if select.having:
            if not isinstance(select.having, BinaryOperation):
                raise PlanningException(
                    f'Unsupported having clause {type(select.having)}, only BinaryOperation is supported now.')

            having = copy.deepcopy(select.having)
            self.recursively_disambiguate_identifiers_in_op(having, integration_name, table_path, table_alias)

        fetch_df_query = Select(targets=new_query_targets,
                                from_table=Identifier(table_path, alias=table_alias),
                                where=where,
                                group_by=group_by,
                                having=having,
                                )
        self.add_step(FetchDataframeStep(integration=integration_name, query=fetch_df_query))

    def recursively_extract_column_values(self, op, row_dict, predictor_name, predictor_alias):
        if isinstance(op, BinaryOperation) and op.op == '=':
            id = self.disambiguate_predictor_column_identifier(op.args[0], predictor_name, predictor_alias)
            value = op.args[1]
            if not (isinstance(id, Identifier) and isinstance(value, Constant)):
                raise PlanningException(f'The WHERE clause for selecting from a predictor'
                                        f' must contain pairs \'Identifier(...) = Constant(...)\','
                                        f' found instead: {id.to_tree()}, {value.to_tree()}')

            if str(id) in row_dict:
                raise PlanningException(f'Multiple values provided for {str(id)}')
            row_dict[str(id)] = value.value
        elif isinstance(op, BinaryOperation) and op.op == 'and':
            self.recursively_extract_column_values(op.args[0], row_dict, predictor_name, predictor_alias)
            self.recursively_extract_column_values(op.args[1], row_dict, predictor_name, predictor_alias)
        else:
            raise PlanningException(f'Only \'and\' and \'=\' operations allowed in WHERE clause, found: {op.to_tree()}')

    def plan_select_from_predictor(self, select):
        predictor_namespace, predictor_name, predictor_alias = self.get_predictor_namespace_and_name_from_identifier(select.from_table)
        new_query_targets = []
        for target in select.targets:
            if isinstance(target, Identifier):
                new_query_targets.append(
                    self.disambiguate_predictor_column_identifier(target, predictor_name, predictor_alias))
            elif isinstance(target, Star):
                new_query_targets.append(target)
            else:
                raise PlanningException(f'Unknown select target {type(target)}')

        row_dict = {}
        where_clause = select.where
        if not where_clause:
            raise PlanningException(f'WHERE clause required when selecting from predictor')

        self.recursively_extract_column_values(where_clause, row_dict, predictor_name, predictor_alias)

        # Get values from WHERE
        self.add_step(ApplyPredictorRowStep(namespace=predictor_namespace, predictor=predictor_name, row_dict=row_dict))

    def plan_join_table_and_predictor(self, query, table, predictor_namespace, predictor_name, predictor_alias):
        join = query.from_table
        self.plan_integration_select(Select(targets=[Star()], from_table=table, where=query.where))
        fetch_table_result = self.add_last_result_reference()
        self.add_step(ApplyPredictorStep(namespace=predictor_namespace, dataframe=fetch_table_result, predictor=predictor_name))
        fetch_predictor_output_result = self.add_last_result_reference()

        self.add_result_reference(current_step=self.last_step_index + 1,
                                  ref_step_index=fetch_table_result.step_num)

        integration_name, table_path, table_alias = self.get_integration_table_or_error_from_identifier(table)
        new_join = Join(left=Identifier(fetch_table_result.ref_name, alias=table.alias or table_path),
                        right=Identifier(fetch_predictor_output_result.ref_name, alias=predictor_alias or predictor_name),
                        join_type=join.join_type)
        self.add_step(JoinStep(left=fetch_table_result, right=fetch_predictor_output_result, query=new_join))

    def plan_join_two_tables(self, join, left_dataframe, right_dataframe):
        left_integration_name, left_table_path, left_table_alias = self.get_integration_table_or_error_from_identifier(
            join.left)
        right_integration_name, right_table_path, right_table_alias = self.get_integration_table_or_error_from_identifier(
            join.right)

        new_condition_args = []
        for arg in join.condition.args:
            if isinstance(arg, Identifier):
                if left_table_path in arg.parts:
                    new_condition_args.append(
                        self.disambiguate_integration_column_identifier(arg, left_integration_name, left_table_path,
                                                            left_table_alias, initial_path_as_alias=False))
                elif right_table_path in arg.parts:
                    new_condition_args.append(
                        self.disambiguate_integration_column_identifier(arg, right_integration_name, right_table_path,
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
        self.add_step(JoinStep(left=left_dataframe, right=right_dataframe, query=new_join))

    def recursively_check_join_identifiers_for_ambiguity(self, op):
        for arg in op.args:
            if isinstance(arg, Identifier):
                if len(arg.parts) == 1:
                    raise PlanningException(f'Ambigous identifier {str(arg)}, provide table name when filtering a join.')
            elif isinstance(arg, Operation):
                self.recursively_check_join_identifiers_for_ambiguity(arg)

    def plan_project(self, query):
        last_step_result = self.add_last_result_reference()
        out_aliases = {}
        out_columns = []
        for target in query.targets:
            if target.alias:
                out_aliases[str(target)] = target.alias
            target.alias = None
            out_columns.append(str(target))

        self.add_step(ProjectStep(dataframe=last_step_result, columns=out_columns, aliases=out_aliases))

    def plan_join(self, query):
        join = query.from_table

        if query.where:
            self.recursively_check_join_identifiers_for_ambiguity(query.where)

        if isinstance(join.left, Identifier) and isinstance(join.right, Identifier):
            if self.is_predictor(join.left) and self.is_predictor(join.right):
                raise PlanningException(f'Can\'t join two predictors {str(join.left.parts[0])} and {str(join.left.parts[1])}')

            predictor_namespace = None
            predictor_name = None
            predictor_alias = None
            table = None
            if self.is_predictor(join.left):
                predictor_namespace, predictor_name, predictor_alias = self.get_predictor_namespace_and_name_from_identifier(join.left)
            else:
                table = join.left

            if self.is_predictor(join.right):
                predictor_namespace, predictor_name, predictor_alias = self.get_predictor_namespace_and_name_from_identifier(join.right)
            else:
                table = join.right

            if predictor_name:
                # One argument is a table, another is a predictor
                # Apply mindsdb model to result of last dataframe fetch
                # Then join results of applying mindsdb with table
                self.plan_join_table_and_predictor(query, table, predictor_namespace, predictor_name, predictor_alias)
            else:
                # Both arguments are tables, join results of 2 dataframe fetches
                self.plan_integration_select(Select(targets=[Star()], from_table=join.left))
                self.plan_integration_select(Select(targets=[Star()], from_table=join.right))
                fetch_left_result = self.add_result_reference(current_step=self.last_step_index + 1,
                                                              ref_step_index=self.last_step_index - 1)
                fetch_right_result = self.add_result_reference(current_step=self.last_step_index + 1,
                                                               ref_step_index=self.last_step_index)
                self.plan_join_two_tables(join, fetch_left_result, fetch_right_result)

            # Filter join results
            # We don't do that if joined predictor with table, because in that case WHERE is pushed to the integration query
            if query.where and not predictor_name:
                last_result = self.add_last_result_reference()
                self.add_step(FilterStep(dataframe=last_result, query=query.where))
        else:
            raise PlanningException(f'Join of unsupported objects, currently only tables and predictors can be joined.')
        self.plan_project(query)

    def plan_select(self, query):
        target_columns = query.targets
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
