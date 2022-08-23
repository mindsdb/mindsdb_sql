import copy
from collections import defaultdict
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import (Select, Identifier, Join, Star, BinaryOperation, Constant, OrderBy,
                                    BetweenOperation, Union, NullConstant, CreateTable, Function, Insert,
                                    NativeQuery)

from mindsdb_sql.parser.dialects.mindsdb.latest import Latest
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, FilterStep, GroupByStep, LimitOffsetStep, OrderByStep,
                                       UnionStep, MapReduceStep, MultipleSteps, ApplyTimeseriesPredictorStep,
                                       GetPredictorColumns, SaveToTable, InsertToTable, SubSelectStep)
from mindsdb_sql.planner.ts_utils import (validate_ts_where_condition, find_time_filter, replace_time_filter,
                                          find_and_remove_time_filter)
from mindsdb_sql.planner.utils import (get_integration_path_from_identifier,
                                       get_predictor_namespace_and_name_from_identifier,
                                       disambiguate_integration_column_identifier,
                                       disambiguate_predictor_column_identifier, recursively_disambiguate_identifiers,
                                       get_deepest_select,
                                       recursively_extract_column_values,
                                       recursively_check_join_identifiers_for_ambiguity,
                                       query_traversal)
from mindsdb_sql.planner.query_plan import QueryPlan
from mindsdb_sql.planner import utils
from .query_prepare import PreparedStatementPlanner



class QueryPlanner():

    def __init__(self,
                 query=None,
                 integrations=None,
                 predictor_namespace=None,
                 predictor_metadata=None,
                 default_namespace=None):
        self.query = query
        self.plan = QueryPlan()

        self.integrations = [int.lower() for int in integrations] if integrations else []
        self.predictor_namespace = predictor_namespace.lower() if predictor_namespace else 'mindsdb'
        self.predictor_metadata = predictor_metadata or defaultdict(dict)
        self.default_namespace = default_namespace

        # map for lower names of predictors
        self.predictor_names = {
            k.lower(): k
            for k in self.predictor_metadata.keys()
        }

        # allow to select from mindsdb namespace
        self.integrations.append(self.predictor_namespace)

        self.statement = None

    def is_predictor(self, identifier):
        parts = identifier.parts
        if not parts[-1].lower() in self.predictor_names:
            return False
        if parts[0].lower() == self.predictor_namespace:
            return True
        elif len(parts) == 1 and self.default_namespace == self.predictor_namespace:
            return True
        return False

    # not used
    # def is_integration_table(self, identifier):
    #     parts = identifier.parts
    #     if parts[0].lower() in self.integrations:
    #         return True
    #     elif len(parts) == 1 and self.default_namespace in self.integrations:
    #         return True
    #     return False

    def get_integration_path_from_identifier_or_error(self, identifier, recurse=True):
        try:
            integration_name, table = get_integration_path_from_identifier(identifier)
            if not integration_name.lower() in self.integrations:
                raise PlanningException(f'Unknown integration {integration_name} for table {str(identifier)}. Available integrations: {", ".join(self.integrations)}')
        except PlanningException:
            if not recurse or not self.default_namespace:
                raise
            else:
                new_identifier = copy.deepcopy(identifier)
                new_identifier.parts = [self.default_namespace, *identifier.parts]
                return self.get_integration_path_from_identifier_or_error(new_identifier, recurse=False)
        return integration_name, table

    def get_integration_select_step(self, select):
        integration_name, table = self.get_integration_path_from_identifier_or_error(select.from_table)

        fetch_df_select = copy.deepcopy(select)
        recursively_disambiguate_identifiers(fetch_df_select, integration_name, table)

        return FetchDataframeStep(integration=integration_name, query=fetch_df_select)

    def plan_integration_select(self, select):
        """Plan for a select query that can be fully executed in an integration"""

        return self.plan.add_step(self.get_integration_select_step(select))

    def plan_nested_select(self, select):

        # get all predictors
        mdb_entities = []
        used_integrations = set()

        def find_predictors(node, is_table, **kwargs):
            if isinstance(node, ast.NativeQuery):
                # has NativeQuery syntax
                mdb_entities.append(node)

            if is_table and isinstance(node, ast.Identifier):
                if len(node.parts) > 1 and node.parts[0] in self.integrations:
                    used_integrations.add(node.parts[0])

                if self.is_predictor(node):
                    mdb_entities.append(node)

        utils.query_traversal(select, find_predictors)

        if (
            len(mdb_entities) == 0
            and len(used_integrations) < 2
            and not 'files' in used_integrations
            and not 'views' in used_integrations
        ):
            # if no predictor inside = run as is
            return self.plan_integration_nested_select(select)
        else:
            return self.plan_mdb_nested_select(select)

    def plan_integration_nested_select(self, select):
        fetch_df_select = copy.deepcopy(select)
        deepest_select = get_deepest_select(fetch_df_select)
        integration_name, table = self.get_integration_path_from_identifier_or_error(deepest_select.from_table)
        recursively_disambiguate_identifiers(deepest_select, integration_name, table)
        return self.plan.add_step(FetchDataframeStep(integration=integration_name, query=fetch_df_select))

    def plan_mdb_nested_select(self, select):
        # plan nested select

        # if select.limit == 0:
            # TODO don't run predictor if limit is 0
            # ...

        # subselect_alias = select.from_table.alias
        # if subselect_alias is not None:
        #     subselect_alias = subselect_alias.parts[0]

        select2 = copy.deepcopy(select.from_table)
        select2.parentheses = False
        select2.alias = None
        self.plan_select(select2)
        last_step = self.plan.steps[-1]

        sup_select = self.sub_select_step(select, last_step)
        if sup_select is not None:
            self.plan.add_step(sup_select)
            last_step = sup_select

        return last_step

        # if select.where is not None:
        #     # remove subselect alias
        #     where_query = select.where
        #     if subselect_alias is not None:
        #         def remove_aliases(node, **kwargs):
        #             if isinstance(node, Identifier):
        #
        #                 if len(node.parts) > 1:
        #                     if node.parts[0] == subselect_alias:
        #                         node.parts = node.parts[1:]
        #
        #         query_traversal(where_query, remove_aliases)
        #
        #     last_step = self.plan.add_step(FilterStep(dataframe=last_step.result, query=where_query))
        #
        # group_step = self.plan_group(select, last_step)
        # if group_step is not None:
        #     self.plan.add_step(group_step)
        #     # don't do project step
        #
        # else:
        #     # do we need projection?
        #     if len(select.targets) != 1 or not isinstance(select.targets[0], Star):
        #         # remove prefix alias
        #         if subselect_alias is not None:
        #             for t in select.targets:
        #                 if isinstance(t, Identifier) and t.parts[0] == subselect_alias:
        #                     t.parts.pop(0)
        #
        #         self.plan_project(select, last_step.result, ignore_doubles=True)

        # do we need limit?
        # last_step = self.plan.steps[-1]
        # if select.limit is not None:
        #     last_step = self.plan.add_step(LimitOffsetStep(dataframe=last_step.result, limit=select.limit.value))
        # return last_step


    def plan_select_from_predictor(self, select):
        predictor_namespace, predictor = get_predictor_namespace_and_name_from_identifier(select.from_table, self.default_namespace)

        if select.where == BinaryOperation('=', args=[Constant(1), Constant(0)]):
            # Hardcoded mysql way of getting predictor columns
            predictor_step = self.plan.add_step(
                GetPredictorColumns(namespace=predictor_namespace,
                                      predictor=predictor)
            )
        else:
            new_query_targets = []
            for target in select.targets:
                if isinstance(target, Identifier):
                    new_query_targets.append(
                        disambiguate_predictor_column_identifier(target, predictor))
                elif type(target) in (Star, Constant):
                    new_query_targets.append(target)
                else:
                    raise PlanningException(f'Unknown select target {type(target)}')

            if select.group_by or select.having:
                raise PlanningException(f'Unsupported operation when querying predictor. Only WHERE is allowed and required.')

            row_dict = {}
            where_clause = select.where
            if not where_clause:
                raise PlanningException(f'WHERE clause required when selecting from predictor')

            recursively_extract_column_values(where_clause, row_dict, predictor)

            predictor_step = self.plan.add_step(
                ApplyPredictorRowStep(namespace=predictor_namespace,
                                                predictor=predictor,
                                                row_dict=row_dict)
            )
        project_step = self.plan_project(select, predictor_step.result)
        return predictor_step, project_step

    def plan_predictor(self, query, table, predictor_namespace, predictor):
        int_select = copy.deepcopy(query)
        int_select.targets = [Star()]  # TODO why not query.targets?
        int_select.from_table = table
        integration_select_step = self.plan_integration_select(int_select)

        predictor_step = self.plan.add_step(ApplyPredictorStep(namespace=predictor_namespace,
                                         dataframe=integration_select_step.result,
                                         predictor=predictor))

        return {
            'predictor': predictor_step,
            'data': integration_select_step
        }

    def plan_fetch_timeseries_partitions(self, query, table, predictor_group_by_names):
        targets = [
            Identifier(column)
            for column in predictor_group_by_names
        ]

        query = Select(
            distinct=True,
            targets=targets,
            from_table=table,
            where=query.where,
            modifiers=query.modifiers,
        )
        select_step = self.plan_integration_select(query)
        return select_step

    def plan_timeseries_predictor(self, query, table, predictor_namespace, predictor):
        predictor_name = predictor.to_string(alias=False).lower()
        # to original case
        predictor_name = self.predictor_names[predictor_name]

        predictor_time_column_name = self.predictor_metadata[predictor_name]['order_by_column']
        predictor_group_by_names = self.predictor_metadata[predictor_name]['group_by_columns']
        if predictor_group_by_names is None:
            predictor_group_by_names = []
        predictor_window = self.predictor_metadata[predictor_name]['window']

        if query.order_by:
            raise PlanningException(
                f'Can\'t provide ORDER BY to time series predictor, it will be taken from predictor settings. Found: {query.order_by}')

        saved_limit = query.limit

        if query.group_by or query.having or query.offset:
            raise PlanningException(f'Unsupported query to timeseries predictor: {str(query)}')

        allowed_columns = [predictor_time_column_name.lower()]
        if len(predictor_group_by_names) > 0:
            allowed_columns += [i.lower() for i in predictor_group_by_names]
        validate_ts_where_condition(query.where, allowed_columns=allowed_columns)

        time_filter = find_time_filter(query.where, time_column_name=predictor_time_column_name)

        order_by = [OrderBy(Identifier(parts=[predictor_time_column_name]), direction='DESC')]

        preparation_where = copy.deepcopy(query.where)

        query_modifiers = query.modifiers

        # add {order_by_field} is not null
        def add_order_not_null(condition):
            order_field_not_null = BinaryOperation(op='is not', args=[
                Identifier(parts=[predictor_time_column_name]),
                NullConstant()
            ])
            if condition is not None:
                condition = BinaryOperation(op='and', args=[
                    condition,
                    order_field_not_null
                ])
            else:
                condition = order_field_not_null
            return condition

        preparation_where2 = copy.deepcopy(preparation_where)
        preparation_where = add_order_not_null(preparation_where)

        # Obtain integration selects
        if isinstance(time_filter, BetweenOperation):
            between_from = time_filter.args[1]
            preparation_time_filter = BinaryOperation('<', args=[Identifier(predictor_time_column_name), between_from])
            preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
            integration_select_1 = Select(targets=[Star()],
                                        from_table=table,
                                        where=add_order_not_null(preparation_where2),
                                        modifiers=query_modifiers,
                                        order_by=order_by,
                                        limit=Constant(predictor_window))

            integration_select_2 = Select(targets=[Star()],
                                          from_table=table,
                                          where=preparation_where,
                                          modifiers=query_modifiers,
                                          order_by=order_by)

            integration_selects = [integration_select_1, integration_select_2]
        elif isinstance(time_filter, BinaryOperation) and time_filter.op == '>' and time_filter.args[1] == Latest():
            integration_select = Select(targets=[Star()],
                                        from_table=table,
                                        where=preparation_where,
                                        modifiers=query_modifiers,
                                        order_by=order_by,
                                        limit=Constant(predictor_window),
                                        )
            integration_select.where = find_and_remove_time_filter(integration_select.where, time_filter)
            integration_selects = [integration_select]
        elif isinstance(time_filter, BinaryOperation) and time_filter.op == '=' and time_filter.args[1] == Latest():
            integration_select = Select(targets=[Star()],
                                        from_table=table,
                                        where=preparation_where,
                                        modifiers=query_modifiers,
                                        order_by=order_by,
                                        limit=Constant(predictor_window),
                                        )
            integration_select.where = find_and_remove_time_filter(integration_select.where, time_filter)
            integration_selects = [integration_select]
        elif isinstance(time_filter, BinaryOperation) and time_filter.op in ('>', '>='):
            time_filter_date = time_filter.args[1]
            preparation_time_filter_op = {'>': '<=', '>=': '<'}[time_filter.op]

            preparation_time_filter = BinaryOperation(preparation_time_filter_op, args=[Identifier(predictor_time_column_name), time_filter_date])
            preparation_where2 = replace_time_filter(preparation_where2, time_filter, preparation_time_filter)
            integration_select_1 = Select(targets=[Star()],
                                          from_table=table,
                                          where=add_order_not_null(preparation_where2),
                                          modifiers=query_modifiers,
                                          order_by=order_by,
                                          limit=Constant(predictor_window))

            integration_select_2 = Select(targets=[Star()],
                                          from_table=table,
                                          where=preparation_where,
                                          modifiers=query_modifiers,
                                          order_by=order_by)

            integration_selects = [integration_select_1, integration_select_2]
        else:
            integration_select = Select(targets=[Star()],
                                        from_table=table,
                                        where=preparation_where,
                                        modifiers=query_modifiers,
                                        order_by=order_by,
                                        )
            integration_selects = [integration_select]

        if len(predictor_group_by_names) == 0:
            # ts query without grouping
            # one or multistep
            if len(integration_selects) == 1:
                select_partition_step = self.get_integration_select_step(integration_selects[0])
            else:
                select_partition_step = MultipleSteps(
                    steps=[self.get_integration_select_step(s) for s in integration_selects], reduce='union')

            # fetch data step
            data_step = self.plan.add_step(select_partition_step)
        else:
            # inject $var to queries
            for integration_select in integration_selects:
                condition = integration_select.where
                for num, column in enumerate(predictor_group_by_names):
                    cond = BinaryOperation('=', args=[Identifier(column), Constant(f'$var[{column}]')])

                    # join to main condition
                    if condition is None:
                        condition = cond
                    else:
                        condition = BinaryOperation('and', args=[condition, cond])

                integration_select.where = condition
            # one or multistep
            if len(integration_selects) == 1:
                select_partition_step = self.get_integration_select_step(integration_selects[0])
            else:
                select_partition_step = MultipleSteps(
                    steps=[self.get_integration_select_step(s) for s in integration_selects], reduce='union')

            # get groping values
            no_time_filter_query = copy.deepcopy(query)
            no_time_filter_query.where = find_and_remove_time_filter(no_time_filter_query.where, time_filter)
            select_partitions_step = self.plan_fetch_timeseries_partitions(no_time_filter_query, table, predictor_group_by_names)

            # sub-query by every grouping value
            map_reduce_step = self.plan.add_step(MapReduceStep(values=select_partitions_step.result, reduce='union', step=select_partition_step))
            data_step = map_reduce_step

        predictor_step = self.plan.add_step(
            ApplyTimeseriesPredictorStep(
                output_time_filter=time_filter,
                namespace=predictor_namespace,
                dataframe=data_step.result,
                predictor=predictor,
            )
        )

        return {
            'predictor': predictor_step,
            'data': data_step,
            'saved_limit': saved_limit,
        }


    def plan_join_two_tables(self, join):
        select_left = join.left
        if isinstance(select_left, Identifier):
            select_left = Select(targets=[Star()], from_table=select_left)

        select_right = join.right
        if isinstance(select_right, Identifier):
            select_right = Select(targets=[Star()], from_table=select_right)

        select_left_step = self.plan_integration_select(select_left)
        select_right_step = self.plan_integration_select(select_right)

        if isinstance(join.left, Select) or isinstance(join.right, Select):
            raise PlanningException('Join table in select using integration is not supported yet')
        left_integration_name, left_table = self.get_integration_path_from_identifier_or_error(join.left)
        right_integration_name, right_table = self.get_integration_path_from_identifier_or_error(join.right)

        left_table_path = left_table.to_string(alias=False)
        right_table_path = right_table.to_string(alias=False)

        new_condition_args = []

        if join.condition is None:
            raise PlanningException('Join between two tables must have ON clause')
        for arg in join.condition.args:
            if isinstance(arg, Identifier):
                if left_table_path in arg.parts:
                    new_condition_args.append(
                        disambiguate_integration_column_identifier(arg, left_integration_name, left_table))
                elif right_table_path in arg.parts:
                    new_condition_args.append(
                        disambiguate_integration_column_identifier(arg, right_integration_name, right_table))
                else:
                    raise PlanningException(
                        f'Wrong table or no source table in join condition for column: {str(arg)}')
            else:
                new_condition_args.append(arg)
        new_join = copy.deepcopy(join)
        new_join.condition.args = new_condition_args
        new_join.left = Identifier(left_table_path, alias=left_table.alias)
        new_join.right = Identifier(right_table_path, alias=right_table.alias)

        # FIXME: INFORMATION_SCHEMA with condition
        # clear join condition for INFORMATION_SCHEMA
        if right_integration_name == 'INFORMATION_SCHEMA':
            new_join.condition = None

        return self.plan.add_step(JoinStep(left=select_left_step.result, right=select_right_step.result, query=new_join))

    def plan_group(self, query, last_step):
        # ! is not using yet

        # check group
        funcs = []
        for t in query.targets:
            if isinstance(t, Function):
                funcs.append(t.op.lower())
        agg_funcs = ['sum', 'min', 'max', 'avg', 'count', 'std']

        if (
                query.having is not None
                or query.group_by is not None
                or set(agg_funcs) & set(funcs)
        ):
            # is aggregate
            group_by_targets = []
            for t in query.targets:
                target_copy = copy.deepcopy(t)
                group_by_targets.append(target_copy)
            # last_step = self.plan.steps[-1]
            return GroupByStep(dataframe=last_step.result, columns=query.group_by, targets=group_by_targets)


    def plan_project(self, query, dataframe, ignore_doubles=False):
        out_identifiers = []

        for target in query.targets:
            if isinstance(target, Identifier) \
                    or isinstance(target, Star) \
                    or isinstance(target, Function) \
                    or isinstance(target, Constant):
                out_identifiers.append(target)
            else:
                new_identifier = Identifier(str(target.to_string(alias=False)), alias=target.alias)
                out_identifiers.append(new_identifier)
        return self.plan.add_step(ProjectStep(dataframe=dataframe, columns=out_identifiers, ignore_doubles=ignore_doubles))

    def get_aliased_fields(self, targets):
        # get aliases from select target
        aliased_fields = {}
        for target in targets:
            if target.alias is not None:
                aliased_fields[target.alias.to_string()] = target
        return aliased_fields

    def plan_join(self, query, integration=None):
        orig_query = query

        join = query.from_table
        join_left = join.left
        join_right = join.right

        if isinstance(join_left, Select):
            # dbt query.

            # move latest into subquery
            moved_conditions = []

            def move_latest(node, **kwargs):
                if isinstance(node, BinaryOperation):
                    if Latest() in node.args:
                        for arg in node.args:
                            if isinstance(arg, Identifier):
                                # remove table alias
                                arg.parts = [arg.parts[-1]]
                        moved_conditions.append(node)

            query_traversal(query.where, move_latest)

            # TODO make project step from query.target

            # TODO support complex query. Only one table is supported at the moment.
            if not isinstance(join_left.from_table, Identifier):
                raise PlanningException(f'Statement not supported: {query.to_string()}')

            # move properties to upper query
            query = join_left

            if query.from_table.alias is not None:
                table_alias = [query.from_table.alias.parts[0]]
            else:
                table_alias = [query.from_table.parts[-1]]

            # add latest to query.where
            for cond in moved_conditions:
                if query.where is not None:
                    query.where = BinaryOperation('and', args=[query.where, cond])
                else:
                    query.where = cond

            def add_aliases(node, is_table, **kwargs):
                if not is_table and isinstance(node, Identifier):
                    if len(node.parts) == 1:
                        # add table alias to field
                        node.parts = table_alias + node.parts

            query_traversal(query.where, add_aliases)

            if isinstance(query.from_table, Identifier):
                # DBT workaround: allow use tables without integration.
                #   if table.part[0] not in integration - take integration name from create table command
                if (
                    integration is not None
                    and query.from_table.parts[0] not in self.integrations
                ):
                    # add integration name to table
                    query.from_table.parts.insert(0, integration)

            join_left = join_left.from_table

            if orig_query.limit is not None:
                query.limit = orig_query.limit

        aliased_fields = self.get_aliased_fields(query.targets)

        recursively_check_join_identifiers_for_ambiguity(query.where)
        recursively_check_join_identifiers_for_ambiguity(query.group_by, aliased_fields=aliased_fields)
        recursively_check_join_identifiers_for_ambiguity(query.having)
        recursively_check_join_identifiers_for_ambiguity(query.order_by, aliased_fields=aliased_fields)

        if isinstance(join_left, Identifier) and isinstance(join_right, Identifier):
            if self.is_predictor(join_left) and self.is_predictor(join_right):
                raise PlanningException(f'Can\'t join two predictors {str(join_left.parts[0])} and {str(join_left.parts[1])}')

            predictor_namespace = None
            predictor = None
            table = None
            predictor_is_left = False
            if self.is_predictor(join_left):
                predictor_namespace, predictor = get_predictor_namespace_and_name_from_identifier(join_left, self.default_namespace)
                predictor_is_left = True
            else:
                table = join_left

            if self.is_predictor(join_right):
                predictor_namespace, predictor = get_predictor_namespace_and_name_from_identifier(join_right, self.default_namespace)
            else:
                table = join_right

            last_step = None
            if predictor:
                # One argument is a table, another is a predictor
                # Apply mindsdb model to result of last dataframe fetch
                # Then join results of applying mindsdb with table

                predictor_name = self.predictor_names[predictor.to_string(alias=False).lower()]
                if self.predictor_metadata[predictor_name].get('timeseries'):
                    predictor_steps = self.plan_timeseries_predictor(query, table, predictor_namespace, predictor)
                else:
                    predictor_steps = self.plan_predictor(query, table, predictor_namespace, predictor)

                # add join
                # Update reference
                _, table = self.get_integration_path_from_identifier_or_error(table)
                table_alias = table.alias or Identifier(table.to_string(alias=False).replace('.', '_'))

                left = Identifier(predictor_steps['predictor'].result.ref_name,
                                   alias=predictor.alias or Identifier(predictor.to_string(alias=False)))
                right = Identifier(predictor_steps['data'].result.ref_name, alias=table_alias)

                if not predictor_is_left:
                    # swap join
                    left, right = right, left
                new_join = Join(left=left, right=right, join_type=join.join_type)

                left = predictor_steps['predictor'].result
                right = predictor_steps['data'].result
                if not predictor_is_left:
                    # swap join
                    left, right = right, left

                last_step = self.plan.add_step(JoinStep(left=left, right=right, query=new_join))

                # limit from timeseries
                if predictor_steps.get('saved_limit'):
                    last_step = self.plan.add_step(LimitOffsetStep(dataframe=last_step.result,
                                                              limit=predictor_steps['saved_limit']))

            else:
                # Both arguments are tables, join results of 2 dataframe fetches

                join_step = self.plan_join_two_tables(join)
                last_step = join_step
                if query.where:
                    # FIXME: INFORMATION_SCHEMA with Where
                    right_integration_name, _ = self.get_integration_path_from_identifier_or_error(join.right)
                    if right_integration_name == 'INFORMATION_SCHEMA':
                        ...
                    else:
                        last_step = self.plan.add_step(FilterStep(dataframe=last_step.result, query=query.where))

                if query.group_by:
                    group_by_targets = []
                    for t in query.targets:
                        target_copy = copy.deepcopy(t)
                        target_copy.alias = None
                        group_by_targets.append(target_copy)
                    last_step = self.plan.add_step(GroupByStep(dataframe=last_step.result, columns=query.group_by, targets=group_by_targets))

                if query.having:
                    last_step = self.plan.add_step(FilterStep(dataframe=last_step.result, query=query.having))

                if query.order_by:
                    last_step = self.plan.add_step(OrderByStep(dataframe=last_step.result, order_by=query.order_by))

                if query.limit is not None or query.offset is not None:
                    limit = query.limit.value if query.limit is not None else None
                    offset = query.offset.value if query.offset is not None else None
                    last_step = self.plan.add_step(LimitOffsetStep(dataframe=last_step.result, limit=limit, offset=offset))

        else:
            raise PlanningException(f'Join of unsupported objects, currently only tables and predictors can be joined.')
        return self.plan_project(orig_query, last_step.result)

    def plan_create_table(self, query):
        if query.from_select is None:
            raise PlanningException(f'Not implemented "create table": {query.to_string()}')

        integration_name = query.name.parts[0]

        last_step = self.plan_select(query.from_select, integration=integration_name)

        # create table step
        self.plan.add_step(SaveToTable(
            table=query.name,
            dataframe=last_step,
            is_replace=query.is_replace,
        ))

    def plan_insert(self, query):
        if query.from_select is None:
            raise PlanningException(f'Support only insert from select')

        integration_name = query.table.parts[0]

        # plan sub-select first
        last_step = self.plan_select(query.from_select, integration=integration_name)

        table = query.table
        self.plan.add_step(InsertToTable(
            table=table,
            dataframe=last_step,
        ))

    def plan_select(self, query, integration=None):
        from_table = query.from_table

        if isinstance(from_table, Identifier):
            if self.is_predictor(from_table):
                return self.plan_select_from_predictor(query)
            else:
                return self.plan_integration_select(query)
        elif isinstance(from_table, Select):
            return self.plan_nested_select(query)
        elif isinstance(from_table, Join):
            return self.plan_join(query, integration=integration)
        elif isinstance(from_table, NativeQuery):
            integration = from_table.integration.parts[0].lower()
            step = FetchDataframeStep(integration=integration, raw_query=from_table.query)
            self.plan.add_step(step)
            sup_select = self.sub_select_step(query, step)
            if sup_select is not None:
                self.plan.add_step(sup_select)
        else:
            raise PlanningException(f'Unsupported from_table {type(from_table)}')

    def sub_select_step(self, query, prev_step):
        if (
            query.group_by is not None
            or query.order_by is not None
            or query.having is not None
            or query.distinct is True
            or query.where is not None
            or query.limit is not None
            or query.offset is not None
            or len(query.targets) != 1
            or not isinstance(query.targets[0], Star)
        ):
            if query.from_table.alias is not None:
                table_name = query.from_table.alias.parts[-1]
            else:
                table_name = None

            query2 = copy.deepcopy(query)
            query2.from_table = None
            return SubSelectStep(query2, prev_step.result, table_name=table_name)

    def plan_union(self, query):
        query1 = self.plan_select(query.left)
        query2 = self.plan_select(query.right)

        return self.plan.add_step(UnionStep(left=query1.result, right=query2.result, unique=query.unique))

    # method for compatibility
    def from_query(self, query=None):
        if query is None:
            query = self.query

        if isinstance(query, Select):
            self.plan_select(query)
        elif isinstance(query, Union):
            self.plan_union(query)
        elif isinstance(query, CreateTable):
            self.plan_create_table(query)
        elif isinstance(query, Insert):
            self.plan_insert(query)
        else:
            raise PlanningException(f'Unsupported query type {type(query)}')

        return self.plan


    def prepare_steps(self, query):
        statement_planner = PreparedStatementPlanner(self)

        # return generator
        return statement_planner.prepare_steps(query)

    def execute_steps(self, params=None):
        statement_planner = PreparedStatementPlanner(self)

        # return generator
        return statement_planner.execute_steps(params)

    # def fetch(self, row_count):
    #     statement_planner = PreparedStatementPlanner(self)
    #     return statement_planner.fetch(row_count)
    #
    # def close(self):
    #     statement_planner = PreparedStatementPlanner(self)
    #     return statement_planner.close()

    def get_statement_info(self):
        statement_planner = PreparedStatementPlanner(self)

        return statement_planner.get_statement_info()



