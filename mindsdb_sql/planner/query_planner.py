import copy
from collections import defaultdict
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import (Select, Identifier, Join, Star, BinaryOperation, Constant, OrderBy,
                                    BetweenOperation, Union, NullConstant, CreateTable, Function, Insert,
                                    Update, NativeQuery, Parameter, Delete)

from mindsdb_sql.parser.dialects.mindsdb.latest import Latest

from mindsdb_sql.planner import steps
from mindsdb_sql.planner.steps import (FetchDataframeStep, ProjectStep, JoinStep, ApplyPredictorStep,
                                       ApplyPredictorRowStep, FilterStep, GroupByStep, LimitOffsetStep, OrderByStep,
                                       UnionStep, MapReduceStep, MultipleSteps, ApplyTimeseriesPredictorStep,
                                       GetPredictorColumns, SaveToTable, InsertToTable, UpdateToTable, SubSelectStep,
                                       DeleteStep, DataStep)
from mindsdb_sql.planner.ts_utils import (validate_ts_where_condition, find_time_filter, replace_time_filter,
                                          find_and_remove_time_filter)
from mindsdb_sql.planner.utils import (disambiguate_predictor_column_identifier,
                                       get_deepest_select,
                                       recursively_extract_column_values,
                                       recursively_check_join_identifiers_for_ambiguity,
                                       query_traversal, filters_to_bin_op)
from mindsdb_sql.planner.query_plan import QueryPlan
from mindsdb_sql.planner import utils


def to_string(identifier):
    # alternative to AST.to_string() but without quoting
    return '.'.join(identifier.parts)


class Table:
    def __init__(self, node=None, ds=None, is_predictor=None):
        self.node = node
        self.is_predictor = is_predictor
        self.ds = ds
        self.name = to_string(node)
        self.columns = None
        # None is unknown
        self.columns_map = None
        self.keys = None
        if node.alias:
            self.alias = to_string(node.alias)
        else:
            self.alias = None
            # self.alias = self.name


class Column:
    def __init__(self, node=None, table=None, name=None, type=None):
        alias = None
        if node is not None:

            if isinstance(node, ast.Identifier):
                # set name
                name = node.parts[-1]  # ???

        else:
            if table is not None and name is not None:
                node = ast.Identifier(parts=[table.name, name])

        self.node = node  # link to AST
        self.alias = alias  # for ProjectStep

        self.is_star = False

        self.table = table  # link to AST table
        self.name = name  # column name
        self.type = type


class Statement:
    def __init__(self):
        self.columns = []
        # self.query = None
        self.params = None
        self.result = None

        # Tables on first level of select
        self.tables_lvl1 = None

        # mapping tables by name {'ta.ble': Table()}
        self.tables_map = None

        self.offset = 0


class QueryPlanner():

    def __init__(self,
                 query=None,
                 integrations: list = None,
                 predictor_namespace=None,
                 predictor_metadata: list = None,
                 default_namespace: str = None):
        self.query = query
        self.plan = QueryPlan()

        _projects = set()
        self.integrations = {}
        if integrations is not None:
            for integration in integrations:
                if isinstance(integration, dict):
                    integration_name = integration['name'].lower()
                    # it is project of system database
                    if integration['type'] != 'data':
                        _projects.add(integration_name)
                        continue
                else:
                    integration_name = integration.lower()
                    integration = {'name': integration}
                self.integrations[integration_name] = integration

        # allow to select from mindsdb namespace
        _projects.add('mindsdb')

        self.default_namespace = default_namespace

        # legacy parameter
        self.predictor_namespace = predictor_namespace.lower() if predictor_namespace else 'mindsdb'

        # map for lower names of predictors

        self.predictor_info = {}
        if isinstance(predictor_metadata, list):
            # convert to dict
            for predictor in predictor_metadata:
                if 'integration_name' in predictor:
                    integration_name = predictor['integration_name']
                else:
                    integration_name = self.predictor_namespace
                    predictor['integration_name'] = integration_name
                idx = f'{integration_name}.{predictor["name"]}'.lower()
                self.predictor_info[idx] = predictor
                _projects.add(integration_name.lower())
        elif isinstance(predictor_metadata, dict):
            # legacy behaviour
            for name, predictor in predictor_metadata.items():
                if '.' not in name:
                    if 'integration_name' in predictor:
                        integration_name = predictor['integration_name']
                    else:
                        integration_name = self.predictor_namespace
                        predictor['integration_name'] = integration_name
                    name = f'{integration_name}.{name}'.lower()
                    _projects.add(integration_name.lower())

                self.predictor_info[name] = predictor

        self.projects = list(_projects)
        self.databases = list(self.integrations.keys()) + self.projects

        self.statement = None

    def is_predictor(self, identifier):
        if not isinstance(identifier, Identifier):
            return False
        return self.get_predictor(identifier) is not None

    def get_predictor(self, identifier):
        name_parts = list(identifier.parts)

        version = None
        if len(name_parts) > 1 and name_parts[-1].isdigit():
            # last part is version
            version = name_parts[-1]
            name_parts = name_parts[:-1]

        name = name_parts[-1]

        namespace = None
        if len(name_parts) > 1:
            namespace = name_parts[-2]
        else:
            if self.default_namespace is not None:
                namespace = self.default_namespace

        idx_ar = [name]
        if namespace is not None:
            idx_ar.insert(0, namespace)

        idx = '.'.join(idx_ar).lower()
        info = self.predictor_info.get(idx)
        if info is not None:
            info['version'] = version
            info['name'] = name
        return info

    def prepare_integration_select(self, database, query):
        # replacement for 'utils.recursively_disambiguate_*' functions from utils
        #   main purpose: make tests working (don't change planner outputs)
        # can be removed in future (with adapting the tests) except 'cut integration part' block

        def _prepare_integration_select(node, is_table, is_target, parent_query, **kwargs):
            if not isinstance(node, Identifier):
                return

            # cut integration part
            if len(node.parts) > 1 and node.parts[0].lower() == database:
                node.parts.pop(0)

            if not hasattr(parent_query, 'from_table'):
                return

            table = parent_query.from_table
            if not is_table:
                # add table name or alias for identifiers
                if isinstance(table, Join):
                    # skip for join
                    return
                if table.alias is not None:
                    prefix = table.alias.parts
                else:
                    prefix = table.parts

                if len(node.parts) > 1:
                    if node.parts[:len(prefix)] != prefix:
                        raise PlanningException(f'Tried to query column {node.to_string()} from table'
                                                f' {table.to_string()}, but a different table name has been specified.')
                else:
                    node.parts = prefix + node.parts

                # keep column name for target
                if is_target:
                    if node.alias is None:
                        last_part = node.parts[-1]
                        if isinstance(last_part, str):
                            node.alias = Identifier(parts=[node.parts[-1]])

        query_traversal(query, _prepare_integration_select)

    def get_integration_select_step(self, select):
        integration_name, table = self.resolve_database_table(select.from_table)

        fetch_df_select = copy.deepcopy(select)
        self.prepare_integration_select(integration_name, fetch_df_select)

        # remove predictor params
        if fetch_df_select.using is not None:
            fetch_df_select.using = None

        return FetchDataframeStep(integration=integration_name, query=fetch_df_select)

    def plan_integration_select(self, select):
        """Plan for a select query that can be fully executed in an integration"""

        return self.plan.add_step(self.get_integration_select_step(select))

    def resolve_database_table(self, node: Identifier):
        # resolves integration name and table name

        parts = node.parts.copy()
        alias = None
        if node.alias is not None:
            alias = node.alias.copy()

        database = self.default_namespace

        if len(parts) > 1:
            if parts[0].lower() in self.databases:
                database = parts.pop(0).lower()

        if database is None:
            raise PlanningException(f'Integration not found for: {node}')

        return database, Identifier(parts=parts, alias=alias)

    def get_query_info(self, query):
        # get all predictors
        mdb_entities = []
        predictors = []
        # projects = set()
        integrations = set()

        def find_predictors(node, is_table, **kwargs):

            if is_table:
                if isinstance(node, ast.Identifier):
                    integration, _ = self.resolve_database_table(node)

                    if self.is_predictor(node):
                        predictors.append(node)

                    if integration in self.projects:
                        # it is project
                        mdb_entities.append(node)

                    elif integration is not None:
                        integrations.add(integration)
                if isinstance(node, ast.NativeQuery) or isinstance(node, ast.Data):
                    mdb_entities.append(node)

        query_traversal(query, find_predictors)
        return {'mdb_entities': mdb_entities, 'integrations': integrations, 'predictors': predictors}

    def get_nested_selects_plan_fnc(self, main_integration, force=False):
        # returns function for traversal over query and inject fetch data query instead of subselects
        def find_selects(node, **kwargs):
            if isinstance(node, Select):
                query_info2 = self.get_query_info(node)
                if force or (
                        len(query_info2['integrations']) > 1 or
                        main_integration not in query_info2['integrations'] or
                        len(query_info2['mdb_entities']) > 0
                ):
                    # need to execute in planner

                    node.parentheses = False
                    last_step = self.plan_select(node)

                    node2 = Parameter(last_step.result)

                    return node2

        return find_selects

    def plan_select_identifier(self, query):
        query_info = self.get_query_info(query)

        if len(query_info['integrations']) == 0 and len(query_info['predictors']) >= 1:
            # select from predictor
            return self.plan_select_from_predictor(query)
        elif len(query_info['integrations']) == 1 and len(query_info['mdb_entities']) == 0:

            int_name = list(query_info['integrations'])[0]
            if self.integrations.get(int_name, {}).get('class_type') != 'api':
                # one integration without predictors, send all query to integration
                return self.plan_integration_select(query)

        # find subselects
        main_integration, _ = self.resolve_database_table(query.from_table)
        is_api_db = self.integrations.get(main_integration, {}).get('class_type') == 'api'

        find_selects = self.get_nested_selects_plan_fnc(main_integration, force=is_api_db)
        query.targets = query_traversal(query.targets, find_selects)
        query_traversal(query.where, find_selects)

        # get info of updated query
        query_info = self.get_query_info(query)

        if len(query_info['predictors']) >= 1:
            # select from predictor
            return self.plan_select_from_predictor(query)
        else:
            # fallback to integration
            return self.plan_integration_select(query)

    def plan_nested_select(self, select):

        query_info = self.get_query_info(select)
        # get all predictors

        if (
                len(query_info['mdb_entities']) == 0
                and len(query_info['integrations']) == 1
                and 'files' not in query_info['integrations']
                and 'views' not in query_info['integrations']
        ):
            int_name = list(query_info['integrations'])[0]
            if self.integrations.get(int_name, {}).get('class_type') != 'api':
                # if no predictor inside = run as is
                return self.plan_integration_nested_select(select)

        return self.plan_mdb_nested_select(select)

    def plan_integration_nested_select(self, select):
        fetch_df_select = copy.deepcopy(select)
        deepest_select = get_deepest_select(fetch_df_select)
        integration_name, table = self.resolve_database_table(deepest_select.from_table)
        self.prepare_integration_select(integration_name, deepest_select)
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

    def get_predictor_namespace_and_name_from_identifier(self, identifier):
        new_identifier = copy.deepcopy(identifier)

        info = self.get_predictor(identifier)
        namespace = info['integration_name']

        parts = [namespace, info['name']]
        if info['version'] is not None:
            parts.append(info['version'])
        new_identifier.parts = parts

        return namespace, new_identifier

    def plan_select_from_predictor(self, select):
        predictor_namespace, predictor = self.get_predictor_namespace_and_name_from_identifier(select.from_table)

        if select.where == BinaryOperation('=', args=[Constant(1), Constant(0)]):
            # Hardcoded mysql way of getting predictor columns
            predictor_identifier = utils.get_predictor_name_identifier(predictor)
            predictor_step = self.plan.add_step(
                GetPredictorColumns(namespace=predictor_namespace,
                                    predictor=predictor_identifier)
            )
        else:
            new_query_targets = []
            for target in select.targets:
                if isinstance(target, Identifier):
                    new_query_targets.append(
                        disambiguate_predictor_column_identifier(target, predictor))
                elif type(target) in (Star, Constant, Function):
                    new_query_targets.append(target)
                else:
                    raise PlanningException(f'Unknown select target {type(target)}')

            if select.group_by or select.having:
                raise PlanningException(
                    f'Unsupported operation when querying predictor. Only WHERE is allowed and required.')

            row_dict = {}
            where_clause = select.where
            if not where_clause:
                raise PlanningException(f'WHERE clause required when selecting from predictor')

            predictor_identifier = utils.get_predictor_name_identifier(predictor)
            recursively_extract_column_values(where_clause, row_dict, predictor_identifier)

            params = None
            if select.using is not None:
                params = select.using
            predictor_step = self.plan.add_step(
                ApplyPredictorRowStep(
                    namespace=predictor_namespace,
                    predictor=predictor_identifier,
                    row_dict=row_dict,
                    params=params
                )
            )
        project_step = self.plan_project(select, predictor_step.result)
        return project_step

    def plan_predictor(self, query, table, predictor_namespace, predictor):
        int_select = copy.deepcopy(query)
        int_select.targets = [Star()]  # TODO why not query.targets?
        int_select.from_table = table

        predictor_alias = None
        if predictor.alias is not None:
            predictor_alias = predictor.alias.parts[0]

        params = {}
        if query.using is not None:
            params = query.using

        binary_ops = []
        table_filters = []
        model_filters = []

        def split_filters(node, **kwargs):
            # split conditions between model and table

            if isinstance(node, BinaryOperation):
                op = node.op.lower()

                binary_ops.append(op)

                if op in ['and', 'or']:
                    return

                arg1, arg2 = node.args
                if not isinstance(arg1, Identifier):
                    arg1, arg2 = arg2, arg1

                if isinstance(arg1, Identifier) and isinstance(arg2, (Constant, Parameter)) and len(arg1.parts) > 1:
                    model = Identifier(parts=arg1.parts[:-1])

                    if (
                            self.is_predictor(model)
                            or (
                            len(model.parts) == 1 and model.parts[0] == predictor_alias
                    )
                    ):
                        model_filters.append(node)
                        return
                table_filters.append(node)

        # find subselects
        main_integration, _ = self.resolve_database_table(table)
        find_selects = self.get_nested_selects_plan_fnc(main_integration, force=True)
        query_traversal(int_select.where, find_selects)

        # split conditions
        query_traversal(int_select.where, split_filters)

        if len(model_filters) > 0 and 'or' not in binary_ops:
            int_select.where = filters_to_bin_op(table_filters)

        integration_select_step = self.plan_integration_select(int_select)

        predictor_identifier = utils.get_predictor_name_identifier(predictor)

        if len(params) == 0:
            params = None

        row_dict = None
        if model_filters:
            row_dict = {}
            for el in model_filters:
                if isinstance(el.args[0], Identifier) and el.op == '=':
                    if isinstance(el.args[1], (Constant, Parameter)):
                        row_dict[el.args[0].parts[-1]] = el.args[1].value

        last_step = self.plan.add_step(ApplyPredictorStep(
            namespace=predictor_namespace,
            dataframe=integration_select_step.result,
            predictor=predictor_identifier,
            params=params,
            row_dict=row_dict
        ))

        return {
            'predictor': last_step,
            'data': integration_select_step,
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

        predictor_metadata = self.get_predictor(predictor)

        predictor_time_column_name = predictor_metadata['order_by_column']
        predictor_group_by_names = predictor_metadata['group_by_columns']
        if predictor_group_by_names is None:
            predictor_group_by_names = []
        predictor_window = predictor_metadata['window']

        if query.order_by:
            raise PlanningException(
                f'Can\'t provide ORDER BY to time series predictor, it will be taken from predictor settings. Found: {query.order_by}')

        saved_limit = None
        if query.limit is not None:
            saved_limit = query.limit.value

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

            preparation_time_filter = BinaryOperation(preparation_time_filter_op,
                                                      args=[Identifier(predictor_time_column_name), time_filter_date])
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
            select_partitions_step = self.plan_fetch_timeseries_partitions(no_time_filter_query, table,
                                                                           predictor_group_by_names)

            # sub-query by every grouping value
            map_reduce_step = self.plan.add_step(
                MapReduceStep(values=select_partitions_step.result, reduce='union', step=select_partition_step))
            data_step = map_reduce_step

        predictor_identifier = utils.get_predictor_name_identifier(predictor)

        params = None
        if query.using is not None:
            params = query.using
        predictor_step = self.plan.add_step(
            ApplyTimeseriesPredictorStep(
                output_time_filter=time_filter,
                namespace=predictor_namespace,
                dataframe=data_step.result,
                predictor=predictor_identifier,
                params=params,
            )
        )

        return {
            'predictor': predictor_step,
            'data': data_step,
            'saved_limit': saved_limit,
        }

    def plan_join_tables(self, query_in):
        query = copy.deepcopy(query_in)

        # replace sub selects, with identifiers with links to original selects
        def replace_subselects(node, **args):
            if isinstance(node, Select) or isinstance(node, NativeQuery) or isinstance(node, ast.Data):
                name = f't_{id(node)}'
                node2 = Identifier(name, alias=node.alias)

                # save in attribute
                if isinstance(node, NativeQuery) or isinstance(node, ast.Data):
                    # wrap to select
                    node = Select(targets=[Star()], from_table=node)
                node2.sub_select = node
                return node2

        query_traversal(query.from_table, replace_subselects)

        def resolve_table(table):
            # gets integration for table and name to access to it
            table = copy.deepcopy(table)
            # get possible table aliases
            aliases = []
            if table.alias is not None:
                # to lowercase
                parts = tuple(map(str.lower, table.alias.parts))
                aliases.append(parts)
            else:
                for i in range(0, len(table.parts)):
                    parts = table.parts[i:]
                    parts = tuple(map(str.lower, parts))
                    aliases.append(parts)

            # try to use default namespace
            integration = self.default_namespace
            if len(table.parts) > 0:
                if table.parts[0] in self.databases:
                    integration = table.parts.pop(0)
                else:
                    integration = self.default_namespace

            if integration is None and not hasattr(table, 'sub_select'):
                raise PlanningException(f'Integration not found for: {table}')

            sub_select = getattr(table, 'sub_select', None)

            return dict(
                integration=integration,
                table=table,
                aliases=aliases,
                conditions=[],
                sub_select=sub_select,
            )

        # get all join tables, form join sequence

        tables_idx = {}

        def get_join_sequence(node):
            sequence = []
            if isinstance(node, Identifier):
                # resolve identifier

                table_info = resolve_table(node)
                for alias in table_info['aliases']:
                    tables_idx[alias] = table_info

                table_info['predictor_info'] = self.get_predictor(node)

                sequence.append(table_info)

            elif isinstance(node, Join):
                # create sequence: 1)table1, 2)table2, 3)join 1 2, 4)table 3, 5)join 3 4

                # put all tables before
                sequence2 = get_join_sequence(node.left)
                for item in sequence2:
                    sequence.append(item)

                sequence2 = get_join_sequence(node.right)
                if len(sequence2) != 1:
                    raise PlanningException('Unexpected join nesting behavior')

                # put next table
                sequence.append(sequence2[0])

                # put join
                sequence.append(node)

            else:
                raise NotImplementedError()
            return sequence

        join_sequence = get_join_sequence(query.from_table)

        # get conditions for tables
        binary_ops = []

        def _check_identifiers(node, is_table, **kwargs):
            if not is_table and isinstance(node, Identifier):
                if len(node.parts) > 1:
                    parts = tuple(map(str.lower, node.parts[:-1]))
                    if parts not in tables_idx:
                        raise PlanningException(f'Table not found for identifier: {node.to_string()}')

                    # # replace identifies name
                    col_parts = list(tables_idx[parts]['aliases'][-1])
                    col_parts.append(node.parts[-1])
                    node.parts = col_parts

        query_traversal(query, _check_identifiers)

        def _check_condition(node, **kwargs):
            if isinstance(node, BinaryOperation):
                binary_ops.append(node.op)

                node2 = copy.deepcopy(node)
                arg1, arg2 = node2.args
                if not isinstance(arg1, Identifier):
                    arg1, arg2 = arg2, arg1

                if isinstance(arg1, Identifier) and isinstance(arg2, (Constant, Parameter)):
                    if len(arg1.parts) < 2:
                        return

                    # to lowercase
                    parts = tuple(map(str.lower, arg1.parts[:-1]))
                    if parts not in tables_idx:
                        raise PlanningException(f'Table not found for identifier: {arg1.to_string()}')

                    # keep only column name
                    arg1.parts = [arg1.parts[-1]]

                    node2._orig_node = node
                    tables_idx[parts]['conditions'].append(node2)

        find_selects = self.get_nested_selects_plan_fnc(self.default_namespace, force=True)
        query_traversal(query.where, find_selects)

        query_traversal(query.where, _check_condition)

        # create plan
        # TODO add optimization: one integration without predictor
        step_stack = []
        for item in join_sequence:
            if isinstance(item, dict):
                table_name = item['table']
                predictor_info = item['predictor_info']

                if item['sub_select'] is not None:
                    # is sub select
                    item['sub_select'].alias = None
                    item['sub_select'].parentheses = False
                    step = self.plan_select(item['sub_select'])

                    where = filters_to_bin_op(item['conditions'])

                    # apply table alias
                    query2 = Select(targets=[Star()], where=where)
                    if item['table'].alias is None:
                        raise PlanningException(
                            f'Subselect in join have to be aliased: {item["sub_select"].to_string()}')
                    table_name = item['table'].alias.parts[-1]

                    add_absent_cols = False
                    if hasattr(item['sub_select'], 'from_table') and \
                            isinstance(item['sub_select'].from_table, ast.Data):
                        add_absent_cols = True

                    step2 = SubSelectStep(query2, step.result, table_name=table_name, add_absent_cols=add_absent_cols)
                    step2 = self.plan.add_step(step2)
                    step_stack.append(step2)
                elif predictor_info is not None:
                    if len(step_stack) == 0:
                        raise NotImplementedError("Predictor can't be first element of join syntax")
                    if predictor_info.get('timeseries'):
                        raise NotImplementedError("TS predictor is not supported here yet")
                    data_step = step_stack[-1]
                    row_dict = None
                    if item['conditions']:
                        row_dict = {}
                        for el in item['conditions']:
                            if isinstance(el.args[0], Identifier) and el.op == '=':

                                if isinstance(el.args[1], (Constant, Parameter)):
                                    row_dict[el.args[0].parts[-1]] = el.args[1].value

                                # exclude condition
                                item['conditions'][0]._orig_node.args = [Constant(0), Constant(0)]

                    predictor_step = self.plan.add_step(ApplyPredictorStep(
                        namespace=item['integration'],
                        dataframe=data_step.result,
                        predictor=table_name,
                        params=query.using,
                        row_dict=row_dict,
                    ))
                    step_stack.append(predictor_step)
                else:
                    # is table
                    query2 = Select(from_table=table_name, targets=[Star()])
                    # parts = tuple(map(str.lower, table_name.parts))
                    conditions = item['conditions']
                    if 'or' in binary_ops:
                        # not use conditions
                        conditions = []

                    for cond in conditions:
                        if query2.where is not None:
                            query2.where = BinaryOperation('and', args=[query2.where, cond])
                        else:
                            query2.where = cond

                    # TODO use self.get_integration_select_step(query2)
                    step = FetchDataframeStep(integration=item['integration'], query=query2)
                    self.plan.add_step(step)
                    step_stack.append(step)
            elif isinstance(item, Join):
                step_right = step_stack.pop()
                step_left = step_stack.pop()

                new_join = copy.deepcopy(item)

                # TODO
                new_join.left = Identifier('tab1')
                new_join.right = Identifier('tab2')
                new_join.implicit = False

                step = self.plan.add_step(JoinStep(left=step_left.result, right=step_right.result, query=new_join))

                step_stack.append(step)

        query_in.where = query.where
        return step_stack.pop()

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

        if len(query.targets) == 1 and isinstance(query.targets[0], Star):
            last_step = self.plan.steps[-1]
            return last_step

        for target in query.targets:
            if isinstance(target, Identifier) \
                    or isinstance(target, Star) \
                    or isinstance(target, Function) \
                    or isinstance(target, Constant) \
                    or isinstance(target, BinaryOperation):
                out_identifiers.append(target)
            else:
                new_identifier = Identifier(str(target.to_string(alias=False)), alias=target.alias)
                out_identifiers.append(new_identifier)
        return self.plan.add_step(
            ProjectStep(dataframe=dataframe, columns=out_identifiers, ignore_doubles=ignore_doubles))

    def get_aliased_fields(self, targets):
        # get aliases from select target
        aliased_fields = {}
        for target in targets:
            if target.alias is not None:
                aliased_fields[target.alias.to_string()] = target
        return aliased_fields

    def adapt_dbt_query(self, query, integration):
        orig_query = query

        join = query.from_table
        join_left = join.left

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
        # if not isinstance(join_left.from_table, Identifier):
        #     raise PlanningException(f'Statement not supported: {query.to_string()}')

        # move properties to upper query
        query = join_left

        if query.from_table.alias is not None:
            table_alias = [query.from_table.alias.parts[0]]
        else:
            table_alias = query.from_table.parts

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
                    and query.from_table.parts[0] not in self.databases
            ):
                # add integration name to table
                query.from_table.parts.insert(0, integration)

        join_left = join_left.from_table

        if orig_query.limit is not None:
            if query.limit is None or query.limit.value > orig_query.limit.value:
                query.limit = orig_query.limit
        query.parentheses = False
        query.alias = None

        return query, join_left

    def plan_join(self, query, integration=None):
        orig_query = query

        join = query.from_table
        join_left = join.left
        join_right = join.right

        if isinstance(join_left, Select) and isinstance(join_left.from_table, Identifier):
            if self.is_predictor(join_right) and self.get_predictor(join_right).get('timeseries'):
                query, join_left = self.adapt_dbt_query(query, integration)

        aliased_fields = self.get_aliased_fields(query.targets)

        # check predictor
        predictor = None
        table = None
        predictor_namespace = None
        predictor_is_left = False

        if not self.is_predictor(join_right):
            # predictor not in the right, swap
            join_left, join_right = join_right, join_left
            predictor_is_left = True

        if self.is_predictor(join_right):
            # predictor is in the right now

            if self.is_predictor(join_left):
                # left is predictor too

                raise PlanningException(
                    f'Can\'t join two predictors {str(join_left.parts[0])} and {str(join_left.parts[1])}')
            elif isinstance(join_left, Identifier):
                # the left is table
                predictor_namespace, predictor = self.get_predictor_namespace_and_name_from_identifier(join_right)

                table = join_left

            last_step = None
            if predictor:
                # One argument is a table, another is a predictor
                # Apply mindsdb model to result of last dataframe fetch
                # Then join results of applying mindsdb with table

                recursively_check_join_identifiers_for_ambiguity(query.where)
                recursively_check_join_identifiers_for_ambiguity(query.group_by, aliased_fields=aliased_fields)
                recursively_check_join_identifiers_for_ambiguity(query.having)
                recursively_check_join_identifiers_for_ambiguity(query.order_by, aliased_fields=aliased_fields)

                if self.get_predictor(predictor).get('timeseries'):
                    predictor_steps = self.plan_timeseries_predictor(query, table, predictor_namespace, predictor)
                else:
                    predictor_steps = self.plan_predictor(query, table, predictor_namespace, predictor)

                # add join
                # Update reference

                left = Identifier(predictor_steps['predictor'].result.ref_name)
                right = Identifier(predictor_steps['data'].result.ref_name)

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

        if predictor is None:

            query_info = self.get_query_info(query)

            # can we send all query to integration?
            if (
                    len(query_info['mdb_entities']) == 0
                    and len(query_info['integrations']) == 1
                    and 'files' not in query_info['integrations']
                    and 'views' not in query_info['integrations']
            ):
                int_name = list(query_info['integrations'])[0]
                if self.integrations.get(int_name, {}).get('class_type') != 'api':
                    # if no predictor inside = run as is
                    self.prepare_integration_select(int_name, query)

                    last_step = self.plan.add_step(FetchDataframeStep(integration=int_name, query=query))

                    return last_step

            # Both arguments are tables, join results of 2 dataframe fetches

            join_step = self.plan_join_tables(query)
            last_step = join_step
            if query.where:
                # FIXME: Tableau workaround, INFORMATION_SCHEMA with Where
                if isinstance(join.right, Identifier) \
                        and self.resolve_database_table(join.right)[0] == 'INFORMATION_SCHEMA':
                    pass
                else:
                    last_step = self.plan.add_step(FilterStep(dataframe=last_step.result, query=query.where))

            if query.group_by:
                group_by_targets = []
                for t in query.targets:
                    target_copy = copy.deepcopy(t)
                    target_copy.alias = None
                    group_by_targets.append(target_copy)
                last_step = self.plan.add_step(
                    GroupByStep(dataframe=last_step.result, columns=query.group_by, targets=group_by_targets))

            if query.having:
                last_step = self.plan.add_step(FilterStep(dataframe=last_step.result, query=query.having))

            if query.order_by:
                last_step = self.plan.add_step(OrderByStep(dataframe=last_step.result, order_by=query.order_by))

            if query.limit is not None or query.offset is not None:
                limit = query.limit.value if query.limit is not None else None
                offset = query.offset.value if query.offset is not None else None
                last_step = self.plan.add_step(LimitOffsetStep(dataframe=last_step.result, limit=limit, offset=offset))

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
        table = query.table
        if query.from_select is not None:
            integration_name = query.table.parts[0]

            # plan sub-select first
            last_step = self.plan_select(query.from_select, integration=integration_name)

            self.plan.add_step(InsertToTable(
                table=table,
                dataframe=last_step,
            ))
        else:
            self.plan.add_step(InsertToTable(
                table=table,
                query=query,
            ))

    def plan_update(self, query):
        last_step = None
        if query.from_select is not None:
            integration_name = query.table.parts[0]
            last_step = self.plan_select(query.from_select, integration=integration_name)

        # plan sub-select first
        update_command = copy.deepcopy(query)
        # clear subselect
        update_command.from_select = None

        table = query.table
        self.plan.add_step(UpdateToTable(
            table=table,
            dataframe=last_step,
            update_command=update_command
        ))

    def plan_delete(self, query: Delete):

        # find subselects
        main_integration, _ = self.resolve_database_table(query.table)

        is_api_db = self.integrations.get(main_integration, {}).get('class_type') == 'api'

        find_selects = self.get_nested_selects_plan_fnc(main_integration, force=is_api_db)
        query_traversal(query.where, find_selects)

        self.prepare_integration_select(main_integration, query.where)

        return self.plan.add_step(DeleteStep(
            table=query.table,
            where=query.where
        ))

    def plan_select(self, query, integration=None):
        from_table = query.from_table

        if isinstance(from_table, Identifier):
            return self.plan_select_identifier(query)
        elif isinstance(from_table, Select):
            return self.plan_nested_select(query)
        elif isinstance(from_table, Join):
            return self.plan_join(query, integration=integration)
        elif isinstance(from_table, NativeQuery):
            integration = from_table.integration.parts[0].lower()
            step = FetchDataframeStep(integration=integration, raw_query=from_table.query)
            last_step = self.plan.add_step(step)
            sup_select = self.sub_select_step(query, step)
            if sup_select is not None:
                last_step = self.plan.add_step(sup_select)
            return last_step
        elif isinstance(from_table, ast.Data):
            step = DataStep(from_table.data)
            last_step = self.plan.add_step(step)
            sup_select = self.sub_select_step(query, step, add_absent_cols=True)
            if sup_select is not None:
                last_step = self.plan.add_step(sup_select)
            return last_step
        else:
            raise PlanningException(f'Unsupported from_table {type(from_table)}')

    def sub_select_step(self, query, prev_step, add_absent_cols=False):
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
            return SubSelectStep(query2, prev_step.result, table_name=table_name, add_absent_cols=add_absent_cols)

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
        elif isinstance(query, Update):
            self.plan_update(query)
        elif isinstance(query, Delete):
            self.plan_delete(query)
        else:
            raise PlanningException(f'Unsupported query type {type(query)}')

        return self.plan

    def prepare_steps(self, query):
        stmt = Statement()
        self.statement = stmt

        self.query = query

        query = copy.deepcopy(query)

        params = utils.get_query_params(query)

        stmt.params = params

        # get columns
        if isinstance(query, ast.Select):
            # prepare select
            return self.prepare_select(query)
        if isinstance(query, ast.Union):
            # get column definition only from select
            return self.prepare_select(query.left)
        if isinstance(query, ast.Insert):
            # return self.prepare_insert(query)
            # TODO do we need columns?
            return []
        if isinstance(query, ast.Delete):
            ...
            # TODO do we need columns?
            return []
        if isinstance(query, ast.Show):
            return self.prepare_show(query)
        else:

            # do nothing
            return []
            # raise NotImplementedError(query.__name__)

    def prepare_select(self, query):
        # prepare select with or without predictor

        stmt = self.statement

        # get all predictors
        query_predictors = []

        def find_predictors(node, is_table, **kwargs):
            if is_table and isinstance(node, ast.Identifier):
                if self.is_predictor(node):
                    query_predictors.append(node)

        utils.query_traversal(query, find_predictors)

        # only 1 predictor is allowed
        # if len(query_predictors) > 1:
        #     raise PlanningException(f'To many predictors in query: {len(query_predictors)}')

        # === get all tables from 1st level of query ===
        stmt.tables_map = {}
        stmt.tables_lvl1 = []
        if query.from_table is not None:

            if isinstance(query.from_table, ast.Join):
                # get all tables
                join_tables = utils.convert_join_to_list(query.from_table)
            else:
                join_tables = [dict(table=query.from_table)]

            if isinstance(query.from_table, ast.Select):
                # nested select, get only last select
                join_tables = [
                    dict(
                        table=utils.get_deepest_select(query.from_table).from_table
                    )
                ]

            for i, join_table in enumerate(join_tables):
                table = join_table['table']
                if isinstance(table, ast.Identifier):
                    tbl = self.table_from_identifier(table)

                    if tbl.is_predictor:
                        # Is the last table?
                        if i + 1 < len(join_tables):
                            raise PlanningException(f'Predictor must be last table in query')

                    stmt.tables_lvl1.append(tbl)
                    for key in tbl.keys:
                        stmt.tables_map[key] = tbl

                else:
                    # don't add unknown table to looking list
                    continue

        # is there any predictors at other levels?
        lvl1_predictors = [i for i in stmt.tables_lvl1 if i.is_predictor]
        if len(query_predictors) != len(lvl1_predictors):
            raise PlanningException('Predictor is not at first level')

        # === get targets ===
        columns = []
        get_all_tables = False
        for t in query.targets:

            column = Column(t)

            # column alias
            alias = None
            if t.alias is not None:
                alias = to_string(t.alias)

            if isinstance(t, ast.Star):
                if len(stmt.tables_lvl1) == 0:
                    # if "from" is emtpy we can't make plan
                    raise PlanningException("Can't find table")

                column.is_star = True
                get_all_tables = True

            elif isinstance(t, ast.Identifier):
                if alias is None:
                    alias = t.parts[-1]

                table = self.get_table_of_column(t)
                if table is None:
                    # table is not known
                    get_all_tables = True
                else:
                    column.table = table

            elif isinstance(t, ast.Constant):
                if alias is None:
                    alias = str(t.value)
                column.type = self.get_type_of_var(t.value)
            elif isinstance(t, ast.Function):
                # mysql function
                if t.op == 'connection_id':
                    column.type = 'integer'
                else:
                    column.type = 'str'
            else:
                # TODO go down into lower level.
                #  It can be function, operation, select.
                #  But now show it as string

                # TODO add several known types for function, i.e ABS-int

                # TODO TypeCast - as casted type
                column.type = 'str'

            if alias is not None:
                column.alias = alias
            columns.append(column)

        # === get columns from tables ===
        request_tables = set()
        for column in columns:
            if column.table is not None:
                request_tables.add(column.table.name)

        for table in stmt.tables_lvl1:
            if get_all_tables or table.name in request_tables:
                if table.is_predictor:
                    step = steps.GetPredictorColumns(namespace=table.ds, predictor=table.node)
                else:
                    step = steps.GetTableColumns(namespace=table.ds, table=table.name)
                yield step

                if step.result_data is not None:
                    # save results

                    if len(step.result_data['tables']) > 0:
                        table_info = step.result_data['tables'][0]
                        columns_info = step.result_data['columns'][table_info]

                        table.columns = []
                        table.ds = table_info[0]
                        for col in columns_info:
                            if isinstance(col, tuple):
                                # is predictor
                                col = dict(name=col[0], type='str')
                            table.columns.append(
                                Column(
                                    name=col['name'],
                                    type=col['type'],
                                )
                            )

                    # map by names
                    table.columns_map = {
                        i.name.upper(): i
                        for i in table.columns
                    }

        # === create columns list ===
        columns_result = []
        for i, column in enumerate(columns):
            if column.is_star:
                # add data from all tables
                for table in stmt.tables_lvl1:
                    if table.columns is None:
                        raise PlanningException(f'Table is not found {table.name}')

                    for col in table.columns:
                        # col = {name: 'col', type: 'str'}
                        column2 = Column(table=table, name=col.name)
                        column2.alias = col.name
                        column2.type = col.type

                        columns_result.append(column2)

                # to next column
                continue

            elif column.name is not None:
                # is Identifier
                col_name = column.name.upper()
                if column.table is not None:
                    table = column.table
                    if table.columns_map is not None:
                        if col_name in table.columns_map:
                            column.type = table.columns_map[col_name].type
                        else:
                            # print(col_name, table.name, query.to_string())
                            # continue
                            raise PlanningException(f'Column not found {col_name}')


                else:
                    # table is not found, looking for in all tables
                    for table in stmt.tables_lvl1:
                        if table.columns_map is not None:
                            col = table.columns_map.get(col_name)
                            if col is not None:
                                column.type = col.type
                                column.table = table
                                break

            # forcing alias
            if column.alias is None:
                column.alias = f'column_{i}'

            # forcing type
            if column.type is None:
                column.type = 'str'

            columns_result.append(column)

        # save columns
        stmt.columns = columns_result

    def table_from_identifier(self, table):
        # disambiguate
        if self.is_predictor(table):
            ds, table = self.get_predictor_namespace_and_name_from_identifier(table)
            is_predictor = True

        else:
            ds, table = self.resolve_database_table(table)
            is_predictor = False

        if table.alias is not None:
            # access by alias if table is having alias
            keys = [to_string(table.alias)]

        else:
            # access by table name, in all variants
            keys = []
            parts = []
            # in reverse order
            for p in table.parts[::-1]:
                parts.insert(0, p)
                keys.append('.'.join(parts))

        # remember table
        tbl = Table(
            ds=ds,
            node=table,
            is_predictor=is_predictor
        )
        tbl.keys = keys

        return tbl

    def get_table_of_column(self, t):

        tables_map = self.statement.tables_map

        # get tables to check
        if len(t.parts) > 1:
            # try to find table
            table_parts = t.parts[:-1]
            table_name = '.'.join(table_parts)
            if table_name in tables_map:
                return tables_map[table_name]

            elif len(table_parts) > 1:
                # maybe datasource is 1st part
                table_parts = table_parts[1:]
                table_name = '.'.join(table_parts)
                if table_name in tables_map:
                    return tables_map[table_name]

    def get_type_of_var(self, v):
        if isinstance(v, str):
            return 'str'
        elif isinstance(v, float):
            return 'float'
        elif isinstance(v, int):
            return 'integer'

        return 'str'

    def execute_steps(self, params=None):
        # find all parameters
        stmt = self.statement

        # is already executed
        if stmt is None:
            if params is not None:
                raise PlanningException("Can't execute statement")
            stmt = Statement()

        # === form query with new target ===

        query = self.query

        if params is not None:

            if len(params) != len(stmt.params):
                raise PlanningException("Count of execution parameters don't match prepared statement")

            query = utils.fill_query_params(query, params)

            self.query = query

        # prevent from second execution
        stmt.params = None

        if (
                isinstance(query, ast.Select)
                or isinstance(query, ast.Union)
                or isinstance(query, ast.CreateTable)
                or isinstance(query, ast.Insert)
                or isinstance(query, ast.Update)
                or isinstance(query, ast.Delete)
        ):
            return self.plan_query(query)
        else:
            return []

    def plan_query(self, query):
        # use v1 planner
        self.from_query(query)
        step = None
        for step in self.plan.steps:
            # print(step)
            yield step

    def get_statement_info(self):
        stmt = self.statement

        if stmt is None:
            raise PlanningException('Statement is not prepared')

        columns_result = []

        for column in stmt.columns:
            table, ds = None, None
            if column.table is not None:
                table = column.table.name
                ds = column.table.ds
            columns_result.append(dict(
                alias=column.alias,
                type=column.type,
                name=column.name,
                table_name=table,
                table_alias=table,
                ds=ds,
            ))

        parameters = []
        for param in stmt.params:
            name = '?'
            parameters.append(dict(
                alias=name,
                type='str',
                name=name,
            ))

        return {
            'parameters': parameters,
            'columns': columns_result
        }
