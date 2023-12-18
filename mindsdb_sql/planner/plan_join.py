import copy
from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import (Select, Identifier, Join, Star, BinaryOperation, Constant, NativeQuery, Parameter)

from mindsdb_sql.planner.steps import (FetchDataframeStep, JoinStep, ApplyPredictorStep, SubSelectStep)
from mindsdb_sql.planner.utils import (query_traversal, filters_to_bin_op)


class PlanJoin:

    def __init__(self, planner):
        self.planner = planner

        self.tables_idx = None

    def plan(self, query):
        self.tables_idx = {}
        return self.plan_join_tables(query)

    def resolve_table(self, table):
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
        integration = self.planner.default_namespace
        if len(table.parts) > 0:
            if table.parts[0] in self.planner.databases:
                integration = table.parts.pop(0)
            else:
                integration = self.planner.default_namespace

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

    def get_join_sequence(self, node):
        sequence = []
        if isinstance(node, Identifier):
            # resolve identifier

            table_info = self.resolve_table(node)
            for alias in table_info['aliases']:
                self.tables_idx[alias] = table_info

            table_info['predictor_info'] = self.planner.get_predictor(node)

            sequence.append(table_info)

        elif isinstance(node, Join):
            # create sequence: 1)table1, 2)table2, 3)join 1 2, 4)table 3, 5)join 3 4

            # put all tables before
            sequence2 = self.get_join_sequence(node.left)
            for item in sequence2:
                sequence.append(item)

            sequence2 = self.get_join_sequence(node.right)
            if len(sequence2) != 1:
                raise PlanningException('Unexpected join nesting behavior')

            # put next table
            sequence.append(sequence2[0])

            # put join
            sequence.append(node)

        else:
            raise NotImplementedError()
        return sequence

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


        # get all join tables, form join sequence

        join_sequence = self.get_join_sequence(query.from_table)

        def _check_identifiers(node, is_table, **kwargs):
            if not is_table and isinstance(node, Identifier):
                if len(node.parts) > 1:
                    parts = tuple(map(str.lower, node.parts[:-1]))
                    if parts not in self.tables_idx:
                        raise PlanningException(f'Table not found for identifier: {node.to_string()}')

                    # # replace identifies name
                    col_parts = list(self.tables_idx[parts]['aliases'][-1])
                    col_parts.append(node.parts[-1])
                    node.parts = col_parts

        query_traversal(query, _check_identifiers)


        find_selects = self.planner.get_nested_selects_plan_fnc(self.planner.default_namespace, force=True)
        query_traversal(query.where, find_selects)

        # get conditions for tables
        binary_ops = []

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
                    if parts not in self.tables_idx:
                        raise PlanningException(f'Table not found for identifier: {arg1.to_string()}')

                    # keep only column name
                    arg1.parts = [arg1.parts[-1]]

                    node2._orig_node = node
                    self.tables_idx[parts]['conditions'].append(node2)

        query_traversal(query.where, _check_condition)

        # workaround for 'model join table': swap tables:
        if len(join_sequence) == 3 and join_sequence[0]['predictor_info'] is not None:
            join_sequence = [join_sequence[1], join_sequence[0], join_sequence[2]]

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
                    step = self.planner.plan_select(item['sub_select'])

                    where = filters_to_bin_op(item['conditions'])

                    # apply table alias
                    query2 = Select(targets=[Star()], where=where)
                    if item['table'].alias is None:
                        raise PlanningException(f'Subselect in join have to be aliased: {item["sub_select"].to_string()}')
                    table_name = item['table'].alias.parts[-1]

                    add_absent_cols = False
                    if hasattr (item['sub_select'], 'from_table') and\
                         isinstance(item['sub_select'].from_table, ast.Data):
                        add_absent_cols = True

                    step2 = SubSelectStep(query2, step.result, table_name=table_name, add_absent_cols=add_absent_cols)
                    step2 = self.planner.plan.add_step(step2)
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

                    predictor_step = self.planner.plan.add_step(ApplyPredictorStep(
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
                    self.planner.plan.add_step(step)
                    step_stack.append(step)
            elif isinstance(item, Join):
                step_right = step_stack.pop()
                step_left = step_stack.pop()

                new_join = copy.deepcopy(item)

                # TODO
                new_join.left = Identifier('tab1')
                new_join.right = Identifier('tab2')
                new_join.implicit = False

                step = self.planner.plan.add_step(JoinStep(left=step_left.result, right=step_right.result, query=new_join))

                step_stack.append(step)

        query_in.where = query.where
        return step_stack.pop()
