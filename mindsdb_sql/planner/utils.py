import copy

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import (Identifier, Operation, Star, Select, BinaryOperation, Constant,
                                    OrderBy, BetweenOperation, NullConstant, TypeCast)
from mindsdb_sql.parser import ast

def get_integration_path_from_identifier(identifier):
    parts = identifier.parts
    integration_name = parts[0]
    new_parts = parts[1:]

    if len(parts) == 1:
        raise PlanningException(f'No integration specified for table: {str(identifier)}')
    elif len(parts) > 4:
        raise PlanningException(f'Too many parts (dots) in table identifier: {str(identifier)}')

    new_identifier = copy.deepcopy(identifier)
    new_identifier.parts = new_parts

    return integration_name, new_identifier


def get_predictor_namespace_and_name_from_identifier(identifier, default_namespace):
    parts = identifier.parts
    namespace = parts[0]
    new_parts = parts[1:]
    if len(parts) == 1:
        if default_namespace:
            namespace = default_namespace
            new_parts = [parts[0]]
        else:
            raise PlanningException(f'No predictor name specified for predictor: {str(identifier)}')
    elif len(parts) > 4:
        raise PlanningException(f'Too many parts (dots) in predictor identifier: {str(identifier)}')

    new_identifier = copy.deepcopy(identifier)
    new_identifier.parts = new_parts
    return namespace, new_identifier


def disambiguate_integration_column_identifier(identifier, integration_name, table,
                                               initial_name_as_alias=False):
    """Removes integration name from column if it's present, adds table path if it's absent"""
    column_table_ref = [table.alias.to_string(alias=False)] if table.alias else table.parts
    parts = list(identifier.parts)

    if len(parts) > 1:
        if parts[0] == integration_name:
            parts = parts[1:]

    if len(parts) > 1:
        if (len(parts) <= len(column_table_ref)
            or
            parts[:len(column_table_ref)] != column_table_ref
        ):
            raise PlanningException(
                f'Tried to query column {identifier.to_tree()} from integration {integration_name} table {column_table_ref}, but a different table name has been specified.')
    elif len(parts) == 1:
        # if parts[0] != column_table_ref:
        parts = column_table_ref + parts

    new_identifier = Identifier(parts=parts)
    if identifier.alias:
        new_identifier.alias = identifier.alias
    elif initial_name_as_alias:
        new_identifier.alias = Identifier(parts[-1])

    return new_identifier


def disambiguate_predictor_column_identifier(identifier, predictor):
    """Removes integration name from column if it's present, adds table path if it's absent"""
    table_ref = predictor.alias.parts_to_str() if predictor.alias else predictor.parts_to_str()
    parts = list(identifier.parts)
    if parts[0] == table_ref:
        parts = parts[1:]

    new_identifier = Identifier(parts=parts)
    return new_identifier


def recursively_disambiguate_identifiers_in_op(op, integration_name, table):
    for arg in op.args:
        if isinstance(arg, Identifier):
            new_identifier = disambiguate_integration_column_identifier(arg, integration_name, table)
            arg.parts = new_identifier.parts
            arg.alias = new_identifier.alias
        elif isinstance(arg, Operation):
            recursively_disambiguate_identifiers_in_op(arg, integration_name, table)
        elif isinstance(arg, Select):
            arg_select_integration_name, arg_table = get_integration_path_from_identifier(arg.from_table)

            recursively_disambiguate_identifiers_in_select(arg, arg_select_integration_name, arg_table)


def disambiguate_select_targets(targets, integration_name, table):
    new_query_targets = []
    for target in targets:
        if isinstance(target, Identifier):
            new_query_targets.append(
                disambiguate_integration_column_identifier(target,
                                                           integration_name,
                                                           table,
                                                           initial_name_as_alias=True)
            )
        elif type(target) in (Star, Constant, NullConstant):
            new_query_targets.append(target)
        elif isinstance(target, Operation) or isinstance(target, Select):
            new_op = copy.deepcopy(target)
            recursively_disambiguate_identifiers(new_op, integration_name, table)
            new_query_targets.append(new_op)
        elif isinstance(target, TypeCast):
            new_op = copy.deepcopy(target)
            if isinstance(target.arg, Identifier):
                disambiguate_integration_column_identifier(new_op.arg, integration_name, table)
            new_query_targets.append(new_op)
        else:
            raise PlanningException(f'Unknown select target {type(target)}')
    return new_query_targets


def recursively_disambiguate_identifiers_in_select(select, integration_name, table):
    select.targets = disambiguate_select_targets(select.targets, integration_name, table)

    if select.from_table:
        if isinstance(select.from_table, Identifier):
            select.from_table = table
    if select.where:
        if not isinstance(select.where, BinaryOperation) and not isinstance(select.where, BetweenOperation):
            raise PlanningException(
                f'Unsupported where clause {type(select.where)}, only BinaryOperation is supported now.')

        where = copy.deepcopy(select.where)
        recursively_disambiguate_identifiers_in_op(where, integration_name, table)
        select.where = where

    if select.group_by:
        group_by = copy.deepcopy(select.group_by)
        group_by2 = []
        for field in group_by:
            if isinstance(field, Identifier):
                field = disambiguate_integration_column_identifier(field, integration_name, table)
            group_by2.append(field)
        select.group_by = group_by2

    if select.having:
        if not isinstance(select.having, BinaryOperation):
            raise PlanningException(
                f'Unsupported having clause {type(select.having)}, only BinaryOperation is supported now.')

        having = copy.deepcopy(select.having)
        recursively_disambiguate_identifiers_in_op(having, integration_name, table)
        select.having = having

    if select.order_by:
        order_by = []
        for order_by_item in select.order_by:
            new_order_item = copy.deepcopy(order_by_item)
            new_order_item.field = disambiguate_integration_column_identifier(new_order_item.field,
                                                                              integration_name, table)
            order_by.append(new_order_item)
        select.order_by = order_by


def recursively_disambiguate_identifiers(obj, integration_name, table):
    if isinstance(obj, Operation):
        recursively_disambiguate_identifiers_in_op(obj, integration_name, table)
    elif isinstance(obj, Select):
        recursively_disambiguate_identifiers_in_select(obj, integration_name, table)
    else:
        raise PlanningException(f'Unsupported object for disambiguation {type(obj)}')


def recursively_extract_column_values(op, row_dict, predictor):
    if isinstance(op, BinaryOperation) and op.op == '=':
        id = op.args[0]
        value = op.args[1]

        if not (isinstance(id, Identifier) and isinstance(value, Constant)):
            raise PlanningException(f'The WHERE clause for selecting from a predictor'
                                    f' must contain pairs \'Identifier(...) = Constant(...)\','
                                    f' found instead: {id.to_tree()}, {value.to_tree()}')

        id = disambiguate_predictor_column_identifier(id, predictor)

        if str(id) in row_dict:
            raise PlanningException(f'Multiple values provided for {str(id)}')
        row_dict[str(id)] = value.value
    elif isinstance(op, BinaryOperation) and op.op == 'and':
        recursively_extract_column_values(op.args[0], row_dict, predictor)
        recursively_extract_column_values(op.args[1], row_dict, predictor)
    else:
        raise PlanningException(f'Only \'and\' and \'=\' operations allowed in WHERE clause, found: {op.to_tree()}')


def recursively_check_join_identifiers_for_ambiguity(item, aliased_fields=None):
    if item is None:
        return
    elif isinstance(item, Identifier):
        if len(item.parts) == 1:
            if aliased_fields is not None and item.parts[0] in aliased_fields:
                # is alias
                return
            raise PlanningException(f'Ambigous identifier {str(item)}, provide table name for operations on a join.')
    elif isinstance(item, Operation):
        recursively_check_join_identifiers_for_ambiguity(item.args, aliased_fields=aliased_fields)
    elif isinstance(item, OrderBy):
        recursively_check_join_identifiers_for_ambiguity(item.field, aliased_fields=aliased_fields)
    elif isinstance(item, list):
        for arg in item:
            recursively_check_join_identifiers_for_ambiguity(arg, aliased_fields=aliased_fields)


def get_deepest_select(select):
    if not select.from_table or not isinstance(select.from_table, Select):
        return select
    return get_deepest_select(select.from_table)


def query_traversal(node, callback, is_table=False):
    # traversal query tree to find and replace nodes

    res = callback(node, is_table=is_table)
    if res is not None:
        # node is going to be replaced
        return res

    if isinstance(node, ast.Select):
        array = []
        for node2 in node.targets:
            node_out = query_traversal(node2, callback) or node2
            array.append(node_out)
        node.targets = array

        if node.cte is not None:
            array = []
            for cte in node.cte:
                node_out = query_traversal(cte.query, callback) or cte
                array.append(node_out)
            node.cte = array

        if node.from_table is not None:
            node_out = query_traversal(node.from_table, callback, is_table=True)
            if node_out is not None:
                node.from_table = node_out

        if node.where is not None:
            node_out = query_traversal(node.where, callback)
            if node_out is not None:
                node.where = node_out

        if node.group_by is not None:
            array = []
            for node2 in node.group_by:
                node_out = query_traversal(node2, callback) or node2
                array.append(node_out)
            node.group_by = array

        if node.having is not None:
            node_out = query_traversal(node.having, callback)
            if node_out is not None:
                node.having = node_out

        if node.order_by is not None:
            array = []
            for node2 in node.order_by:
                node_out = query_traversal(node2, callback) or node2
                array.append(node_out)
            node.order_by = array

    elif isinstance(node, ast.Union):
        node_out = query_traversal(node.left, callback)
        if node_out is not None:
            node.left = node_out
        node_out= query_traversal(node.right, callback)
        if node_out is not None:
            node.right = node_out
    # elif isinstance(node, ast.Update):
    #     TODO
    # elif isinstance(node, ast.Insert):
    #     TODO
    # elif isinstance(node, ast.Delete):
    #     TODO
    elif isinstance(node, ast.Join):
        node_out = query_traversal(node.right, callback, is_table=True)
        if node_out is not None:
            node.right = node_out
        node_out = query_traversal(node.left, callback, is_table=True)
        if node_out is not None:
            node.left = node_out
        if node.condition is not None:
            node_out = query_traversal(node.condition, callback)
            if node_out is not None:
                node.condition = node_out
    elif isinstance(node, ast.Function) \
            or isinstance(node, ast.BinaryOperation)\
            or isinstance(node, ast.UnaryOperation) \
            or isinstance(node, ast.BetweenOperation):
        array = []
        for arg in node.args:
            node_out = query_traversal(arg, callback) or arg
            array.append(node_out)
        node.args = array
    elif isinstance(node, ast.WindowFunction):
        query_traversal(node.function, callback)
        if node.partition is not None:
            array = []
            for node2 in node.partition:
                node_out = query_traversal(node2, callback) or node2
                array.append(node_out)
            node.partition = array
        if node.order_by is not None:
            array = []
            for node2 in node.order_by:
                node_out = query_traversal(node2, callback) or node2
                array.append(node_out)
            node.partition = array
    elif isinstance(node, ast.TypeCast):
        node_out = query_traversal(node.arg, callback)
        if node_out is not None:
            node.arg = node_out
    elif isinstance(node, ast.Tuple):
        array = []
        for node2 in node.items:
            node_out = query_traversal(node2, callback) or node2
            array.append(node_out)
        node.items = array
    elif isinstance(node, ast.Insert):
        if not node.values is None:
            rows = []
            for row in node.values:
                items = []
                for item in row:
                    item2 = query_traversal(item, callback) or item
                    items.append(item2)
                rows.append(items)
            node.values = rows

        if not node.from_select is None:
            node_out = query_traversal(node.from_select, callback)
            if node_out is not None:
                node.from_select = node_out
    elif isinstance(node, ast.CreateTable):
        array = []
        if node.columns is not None:
            for node2 in node.columns:
                node_out = query_traversal(node2, callback) or node2
                array.append(node_out)
            node.columns = array

        if node.name is not None:
            node_out = query_traversal(node.name, callback, is_table=True)
            if node_out is not None:
                node.name = node_out

        if node.from_select is not None:
            node_out = query_traversal(node.from_select, callback)
            if node_out is not None:
                node.from_select = node_out
    elif isinstance(node, ast.Delete):
        if node.where is not None:
            node_out = query_traversal(node.where, callback)
            if node_out is not None:
                node.where = node_out
    elif isinstance(node, ast.OrderBy):
        if node.field is not None:
            node_out = query_traversal(node.field, callback)
            if node_out is not None:
                node.field = node_out
    # TODO update statement


def convert_join_to_list(join):
    # join tree to table list

    if isinstance(join.right, ast.Join):
        raise NotImplementedError('Wrong join AST')

    items = []

    if isinstance(join.left, ast.Join):
        # dive to next level
        items.extend(convert_join_to_list(join.left))
    else:
        # this is first table
        items.append(dict(
            table=join.left
        ))

    # all properties set to right table
    items.append(dict(
        table=join.right,
        join_type=join.join_type,
        is_implicit=join.implicit,
        condition=join.condition
    ))

    return items


def get_query_params(query):
    # find all parameters
    params = []

    def params_find(node, **kwargs):
        if isinstance(node, ast.Parameter):
            params.append(node)
            return node

    query_traversal(query, params_find)
    return params

def fill_query_params(query, params):

    params = copy.deepcopy(params)

    def params_replace(node, **kwargs):
        if isinstance(node, ast.Parameter):
            value = params.pop(0)
            return ast.Constant(value)

    # put parameters into query
    query_traversal(query, params_replace)

    return query

