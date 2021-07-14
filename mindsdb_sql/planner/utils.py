import copy

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import (Identifier, Operation, Star, Select, BinaryOperation, Constant, OrderBy)


def get_integration_path_from_identifier(identifier):
    parts = identifier.parts

    if len(parts) == 1:
        raise PlanningException(f'No integration specified for table: {str(identifier)}')
    elif len(parts) > 4:
        raise PlanningException(f'Too many parts (dots) in table identifier: {str(identifier)}')

    integration_name = parts[0]

    table_path = '.'.join(parts[1:])
    table_alias = identifier.alias
    return integration_name, table_path, table_alias


def get_predictor_namespace_and_name_from_identifier(identifier):
    parts = identifier.parts

    if len(parts) == 1:
        raise PlanningException(f'No predictor name specified for predictor: {str(identifier)}')
    elif len(parts) > 4:
        raise PlanningException(f'Too many parts (dots) in predictor identifier: {str(identifier)}')

    namespace = parts[0]

    predictor_path = '.'.join(parts[1:])
    predictor_alias = identifier.alias
    return namespace, predictor_path, predictor_alias


def disambiguate_integration_column_identifier(identifier, integration_name, table_path, table_alias,
                                               initial_path_as_alias=False):
    """Removes integration name from column if it's present, adds table path if it's absent"""
    column_table_ref = table_alias or table_path
    initial_path_str = identifier.parts_to_str()
    parts = list(identifier.parts)

    if len(parts) > 1:
        if parts[0] == integration_name:
            parts = parts[1:]

    if len(parts) > 1:
        if parts[0] != column_table_ref:
            raise PlanningException(
                f'Tried to query column {identifier.to_tree()} from integration {integration_name} table {column_table_ref}, but a different table name has been specified.')
    elif len(parts) == 1:
        if parts[0] != column_table_ref:
            parts.insert(0, column_table_ref)

    new_identifier = Identifier(parts=parts)
    if identifier.alias:
        new_identifier.alias = identifier.alias
    elif initial_path_as_alias:
        new_identifier.alias = initial_path_str

    return new_identifier


def disambiguate_predictor_column_identifier(identifier, predictor_name, predictor_alias):
    """Removes integration name from column if it's present, adds table path if it's absent"""
    table_ref = predictor_alias or predictor_name
    parts = list(identifier.parts)
    if parts[0] == table_ref:
        parts = parts[1:]

    new_identifier = Identifier(parts=parts)
    return new_identifier


def recursively_disambiguate_identifiers_in_op(op, integration_name, table_path, table_alias):
    for arg in op.args:
        if isinstance(arg, Identifier):
            new_identifier = disambiguate_integration_column_identifier(arg, integration_name, table_path,
                                                                             table_alias,
                                                                             initial_path_as_alias=False)
            arg.parts = new_identifier.parts
            arg.alias = new_identifier.alias
        elif isinstance(arg, Operation):
            recursively_disambiguate_identifiers_in_op(arg, integration_name, table_path, table_alias)
        elif isinstance(arg, Select):
            arg_select_integration_name, arg_select_table_path, arg_select_table_alias = get_integration_path_from_identifier(arg.from_table)

            recursively_disambiguate_identifiers_in_select(arg, arg_select_integration_name, arg_select_table_path, arg_select_table_alias)


def disambiguate_select_targets(targets, integration_name, table_path, table_alias):
    new_query_targets = []
    for target in targets:
        if isinstance(target, Identifier):
            new_query_targets.append(
                disambiguate_integration_column_identifier(target, integration_name, table_path, table_alias,
                                                                initial_path_as_alias=True))
        elif isinstance(target, Star):
            new_query_targets.append(target)
        elif isinstance(target, Operation) or isinstance(target, Select):
            new_op = copy.deepcopy(target)
            recursively_disambiguate_identifiers(new_op, integration_name, table_path, table_alias)
            new_query_targets.append(new_op)
        else:
            raise PlanningException(f'Unknown select target {type(target)}')
    return new_query_targets


def recursively_disambiguate_identifiers_in_select(select, integration_name, table_path, table_alias):
    select.targets = disambiguate_select_targets(select.targets, integration_name, table_path, table_alias)

    if select.from_table:
        if isinstance(select.from_table, Identifier):
            select.from_table = Identifier(table_path, alias=table_alias)
    if select.where:
        if not isinstance(select.where, BinaryOperation):
            raise PlanningException(
                f'Unsupported where clause {type(select.where)}, only BinaryOperation is supported now.')

        where = copy.deepcopy(select.where)
        recursively_disambiguate_identifiers_in_op(where, integration_name, table_path, table_alias)
        select.where = where

    if select.group_by:
        group_by = copy.deepcopy(select.group_by)
        group_by = [
            disambiguate_integration_column_identifier(id, integration_name, table_path, table_alias,
                                                       initial_path_as_alias=False)
            for id in group_by]
        select.group_by = group_by

    if select.having:
        if not isinstance(select.having, BinaryOperation):
            raise PlanningException(
                f'Unsupported having clause {type(select.having)}, only BinaryOperation is supported now.')

        having = copy.deepcopy(select.having)
        recursively_disambiguate_identifiers_in_op(having, integration_name, table_path, table_alias)
        select.having = having

    if select.order_by:
        order_by = []
        for order_by_item in select.order_by:
            new_order_item = copy.deepcopy(order_by_item)
            new_order_item.field = disambiguate_integration_column_identifier(new_order_item.field,
                                                                              integration_name, table_path,
                                                                              table_alias)
            order_by.append(new_order_item)
        select.order_by = order_by


def recursively_disambiguate_identifiers(obj, integration_name, table_path, table_alias):
    if isinstance(obj, Operation):
        recursively_disambiguate_identifiers_in_op(obj, integration_name, table_path, table_alias)
    elif isinstance(obj, Select):
        recursively_disambiguate_identifiers_in_select(obj, integration_name, table_path, table_alias)
    else:
        raise PlanningException(f'Unsupported object for disambiguation {type(obj)}')


def recursively_extract_column_values(op, row_dict, predictor_name, predictor_alias):
    if isinstance(op, BinaryOperation) and op.op == '=':
        id = disambiguate_predictor_column_identifier(op.args[0], predictor_name, predictor_alias)
        value = op.args[1]
        if not (isinstance(id, Identifier) and isinstance(value, Constant)):
            raise PlanningException(f'The WHERE clause for selecting from a predictor'
                                    f' must contain pairs \'Identifier(...) = Constant(...)\','
                                    f' found instead: {id.to_tree()}, {value.to_tree()}')

        if str(id) in row_dict:
            raise PlanningException(f'Multiple values provided for {str(id)}')
        row_dict[str(id)] = value.value
    elif isinstance(op, BinaryOperation) and op.op == 'and':
        recursively_extract_column_values(op.args[0], row_dict, predictor_name, predictor_alias)
        recursively_extract_column_values(op.args[1], row_dict, predictor_name, predictor_alias)
    else:
        raise PlanningException(f'Only \'and\' and \'=\' operations allowed in WHERE clause, found: {op.to_tree()}')


def recursively_check_join_identifiers_for_ambiguity(item):
    if item is None:
        return
    elif isinstance(item, Identifier):
        if len(item.parts) == 1:
            raise PlanningException(f'Ambigous identifier {str(item)}, provide table name for operations on a join.')
    elif isinstance(item, Operation):
        recursively_check_join_identifiers_for_ambiguity(item.args)
    elif isinstance(item, OrderBy):
        recursively_check_join_identifiers_for_ambiguity(item.field)
    elif isinstance(item, list):
        for arg in item:
            recursively_check_join_identifiers_for_ambiguity(arg)

def get_deepest_select(select):
    if not select.from_table or not isinstance(select.from_table, Select):
        return select
    return get_deepest_select(select.from_table)
