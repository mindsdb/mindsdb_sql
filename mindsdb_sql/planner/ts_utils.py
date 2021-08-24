from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import Identifier, Operation, BinaryOperation


def find_time_filter(op, time_column_name):
    if not op:
        return
    if op.op == 'and':
        left = find_time_filter(op.args[0], time_column_name)
        right = find_time_filter(op.args[1], time_column_name)
        if left and right:
            raise PlanningException('Can provide only one filter by predictor order_by column, found two')

        return left or right
    elif ((isinstance(op.args[0], Identifier) and op.args[0].parts[-1] == time_column_name) or
          (isinstance(op.args[1], Identifier) and op.args[1].parts[-1] == time_column_name)):
        return op


def replace_time_filter(op, time_filter, new_filter):
    if op == time_filter:
        return new_filter
    elif op.args[0] == time_filter:
        op.args[0] = new_filter
    elif op.args[1] == time_filter:
        op.args[1] = new_filter


def find_and_remove_time_filter(op, time_filter):
    if isinstance(op, BinaryOperation):
        if op == time_filter:
            return None
        elif op.op == 'and':
            left_arg = op.args[0] if op.args[0] != time_filter else None
            right_arg = op.args[1] if op.args[1] != time_filter else None
            if not left_arg:
                return op.args[1]
            elif not right_arg:
                return op.args[0]
            return op
    return op


def validate_ts_where_condition(op, allowed_columns, allow_and=True):
    """Error if the where condition caontains invalid ops, is nested or filters on some column that's not time or partition"""
    if not op:
        return
    allowed_ops = ['and', '>', '>=', '=', '<', '<=', 'between', 'in']
    if not allow_and:
        allowed_ops.remove('and')
    if op.op not in allowed_ops:
        raise PlanningException(
            f'For time series predictors only the following operations are allowed in WHERE: {str(allowed_ops)}, found instead: {str(op)}.')

    for arg in op.args:
        if isinstance(arg, Identifier):
            if arg.parts[-1] not in allowed_columns:
                raise PlanningException(
                    f'For time series predictor only the following columns are allowed in WHERE: {str(allowed_columns)}, found instead: {str(arg)}.')

    if isinstance(op.args[0], Operation):
        validate_ts_where_condition(op.args[0], allowed_columns, allow_and=False)
    if isinstance(op.args[1], Operation):
        validate_ts_where_condition(op.args[1], allowed_columns, allow_and=False)