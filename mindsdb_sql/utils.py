from mindsdb_sql.exceptions import ParsingException


def indent(level):
    return '  ' * level


def ensure_select_keyword_order(select, operation):
    op_to_attr = {
        'FROM': select.from_table,
        'WHERE': select.where,
        'GROUP BY': select.group_by,
        'HAVING': select.having,
        'ORDER BY': select.order_by,
        'LIMIT': select.limit,
        'OFFSET': select.offset,
    }

    requirements = {
        'WHERE': ['FROM'],
        'GROUP BY': ['FROM'],
        'ORDER BY': ['FROM'],
        'HAVING': ['GROUP BY'],
    }

    precedence = ['FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET']

    if op_to_attr[operation]:
        raise ParsingException(f"Duplicate {operation} clause. Only one {operation} allowed per SELECT.")

    op_requires = requirements.get(operation, [])

    for req in op_requires:
        if not op_to_attr[req]:
            raise ParsingException(f"{operation} requires {req}")

    op_precedence_pos = precedence.index(operation)

    for next_op in precedence[op_precedence_pos:]:
        if op_to_attr[next_op]:
            raise ParsingException(f"{operation} must go after {next_op}")


class JoinType:
    JOIN = 'JOIN'
    INNER_JOIN = 'INNER JOIN'
    OUTER_JOIN = 'OUTER JOIN'
    CROSS_JOIN = 'CROSS JOIN'
    LEFT_JOIN = 'LEFT JOIN'
    RIGHT_JOIN = 'RIGHT JOIN'
    FULL_JOIN = 'FULL JOIN'

