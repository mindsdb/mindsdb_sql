from sly import Parser

from sql_parser.ast import Constant, Identifier, Select, BinaryOperation, UnaryOperation
from sql_parser.ast.operation import Operation
from sql_parser.ast.order_by import OrderBy
from sql_parser.exceptions import ParsingException
from sql_parser.lexer import SQLLexer


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


class SQLParser(Parser):
    tokens = SQLLexer.tokens

    precedence = (
        ('nonassoc', LESS, LEQ, GREATER, GEQ, EQUALS, NEQUALS),
        ('left', PLUS, MINUS),
        ('left', STAR, DIVIDE),
        ('right', UMINUS, UNOT),  # Unary minus operator
    )

    def __init__(self):
        self.names = dict()

    # SELECT

    @_('select OFFSET constant')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'OFFSET')
        if not isinstance(p.constant.value, int):
            raise ParsingException(f'OFFSET must be an integer value, got: {p.constant.value}')

        select.offset = p.constant
        return select

    @_('select LIMIT constant')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'LIMIT')
        if not isinstance(p.constant.value, int):
            raise ParsingException(f'LIMIT must be an integer value, got: {p.constant.value}')
        select.limit = p.constant
        return select

    @_('select ORDERBY ordering_terms')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'ORDER BY')
        select.order_by = p.ordering_terms
        return select

    @_('ordering_terms COMMA ordering_term')
    def ordering_terms(self, p):
        terms = p.ordering_terms
        terms.append(p.ordering_term)
        return terms

    @_('ordering_term')
    def ordering_terms(self, p):
        return [p.ordering_term]

    @_('identifier')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='default')

    @_('identifier DESC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='DESC')

    @_('identifier ASC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='ASC')

    @_('select HAVING expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'HAVING')
        having = p.expr
        if not isinstance(having, Operation):
            raise ParsingException(
                f"HAVING must contain an operation that evaluates to a boolean, got: {str(having)}")
        select.having = having
        return select

    @_('select GROUPBY expr_list')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'GROUP BY')
        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        if not all([isinstance(g, Identifier) for g in group_by]):
            raise ParsingException(
                f"GROUP BY must contain a list identifiers, got: {str(group_by)}")

        select.group_by = group_by
        return select

    @_('select WHERE expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'WHERE')
        where_expr = p.expr
        if not isinstance(where_expr, Operation):
            raise ParsingException(f"WHERE must contain an operation that evaluates to a boolean, got: {str(where_expr)}")
        select.where = where_expr
        return select

    @_('select FROM identifier')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'FROM')
        select.from_table = p.identifier
        return select

    @_('SELECT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets)

    @_('result_columns COMMA result_column')
    def result_columns(self, p):
        p.result_columns.append(p.result_column)
        return p.result_columns

    @_('result_column')
    def result_columns(self, p):
        return [p.result_column]

    @_('result_column AS identifier')
    def result_column(self, p):
        col = p.result_column
        col.alias = p.identifier.value
        return col

    @_('expr')
    def result_column(self, p):
        return p.expr



    # OPERATIONS

    @_('enumeration')
    def expr_list(self, p):
        return p.enumeration

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    @_('expr PLUS expr',
        'expr MINUS expr',
        'expr STAR expr',
        'expr DIVIDE expr',
        'expr MODULO expr',
        'expr EQUALS expr',
        'expr NEQUALS expr',
        'expr GEQ expr',
        'expr GREATER expr',
        'expr LEQ expr',
        'expr LESS expr',
        'expr AND expr',
        'expr OR expr',
        'expr NOT expr',
        'expr ISNOT expr',
        'expr IS expr',
        'expr LIKE expr',
        'expr IN expr')
    def expr(self, p):
        return BinaryOperation(op=p[1], args=(p.expr0, p.expr1))

    @_('MINUS expr %prec UMINUS',
       'NOT expr %prec UNOT',)
    def expr(self, p):
        return UnaryOperation(op=p[0], args=(p.expr,))

    # EXPRESSIONS
    @_('enumeration COMMA expr')
    def enumeration(self, p):
        return p.enumeration + [p.expr]

    @_('expr COMMA expr')
    def enumeration(self, p):
        return [p.expr0, p.expr1]

    @_('identifier')
    def expr(self, p):
        return p.identifier

    @_('constant')
    def expr(self, p):
        return p.constant

    @_('INTEGER')
    def constant(self, p):
        return Constant(value=int(p.INTEGER))

    @_('FLOAT')
    def constant(self, p):
        return Constant(value=float(p.FLOAT))

    @_('STRING')
    def constant(self, p):
        return Constant(value=str(p.STRING))

    @_('ID')
    def identifier(self, p):
        return Identifier(value=p.ID)

    def error(self, p):
        if p:
            raise ParsingException(f"Syntax error at token {p.type}")
        else:
            raise ParsingException("Syntax error at EOF")
