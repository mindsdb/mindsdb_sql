from sly import Parser

from sql_parser.ast import Constant, Identifier, Select, BinaryOperation, UnaryOperation
from sql_parser.ast.operation import Operation
from sql_parser.exceptions import ParsingException
from sql_parser.lexer import SQLLexer


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

    @_('SELECT expr_list FROM from_table WHERE expr GROUPBY expr_list HAVING expr')
    def select(self, p):
        targets = list(p.expr_list0)
        from_table = p.from_table
        where = p.expr0
        if not isinstance(where, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where)}")

        group_by = p.expr_list1
        if not isinstance(group_by, list):
            group_by = [group_by]

        if not all([isinstance(g, Identifier) for g in group_by]):
            raise ParsingException(
                f"GROUP BY must contain a list identifiers, got: {str(group_by)}")

        having = p.expr1
        if not isinstance(having, Operation):
            raise ParsingException(
                f"HAVING must contain an operation that evaluates to a boolean, got: {str(having)}")

        return Select(targets=targets,
                      from_table=from_table,
                      where=where,
                      group_by=group_by,
                      having=having)

    @_('SELECT expr_list FROM from_table WHERE expr GROUPBY expr_list')
    def select(self, p):
        targets = list(p.expr_list0)
        from_table = p.from_table
        where = p.expr
        if not isinstance(where, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where)}")

        group_by = p.expr_list1
        if not isinstance(group_by, list):
            group_by = [group_by]

        if not all([isinstance(g, Identifier) for g in group_by]):
            raise ParsingException(
                f"GROUP BY must contain a list identifiers, got: {str(group_by)}")

        return Select(targets=targets,
                      from_table=from_table,
                      where=where,
                      group_by=group_by)

    @_('SELECT expr_list FROM from_table WHERE expr')
    def select(self, p):
        targets = list(p.expr_list)
        from_table = p.from_table
        where_expr = p.expr

        if not isinstance(where_expr, Operation):
            raise ParsingException(f"WHERE must contain an operation that evaluates to a boolean, got: {str(where_expr)}")

        return Select(targets=targets,
                      from_table=from_table,
                      where=where_expr)

    @_('SELECT expr_list FROM from_table')
    def select(self, p):
        targets = list(p.expr_list)
        from_table = p.from_table
        return Select(targets=targets, from_table=from_table)

    @_('SELECT expr_list')
    def select(self, p):
        targets = list(p.expr_list)
        return Select(targets=targets)

    @_('identifier')
    def from_table(self, p):
        return p.identifier

    @_('enumeration')
    def expr_list(self, p):
        return p.enumeration

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    # OPERATIONS

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

    @_('expr AS identifier')
    def expr(self, p):
        p.expr.alias = p.identifier.value
        return p.expr

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
