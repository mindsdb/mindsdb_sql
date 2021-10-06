from mindsdb_sql.parser.parser import SQLParser
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.dialects.mysql.lexer import MySQLLexer
from mindsdb_sql.parser.dialects.mysql.variable import Variable
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.utils import ensure_select_keyword_order, JoinType


class MySQLParser(SQLParser):
    tokens = MySQLLexer.tokens

    precedence = (
        ('left', PLUS, MINUS, OR),
        ('left', STAR, DIVIDE, AND),
        ('right', UMINUS, UNOT),  # Unary minus operator, unary not
        ('nonassoc', LESS, LEQ, GREATER, GEQ, EQUALS, NEQUALS, IN, BETWEEN, IS, LIKE),
    )

    # Top-level statements
    @_('show',
       'start_transaction',
       'commit_transaction',
       'rollback_transaction',
       'alter_table',
       'explain',
       'set',
       'use',
       'union',
       'select',
       )
    def query(self, p):
        return p[0]

    # Explain
    @_('EXPLAIN identifier')
    def explain(self, p):
        return Explain(target=p.identifier)

    # Alter table
    @_('ALTER TABLE identifier ID ID')
    def alter_table(self, p):
        return AlterTable(target=p.identifier,
                          arg=' '.join([p.ID0, p.ID1]))

    # Transactions

    @_('START TRANSACTION')
    def start_transaction(self, p):
        return StartTransaction()

    @_('COMMIT')
    def commit_transaction(self, p):
        return CommitTransaction()

    @_('ROLLBACK')
    def rollback_transaction(self, p):
        return RollbackTransaction()

    # Set

    @_('SET AUTOCOMMIT')
    def set(self, p):
        return Set(category=p.AUTOCOMMIT)

    @_('SET ID identifier')
    def set(self, p):
        if not p.ID == 'names':
            raise ParsingException(f'Excepted "names"')
        return Set(category=p.ID, arg=p.identifier)

    # Show

    @_('SHOW show_category show_condition_or_nothing')
    def show(self, p):
        condition = p.show_condition_or_nothing['condition'] if p.show_condition_or_nothing else None
        expression = p.show_condition_or_nothing['expression'] if p.show_condition_or_nothing else None
        return Show(category=p.show_category,
                    condition=condition,
                    expression=expression)

    @_('show_condition_token expr',
       'empty')
    def show_condition_or_nothing(self, p):
        if not p[0]:
            return None
        return dict(condition=p[0], expression=p[1])

    @_('WHERE',
       'FROM',
       'LIKE')
    def show_condition_token(self, p):
        return p[0]

    @_('SCHEMAS',
       'DATABASES',
       'TABLES',
       'FULL TABLES',
       'VARIABLES',
       'SESSION VARIABLES',
       'SESSION STATUS',
       'GLOBAL VARIABLES',
       'PROCEDURE STATUS',
       'FUNCTION STATUS',
       'INDEX',
       'CREATE TABLE',
       'WARNINGS',
       'ENGINES',
       'CHARSET',
       'COLLATION',
       'TABLE STATUS')
    def show_category(self, p):
        return ' '.join([x for x in p])

    # USE

    @_('USE identifier')
    def use(self, p):
        return Use(value=p.identifier)

    # UNION / UNION ALL
    @_('select UNION select')
    def union(self, p):
        return Union(left=p.select0, right=p.select1, unique=True)

    @_('select UNION ALL select')
    def union(self, p):
        return Union(left=p.select0, right=p.select1, unique=False)

    # WITH
    @_('ctes select')
    def select(self, p):
        select = p.select
        select.cte = p.ctes
        return select

    @_('ctes COMMA identifier cte_columns_or_nothing AS LPAREN select RPAREN')
    def ctes(self, p):
        ctes = p.ctes
        ctes = ctes + [
            CommonTableExpression(
                name=p.identifier,
                columns=p.cte_columns_or_nothing,
                query=p.select)
        ]
        return ctes

    @_('WITH identifier cte_columns_or_nothing AS LPAREN select RPAREN')
    def ctes(self, p):
        return [
            CommonTableExpression(
                name=p.identifier,
                columns=p.cte_columns_or_nothing,
                query=p.select)
        ]

    @_('empty')
    def cte_columns_or_nothing(self, p):
        pass

    @_('LPAREN enumeration RPAREN')
    def cte_columns_or_nothing(self, p):
        return p.enumeration

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

    @_('select ORDER_BY ordering_terms')
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

    @_('ordering_term NULLS_FIRST')
    def ordering_term(self, p):
        p.ordering_term.nulls = p.NULLS_FIRST
        return p.ordering_term

    @_('ordering_term NULLS_LAST')
    def ordering_term(self, p):
        p.ordering_term.nulls = p.NULLS_LAST
        return p.ordering_term

    @_('identifier DESC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='DESC')

    @_('identifier ASC')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='ASC')

    @_('identifier')
    def ordering_term(self, p):
        return OrderBy(field=p.identifier, direction='default')

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

    @_('select GROUP_BY expr_list')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'GROUP BY')
        group_by = p.expr_list
        if not isinstance(group_by, list):
            group_by = [group_by]

        select.group_by = group_by
        return select

    @_('select WHERE expr')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'WHERE')
        where_expr = p.expr
        if not isinstance(where_expr, Operation):
            raise ParsingException(
                f"WHERE must contain an operation that evaluates to a boolean, got: {str(where_expr)}")
        select.where = where_expr
        return select

    # Special cases for keyword-like identifiers
    @_('select FROM TABLES')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'FROM')
        select.from_table = Identifier(p.TABLES)
        return select

    @_('select FROM from_table')
    def select(self, p):
        select = p.select
        ensure_select_keyword_order(select, 'FROM')
        select.from_table = p.from_table
        return select

    @_('from_table join_clause from_table')
    def from_table(self, p):
        return Join(left=p.from_table0,
                    right=p.from_table1,
                    join_type=p.join_clause)

    @_('from_table COMMA from_table')
    def from_table(self, p):
        return Join(left=p.from_table0,
                    right=p.from_table1,
                    join_type=JoinType.INNER_JOIN,
                    implicit=True)

    @_('from_table join_clause from_table ON expr')
    def from_table(self, p):
        return Join(left=p.from_table0,
                    right=p.from_table1,
                    join_type=p.join_clause,
                    condition=p.expr)

    @_('from_table AS identifier')
    def from_table(self, p):
        entity = p.from_table
        entity.alias = p.identifier
        return entity

    @_('LPAREN query RPAREN')
    def from_table(self, p):
        query = p.query
        query.parentheses = True
        return query

    @_('identifier')
    def from_table(self, p):
        return p.identifier

    @_('parameter')
    def from_table(self, p):
        return p.parameter

    @_('JOIN',
       'LEFT JOIN',
       'RIGHT JOIN',
       'INNER JOIN',
       'FULL JOIN',
       'CROSS JOIN',
       'OUTER JOIN',
       )
    def join_clause(self, p):
        return ' '.join([x for x in p])

    @_('SELECT DISTINCT result_columns')
    def select(self, p):
        targets = p.result_columns
        return Select(targets=targets, distinct=True)

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
        if col.alias:
            raise ParsingException(f'Attempt to provide two aliases for {str(col)}')
        col.alias = p.identifier
        return col

    @_('LPAREN select RPAREN')
    def result_column(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('star')
    def result_column(self, p):
        return p.star

    @_('expr')
    def result_column(self, p):
        return p.expr

    # OPERATIONS

    @_('LPAREN select RPAREN')
    def expr(self, p):
        select = p.select
        select.parentheses = True
        return select

    @_('LPAREN expr RPAREN')
    def expr(self, p):
        if isinstance(p.expr, ASTNode):
            p.expr.parentheses = True
        return p.expr

    @_('ID LPAREN DISTINCT expr_list RPAREN')
    def expr(self, p):
        return Function(op=p.ID, distinct=True, args=p.expr_list)

    @_('ID LPAREN expr_list_or_nothing RPAREN')
    def expr(self, p):
        args = p.expr_list_or_nothing
        if not args:
            args = []
        return Function(op=p.ID, args=args)

    # arguments are optional in functions, so that things like `select database()` are possible
    @_('expr BETWEEN expr AND expr')
    def expr(self, p):
        return BetweenOperation(args=(p.expr0, p.expr1, p.expr2))

    @_('expr_list')
    def expr_list_or_nothing(self, p):
        return p.expr_list

    @_('empty')
    def expr_list_or_nothing(self, p):
        pass

    @_('CAST LPAREN expr AS ID RPAREN')
    def expr(self, p):
        return TypeCast(arg=p.expr, type_name=str(p.ID))

    @_('enumeration')
    def expr_list(self, p):
        return p.enumeration

    @_('expr')
    def expr_list(self, p):
        return [p.expr]

    @_('LPAREN enumeration RPAREN')
    def expr(self, p):
        tup = Tuple(items=p.enumeration)
        return tup

    @_('STAR')
    def star(self, p):
        return Star()

    @_('expr IS NOT expr',
       'expr NOT IN expr')
    def expr(self, p):
        op = p[1] + ' ' + p[2]
        return BinaryOperation(op=op, args=(p.expr0, p.expr1))

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
       'expr IS expr',
       'expr LIKE expr',
       'expr CONCAT expr',
       'expr IN expr')
    def expr(self, p):
        return BinaryOperation(op=p[1], args=(p.expr0, p.expr1))


    @_('MINUS expr %prec UMINUS',
       'NOT expr %prec UNOT', )
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

    @_('parameter')
    def expr(self, p):
        return p.parameter

    @_('constant')
    def expr(self, p):
        return p.constant

    @_('NULL')
    def constant(self, p):
        return NullConstant()

    @_('TRUE')
    def constant(self, p):
        return Constant(value=True)

    @_('FALSE')
    def constant(self, p):
        return Constant(value=False)

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
        value = p.ID
        return Identifier.from_path_str(value)

    @_('PARAMETER')
    def parameter(self, p):
        return Parameter(value=p.PARAMETER)

    @_('')
    def empty(self, p):
        pass

    # MySQL specific

    @_('variable')
    def table_or_subquery(self, p):
        return p.variable

    @_('variable')
    def expr(self, p):
        return p.variable

    @_('SYSTEM_VARIABLE')
    def variable(self, p):
        return Variable(value=p.SYSTEM_VARIABLE, is_system_var=True)

    @_('VARIABLE')
    def variable(self, p):
        return Variable(value=p.VARIABLE)
