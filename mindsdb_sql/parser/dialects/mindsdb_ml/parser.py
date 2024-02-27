from sly import Parser
from mindsdb_sql.parser.dialects.mindsdb_ml.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.dialects.mindsdb_lite.lexer import MindsDBLexer
from mindsdb_sql.parser.logger import ParserLogger
from mindsdb_sql.parser.utils import ensure_select_keyword_order, JoinType, tokens_to_string


# noinspection SqlDialectInspection
class MindsDBParser(Parser):
    log = ParserLogger()
    tokens = MindsDBLexer.tokens

    sql_tokens = tokens.copy()
    for parsed in ['SELECT', 'CREATE', 'ID', 'NAT_ID', 'AMB_ID', 'MINDS_ID']:
        sql_tokens.remove(parsed)

    # Get a list of low priority tokens to set for left (reduce) priority

    right_precedence_tokens = ['AS', 'DOT', 'JOIN', 'WHERE']

    left_precedence_tokens = sql_tokens.copy()
    for parsed in right_precedence_tokens:
        left_precedence_tokens.remove(parsed)

    precedence = (
        ('left', *list(left_precedence_tokens)),
        ('right', *list(right_precedence_tokens)),
    )

    def register_integrations(self, mindsdb_obs, native_ints):
        self.query_domain = None
        self.mindsdb_objs = mindsdb_obs
        self.native_ints = native_ints

    def _check_domain(self, identifier: Identifier):
        if not hasattr(self, 'mindsdb_domain'):
            self.mindsdb_domain = set()
        if not hasattr(self, 'native_domain'):
            self.native_domain = set()

        if identifier.id_type == Identifier.ID_TYPE.MINDSDB:
            if identifier.left:
                self.mindsdb_domain.add(identifier.left.value)
            else:
                self.mindsdb_domain.add(identifier.value)

        if identifier.id_type == Identifier.ID_TYPE.NATIVE:
            if identifier.left:
                self.native_domain.add(identifier.left.value)
            else:
                self.native_domain.add(identifier.value)

        if len(self.mindsdb_domain) > 1 and len(self.native_domain) > 1:
            raise ParsingException(
                f"Query has conflicting domains.\n MindsDB domain: {self.mindsdb_domain}.\n Native domain: {self.native_domain}.\n Only one domain can have more than 1 member.")

    def _resolve_id_references(self, id: Identifier):
        for symbol in self.symstack:
            # type(self).log.error(f"resolving id reference for symbol string {str(symbol)} of type {type(symbol)}")
            if str(symbol) == "sql_statement":
                for sym in symbol.value:
                    if type(sym) == Identifier:
                        if sym.id_type == Identifier.ID_TYPE.AMBIGUOUS and sym.left:
                            type(self).log.error(f"symbol left: {sym.left.value}, id alias: {id.alias.value}")
                            if sym.left.value == id.alias.value:
                                type(self).log.error(f"resolving id reference")
                                sym.id_type = id.id_type
                                sym.left.id_type = id.id_type
                                sym.right.id_type = id.id_type

    ################################################ terminals #########################################################

    @_('raw_query',
       'native_query',
       'select',
       'view')
    def query(self, p):
        return Query(clauses=[p[0], ])

    ################################################ terminals #########################################################
    """@_('LPAREN sql_statement RPAREN')
    def sub_query(self, p):
        pass"""

    ################################################ Parse SQL #########################################################
    """
    @_(*list(sql_tokens))
    def sql_statement(self, p):
        type(self).log.debug(f"found sql_statement {p[0]}")
        return [p[0], ]

    @_('sql_statement sql_statement',
       'sql_statement id_list',
       'sql_statement id')
    def sql_statement(self, p):
        type(self).log.debug(f"combined sql_statements")
        if hasattr(p, 'id'):
            return p[0] + [p[1], ]
        else:
            return p[0] + p[1]
    """
    ################################################ Native Queries ####################################################

    native_query_tokens = tokens.copy()
    for parsed in ['LPAREN', 'RPAREN']:
        native_query_tokens.remove(parsed)

    @_(*list(native_query_tokens))
    def raw_query(self, p):
        return RawQuery(raw_query=p[0])

    @_('raw_query raw_query',
       'LPAREN raw_query RPAREN')
    def raw_query(self, p):
        if hasattr(p, 'LPAREN'):
            p[1].parentheses = True
            return p[1].parentheses
        else:
            return RawQuery(raw_query=str(p[0]) + str(p[1]))

    @_('id LPAREN raw_query RPAREN')
    def native_query(self, p):
        return NativeQuery(integration=p[0], raw_query=p[2])

    ################################################ IDs ###############################################################
    @_('id COMMA id',
       'id_list COMMA id')
    def id_list(self, p):
        if hasattr(p, 'id_list'):
            p[0].id_list.extend([p[2], ])
            return p[0]
        else:
            return IdentifierList(id_list=[p[0], p[2]])

    @_('ID')
    def id(self, p):
        return Identifier(column=p[0])

    @_('ID DOT ID')
    def id(self, p):
        return Identifier(column=p[2], table=p[0])

    @_('ID DOT ID AS ID')
    def id(self, p):
        return Identifier(column=p[2], table=p[0], alias=p[4])

    ################################################ SELECT ############################################################
    @_('')
    def empty(self, p):
        return None

    @_('select_clause from_clause where_clause')
    def select(self, p):
        return Select(select_clause=p[0], from_clause=p[1], where_clause=p[2])

    @_('SELECT STAR',
       'SELECT id',
       'SELECT id_list')
    def select_clause(self, p):
        return SelectClause(select_arg=p[1])

    ################################################ FROM ##############################################################
    @_('FROM id',
       'FROM id_list',
       'FROM native_query',
       'FROM join_clause')
    def from_clause(self, p):
        return FromClause(from_arg=p[1])

    ################################################ JOIN ##############################################################
    @_('id JOIN id',
       'id JOIN native_query',
       'join_clause JOIN id',
       'join_clause JOIN native_query')
    def join_clause(self, p):
        return JoinClause(left_arg=p[0], right_arg=p[2])

    ################################################ WHERE #############################################################

    @_('empty')
    def where_clause(self, p):
        return WhereClause()

    @_('WHERE condition',
       'WHERE condition_list',
       'WHERE condition_list LIMIT INTEGER', )
    def where_clause(self, p):
        if hasattr(p, 'LIMIT'):
            return WhereClause(where_conditions=p[1], limit=p[3])
        else:
            return WhereClause(where_conditions=p[1])

    @_('LPAREN condition_list RPAREN',
       'condition boolean condition',
       'condition_list boolean condition',
       'condition_list boolean condition_list')
    def condition_list(self, p):
        if hasattr(p, 'LPAREN') and hasattr(p, 'RPAREN'):
            p[1].parentheses = True
            return p[1]
        else:
            return ConditionList(left_condition=p[0], boolean=p[1], right_condition=p[2])

    @_('AND',
       'OR',
       'NOT')
    def boolean(self, p):
        return Boolean(boolean=p[0])

    @_('id comparator value',
       'LPAREN id comparator value RPAREN',
       'id comparator id',
       'LPAREN id comparator id RPAREN')
    def condition(self, p):
        if hasattr(p, 'LPAREN') and hasattr(p, 'RPAREN'):
            return Condition(left_arg=p[1], comparator=p[2], right_arg=p[3])
        else:
            return Condition(left_arg=p[0], comparator=p[1], right_arg=p[2])

    @_('EQUALS',
       'NEQUALS',
       'GEQ',
       'GREATER',
       'LEQ',
       'LESS')
    def comparator(self, p):
        return Comparator(comparator=p[0])

    @_('INTEGER',
       'FLOAT',
       'QUOTE_STRING',
       'DQUOTE_STRING',
       'TRUE',
       'FALSE',
       'LATEST')
    def value(self, p):
        return Value(value=p[0])

    ################################################ VIEWS #############################################################

    @_('CREATE VIEW id FROM native_query')
    def view(self, p):
        return View(view_name=p.id, native_query=p.native_query)

    ################################################ TRAIN #############################################################
