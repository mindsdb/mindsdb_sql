from sly import Parser
from mindsdb_sql.parser.dialects.mindsdb_lite.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.dialects.mindsdb_lite.lexer import MindsDBLexer
from mindsdb_sql.parser.logger import ParserLogger
from mindsdb_sql.parser.utils import ensure_select_keyword_order, JoinType, tokens_to_string


# noinspection SqlDialectInspection
class MindsDBParser(Parser):
    log = ParserLogger()
    tokens = MindsDBLexer.tokens

    sql_tokens = tokens.copy()
    for parsed in ['NAT_ID', 'AMB_ID', 'MINDS_ID']:
        sql_tokens.remove(parsed)

    # Get a list of low priority tokens to set for left (reduce) priority

    right_precedence_tokens = ['AS', 'DOT', 'COMMA', 'JOIN']

    left_precedence_tokens = sql_tokens.copy()
    for parsed in ['AS', 'DOT', 'COMMA']:
        right_precedence_tokens.remove(parsed)

    precedence = (
        ('left', *list(left_precedence_tokens)),
        ('right', DOT, AS, COMMA),
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

    ################################################ Parse SQL #########################################################

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

    ################################################ Combine IDs #######################################################
    @_('id COMMA id')
    def id_list(self, p):
        if hasattr(p, 'id_list'):
            return p[0] + [p[2], ]
        else:
            return [p[0], p[2]]

    ################################################ IDs #######################################################
    @_('ID')
    def id(self, p):
        return p.ID

    @_('id')
    def id(self, p):
        id_dict = {'type': 'id',
                   'column': p[1]}

        return id_dict

    @_('id DOT id')
    def id(self, p):
        id_dict = {'type': 'id',
                   'database': p[0],
                   'column': p[1]}

        return id_dict

    @_('id DOT id AS id')
    def id(self, p):
        id_dict = {'type': 'id',
                   'database': p[0],
                   'column': p[1],
                   'alias': p[2]}

        return id_dict

    ################################################ SELECT ############################################################
    @_('')
    def empty(self, p):
        pass

    @_('select_clause from_clause')
    def select(self, p):
        pass

    @_('SELECT STAR',
       'SELECT id',
       'SELECT id_list')
    def select_clause(self, p):
        pass

    ################################################ FROM ##############################################################
    @_('FROM id',
       'FROM id_list',
       'FROM native_query'
       'FROM join where_clause')
    def from_clause(self, p):
        pass

    @_('join_list join_condition',
       'join_list join_condition AS id')
    def join(self, p):
        pass

    @_('id JOIN id',
       'id JOIN native_query',
       'join_list JOIN id',
       'join_list JOIN native_query')
    def join_list(self, p):
        pass

    ################################################ WHERE #############################################################
    @_('WHERE condition',
       'WHERE condition_list',
       'empty')
    def where_clause(self, p):
        pass

    @_('condition boolean condition',
       'condition_list boolean condition',
       'condition_list boolean LPAREN condition RPAREN',
       'condition_list boolean LPAREN condition_list RPAREN')
    def condition_list(self, p):
        pass

    @_('AND',
       'OR',
       'NOT')
    def boolean(self, p):
        pass

    @_('id comparator value',
       'id comparator id')
    def condition(self, p):
        pass

    @_('EQUALS',
       'NEQUALS',
       'GEQ',
       'GREATER',
       'LEQ',
       'LESS')
    def comparator(self, p):
        pass

    @_('INTEGER',
       'FLOAT',
       'QUOTE_STRING',
       'DQUOTE_STRING',
       'TRUE',
       'FALSE')
    def value(self, p):
        pass
