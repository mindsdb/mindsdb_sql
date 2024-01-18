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

    # Get a list of low priority tokens to set for right priority
    sql_tokens = tokens.copy()
    for parsed in ['NAT_ID', 'AMB_ID', 'MINDS_ID']:
        sql_tokens.remove(parsed)

    right_precedence_tokens = sql_tokens.copy()
    for parsed in ['AS', 'DOT', 'COMMA']:
        right_precedence_tokens.remove(parsed)

    precedence = (
        ('right', *list(right_precedence_tokens)),
        ('right', AS, COMMA),
        ('right', DOT),
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
            raise ParsingException(f"Query has conflicting domains.\n MindsDB domain: {self.mindsdb_domain}.\n Native domain: {self.native_domain}.\n Only one domain can have more than 1 member.")

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

    ################################################ Check Domain ######################################################
    # perform domain checks as aliases are resolved.
    @_('nat_id',
       'minds_id',
       'amb_id')
    def id(self, p):
        # check domain only for native ids.
        if hasattr(p, 'nat_id') or hasattr(p, 'minds_id'):
            self._check_domain(p[0])
        return p[0]

    # Resolve Aliases. Aliases cannot be named ids, they must be ambiguous.
    @_('nat_id AS amb_id')
    def nat_id(self, p):
        p[0].alias = p[2]
        self._resolve_id_references(p[0])
        return p[0]

    @_('minds_id AS amb_id')
    def minds_id(self, p):
        p[0].alias = p[2]
        self._resolve_id_references(p[0])
        return p[0]

    @_('amb_id AS amb_id')
    def amb_id(self, p):
        p[0].alias = p[2]
        self._resolve_id_references(p[0])
        return p[0]

    # Parse dot names, correcting types.
    @_('nat_id DOT nat_id',
       'nat_id DOT amb_id',
       'nat_id DOT minds_id')
    def nat_id(self, p):
        p[2].id_type = Identifier.ID_TYPE.NATIVE
        identifier = Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                                left=p[0],
                                right=p[2])
        return identifier

    @_('minds_id DOT minds_id',
       'minds_id DOT nat_id',
       'minds_id DOT amb_id')
    def minds_id(self, p):
        p[2].id_type = Identifier.ID_TYPE.MINDSDB
        identifier = Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                                left=p[0],
                                right=p[2])
        return identifier

    @_('amb_id DOT amb_id',
       'amb_id DOT nat_id',
       'amb_id DOT minds_id')
    def amb_id(self, p):
        p[2].id_type = Identifier.ID_TYPE.AMBIGUOUS
        identifier = Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                                left=p[0],
                                right=p[2])
        return identifier

    @_('NAT_ID')
    def nat_id(self, p):
        type(self).log.debug(f"found id {p[0]}")
        temp = Identifier(id_type=Identifier.ID_TYPE.NATIVE, value=p.NAT_ID)

        return temp

    @_('MINDS_ID')
    def minds_id(self, p):
        type(self).log.debug(f"found id {p[0]}")
        temp = Identifier(id_type=Identifier.ID_TYPE.MINDSDB, value=p.MINDS_ID)

        return temp

    @_('AMB_ID')
    def amb_id(self, p):
        type(self).log.debug(f"found id {p[0]}")
        temp = Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS, value=p.AMB_ID)

        return temp
