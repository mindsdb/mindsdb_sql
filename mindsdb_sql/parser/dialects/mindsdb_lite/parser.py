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
    for parsed in ['AS', 'DOT']:
        right_precedence_tokens.remove(parsed)

    precedence = (
        ('right', *list(right_precedence_tokens)),
        ('left', AS),
        ('left', DOT),
    )

    def register_integrations(self, mindsdb_obs, native_ints):
        self.query_domain = None
        self.mindsdb_objs = mindsdb_obs
        self.native_ints = native_ints

    def _check_domain(self, check_val: Identifier.ID_TYPE):
        if self.query_domain == None:
            self.query_domain = check_val
            return
        elif self.query_domain == check_val:
            return
        else:
            return
            raise ParsingException(f"Query has conflictting domains: {self.query_domain}, {check_val}")

    def _resolve_id_references(self, id: Identifier):
        for symbol in self.symstack:
            if type(symbol) == "sql_statement":
                for sym in symbol.value:
                    if type(sym) == Identifier and sym.id_type == Identifier.ID_TYPE.AMBIGUOUS:
                        if sym.left.value == id.alias.value:
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
       'sql_statement nat_id',
       'sql_statement minds_id',
       'sql_statement amb_id')
    def sql_statement(self, p):
        type(self).log.debug(f"combined sql_statements")
        if hasattr(p, 'nat_id') or hasattr(p, 'minds_id') or hasattr(p, 'amb_id'):
            return p[0] + [p[1], ]
        else:
            return p[0] + p[1]

    ################################################ Parse MindsDB SQL #################################################
    # Should be done first to determine if queries are Native or MindsDB
    """
    @_('SELECT id FROM id where',
       'SELECT id FROM id_list where',
       'SELECT id_list FROM id where',
       'SELECT id_list FROM id_list where',
       'SELECT id_list FROM id_list where',
       'SELECT id_list FROM join where group_by')

    @_('id JOIN id',
       'id_list JOIN id_list')
    def join(self, p):
        pass
    """

    ################################################ Parse IDs #########################################################
    # Should be done first to determine if queries are Native or MindsDB
    @_('nat_id COMMA nat_id',
       'nat_id COMMA amb_id',
       'nat_id COMMA minds_id',
       'minds_id COMMA minds_id',
       'minds_id COMMA nat_id',
       'minds_id COMMA amb_id',
       'amb_id COMMA amb_id',
       'amb_id COMMA nat_id',
       'amb_id COMMA minds_id',
       'id_list COMMA nat_id',
       'id_list COMMA amb_id',
       'id_list COMMA minds_id'
       )
    def id_list(self, p):
        if hasattr(p, 'id_list'):
            return p[0] + [p[2], ]
        else:
            return [p[0], p[2]]


    # Aliases cannot be named ids, they must be ambiguous.
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

    @_('nat_id DOT nat_id',
       'nat_id DOT amb_id',
       'nat_id DOT minds_id')
    def nat_id(self, p):
        id = Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                        left=Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                                        value=p[0]),
                        right=Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                                         value=p[2]))
        self._check_domain(Identifier.ID_TYPE.NATIVE)
        return id

    @_('minds_id DOT minds_id',
       'minds_id DOT nat_id',
       'minds_id DOT amb_id')
    def minds_id(self, p):
        id = Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                        left=Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                                        value=p[0]),
                        right=Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                                         value=p[2]))
        self._check_domain(Identifier.ID_TYPE.MINDSDB)
        return id

    @_('amb_id DOT amb_id',
       'amb_id DOT nat_id',
       'amb_id DOT minds_id')
    def amb_id(self, p):
        id = Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                        left=Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                                        value=p[0]),
                        right=Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                                         value=p[2]))
        self._check_domain(Identifier.ID_TYPE.AMBIGUOUS)
        return id


    @_('NAT_ID')
    def nat_id(self, p):
        type(self).log.debug(f"found id {p[0]}")
        temp = Identifier(id_type=Identifier.ID_TYPE.NATIVE, value=p.NAT_ID)
        self._check_domain(Identifier.ID_TYPE.NATIVE)

        return temp

    @_('MINDS_ID')
    def minds_id(self, p):
        type(self).log.debug(f"found id {p[0]}")
        temp = Identifier(id_type=Identifier.ID_TYPE.MINDSDB, value=p.MINDS_ID)
        self._check_domain(Identifier.ID_TYPE.MINDSDB)

        return temp

    @_('AMB_ID')
    def amb_id(self, p):
        type(self).log.debug(f"found id {p[0]}")
        temp = Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS, value=p.AMB_ID)
        self._check_domain(Identifier.ID_TYPE.AMBIGUOUS)

        return temp
