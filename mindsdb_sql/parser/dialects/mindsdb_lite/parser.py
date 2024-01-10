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
    """
    precedence = (
        ('left', OR),
        ('left', AND),
        ('right', UNOT),
        ('left', EQUALS, NEQUALS),
        ('left', PLUS, MINUS),
        ('left', STAR, DIVIDE),
        ('right', UMINUS),  # Unary minus operator, unary not
        ('nonassoc', LESS, LEQ, GREATER, GEQ, IN, BETWEEN, IS, IS_NOT, LIKE),
    )
    """

    def register_integrations(self, mindsdb_obs, native_ints):
        self.mindsdb_query = False
        self.mindsdb_objs = mindsdb_obs

        self.native_query = False
        self.native_ints = native_ints

    ################################################ Parse IDs #########################################################
    sql_tokens = tokens.copy()
    for parsed in ['NAT_ID', 'AMB_ID', 'MINDS_ID']:
        sql_tokens.remove(parsed)

    @_(*list(sql_tokens))
    def sql_statement(self, p):
        type(self).log.error(f"found sql_statement {p[0]}")
        return [p[0], ]

    @_('sql_statement sql_statement',
       'sql_statement id_list',
       'sql_statement id',
       )
    def sql_statement(self, p):
        type(self).log.error(f"combined sql_statements")
        if hasattr(p, 'id'):
            return p[0] + [p[1],]
        else:
            return p[0] + p[1]

    @_('NAT_ID', 'MINDS_ID', 'AMB_ID')
    def id(self, p):
        if hasattr(p, 'NAT_ID'):
            type(self).log.error(f"found id {p[0]}")
            temp = Identifier(id_type=Identifier.ID_TYPE.NATIVE, value=p.NAT_ID)
        elif hasattr(p, 'MINDS_ID'):
            type(self).log.error(f"found id {p[0]}")
            temp = Identifier(id_type=Identifier.ID_TYPE.MINDSDB, value=p.MINDS_ID)
        elif hasattr(p, 'AMB_ID'):
            type(self).log.error(f"found id {p[0]}")
            temp = Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS, value=p.AMB_ID)

        return temp


    @_('NAT_ID DOT NAT_ID',
       'NAT_ID DOT AMB_ID',
       'NAT_ID DOT MINDS_ID')
    def id(self, p):
        id = Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                        left=Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                                        value=p[0]),
                        right=Identifier(id_type=Identifier.ID_TYPE.NATIVE,
                                         value=p[2]))
        return id

    @_('MINDS_ID DOT MINDS_ID',
       'MINDS_ID DOT NAT_ID',
       'MINDS_ID DOT AMB_ID')
    def id(self, p):
        id = Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                        left=Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                                        value=p[0]),
                        right=Identifier(id_type=Identifier.ID_TYPE.MINDSDB,
                                         value=p[2]))
        return id

    @_('AMB_ID DOT AMB_ID',
       'AMB_ID DOT NAT_ID',
       'AMB_ID DOT MINDS_ID')
    def id(self, p):
        id = Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                        left=Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                                        value=p[0]),
                        right=Identifier(id_type=Identifier.ID_TYPE.AMBIGUOUS,
                                         value=p[2]))
        return id

    @_('id AS id')
    def id(self, p):
        p[0].alias = p[2]

    @_('id COMMA id',
       'id_list COMMA id')
    def id_list(self, p):
        if hasattr(p, 'id_list'):
            return p[0].append(p[2])
        else:
            return [p[0], p[1]]


