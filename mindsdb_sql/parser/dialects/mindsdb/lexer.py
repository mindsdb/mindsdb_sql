import re
from sly import Lexer


class MindsDBLexer(Lexer):
    reflags = re.IGNORECASE
    ignore = ' \t\n'

    tokens = {
        CREATE, SHOW, USE, DROP,

        VIEW, VIEWS, PREDICTOR, PREDICTORS, INTEGRATION, INTEGRATIONS,
        STREAM, STREAMS, TABLE, TABLES, PUBLICATION, PUBLICATIONS,

        # Mindsdb special
        LATEST,

        ENGINE, TRAIN, TEST, PREDICT, MODEL,

        # SELECT Keywords
        SELECT, DISTINCT, FROM, WHERE, AS,
        LIMIT, OFFSET, ASC, DESC, NULLS_FIRST, NULLS_LAST,
        GROUP_BY, HAVING, ORDER_BY,
        STAR,

        JOIN, INNER_JOIN, OUTER_JOIN, CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, ON,

        UNION, ALL,

        # Special
        COMMA, LPAREN, RPAREN, PARAMETER,

        # Operators
        PLUS, MINUS, DIVIDE, MODULO,
        EQUALS, NEQUALS, GREATER, GEQ, LESS, LEQ,
        AND, OR, NOT, IS,
        IN, LIKE, CONCAT, BETWEEN, WINDOW,

        # Data types
        CAST, ID, INTEGER, FLOAT, STRING, NULL, TRUE, FALSE}

    # Custom commands

    USE = r'\bUSE\b'
    SHOW = r'\bSHOW\b'
    CREATE = r'\bCREATE\b'
    VIEW = r'\bVIEW\b'
    VIEWS = r'\bVIEWS\b'
    STREAM = r'\bSTREAM\b'
    STREAMS = r'\bSTREAMS\b'
    TABLE = r'\bTABLE\b'
    TABLES = r'\bTABLES\b'
    PREDICTOR = r'\bPREDICTOR\b'
    PREDICTORS = r'\bPREDICTORS\b'
    INTEGRATION = r'\bINTEGRATION\b'
    INTEGRATIONS = r'\bINTEGRATIONS\b'
    PUBLICATION = r'\bPUBLICATION\b'
    PUBLICATIONS = r'\bPUBLICATIONS\b'
    ENGINE = r'\bENGINE\b'
    TRAIN = r'\bTRAIN\b'
    TEST = r'\bTEST\b'
    PREDICT = r'\bPREDICT\b'
    MODEL = r'\bMODEL\b'
    DROP = r'\bDROP\b'

    # SELECT

    # Keywords
    ON = r'\bON\b'
    ASC = r'\bASC\b'
    DESC = r'\bDESC\b'
    NULLS_FIRST = r'\bNULLS FIRST\b'
    NULLS_LAST = r'\bNULLS LAST\b'
    SELECT = r'\bSELECT\b'
    DISTINCT = r'\bDISTINCT\b'
    FROM = r'\bFROM\b'
    AS = r'\bAS\b'
    WHERE = r'\bWHERE\b'
    LIMIT = r'\bLIMIT\b'
    OFFSET = r'\bOFFSET\b'
    GROUP_BY = r'\bGROUP BY\b'
    HAVING = r'\bHAVING\b'
    ORDER_BY = r'\bORDER BY\b'
    STAR = r'\*'
    LATEST = r'\bLATEST\b'

    JOIN = r'\bJOIN\b'
    INNER_JOIN = r'\bINNER JOIN\b'
    OUTER_JOIN = r'\bOUTER JOIN\b'
    CROSS_JOIN = r'\bCROSS JOIN\b'
    LEFT_JOIN = r'\bLEFT JOIN\b'
    RIGHT_JOIN = r'\bRIGHT JOIN\b'
    FULL_JOIN = r'\bFULL JOIN\b'

    UNION = r'\bUNION\b'
    ALL = r'\bALL\b'

    # Special
    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'
    PARAMETER = r'\?'

    # Operators
    PLUS = r'\+'
    MINUS = r'-'
    DIVIDE = r'/'
    MODULO = r'%'
    EQUALS = r'='
    NEQUALS = r'!='
    GEQ = r'>='
    GREATER = r'>'
    LEQ = r'<='
    LESS = r'<'
    AND = r'\bAND\b'
    OR = r'\bOR\b'
    NOT = r'\bNOT\b'
    IS = r'\bIS\b'
    LIKE = r'\bLIKE\b'
    IN = r'\bIN\b'
    CAST = r'\bCAST\b'
    CONCAT = r'\|\|'
    BETWEEN = r'\bBETWEEN\b'
    WINDOW = r'\bWINDOW\b'

    # Data types
    NULL = r'\bNULL\b'
    TRUE = r'\bTRUE\b'
    FALSE = r'\bFALSE\b'

    @_(r'([a-zA-Z_][a-zA-Z_0-9]*|`([^`]+)`)(\.([a-zA-Z_][a-zA-Z_.0-9]*|`([^`]+)`))*')
    def ID(self, t):
        return t

    @_(r'\d+\.\d+')
    def FLOAT(self, t):
        t.value = float(t.value)
        return t

    @_(r'\d+')
    def INTEGER(self, t):
        t.value = int(t.value)
        return t

    @_(r'"[^"]*"',
       r"'[^']*'")
    def STRING(self, t):
        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        else:
            t.value = t.value.strip('\'')
        return t
