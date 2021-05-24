import re
from sly import Lexer


class SQLLexer(Lexer):
    reflags = re.IGNORECASE

    tokens = {
        # Keywords
        SELECT, DISTINCT, FROM, WHERE, AS,
        LIMIT, OFFSET, ASC, DESC, NULLS_FIRST, NULLS_LAST,
        GROUP_BY, HAVING, ORDER_BY,
        STAR,

        INNER_JOIN, OUTER_JOIN, CROSS_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN, ON,

        # Symbols
        COMMA, LPAREN, RPAREN,

        # Operators
        PLUS, MINUS, DIVIDE, MODULO,
        EQUALS, NEQUALS, GREATER, GEQ, LESS, LEQ,
        AND, OR, NOT, IS,
        IN, LIKE, CONCAT,

        # Data types
        CAST, ID, INTEGER, FLOAT, STRING, NULL, TRUE, FALSE }
    ignore = ' \t\n'

    # Tokens
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

    INNER_JOIN = r'\bINNER JOIN\b'
    OUTER_JOIN = r'\bOUTER JOIN\b'
    CROSS_JOIN = r'\bCROSS JOIN\b'
    LEFT_JOIN = r'\bLEFT JOIN\b'
    RIGHT_JOIN = r'\bRIGHT JOIN\b'
    FULL_JOIN = r'\bFULL JOIN\b'


    COMMA = r','
    LPAREN = r'\('
    RPAREN = r'\)'

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

    # Data types
    NULL = r'\bNULL\b'
    TRUE = r'\bTRUE\b'
    FALSE = r'\bFALSE\b'
    ID = r'[a-zA-Z][a-zA-Z_.0-9]+'

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
