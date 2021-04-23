from sly import Lexer


class SQLLexer(Lexer):
    tokens = {
        # Keywords
        SELECT, DISTINCT, FROM, WHERE, AS,
        LIMIT, OFFSET, ASC, DESC,
        GROUPBY, HAVING, ORDERBY,
        STAR,

        # Symbols
        COMMA, LPAREN, RPAREN,

        # Operators
        PLUS, MINUS, DIVIDE, MODULO,
        EQUALS, NEQUALS, GREATER, GEQ, LESS, LEQ,
        AND, OR, NOT, IS, ISNOT,
        IN, LIKE,

        # Data types
        ID, INTEGER, FLOAT, STRING }
    ignore = ' \t\n'

    # Tokens
    ASC = r'ASC'
    DESC = r'DESC'
    SELECT = r'SELECT'
    DISTINCT = r'DISTINCT'
    FROM = r'FROM'
    AS = r'AS'
    WHERE = r'WHERE'
    LIMIT = r'LIMIT'
    OFFSET = r'OFFSET'
    GROUPBY = r'GROUP BY'
    HAVING = r'HAVING'
    ORDERBY = r'ORDER BY'
    STAR = r'\*'

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
    AND = r'AND'
    OR = r'OR'
    NOT = r'NOT'
    ISNOT = r'IS NOT'
    IS = r'IS'
    LIKE = r'LIKE'
    IN = r'IN'

    # Data types
    ID = r'[a-zA-Z][a-zA-Z_.0-9]+'

    @_(r'\d+\.\d+')
    def FLOAT(self, t):
        t.value = float(t.value)
        return t

    @_(r'\d+')
    def INTEGER(self, t):
        t.value = int(t.value)
        return t

    @_(r'".*"')
    def STRING(self, t):
        t.value = t.value.strip('\"')
        return t
