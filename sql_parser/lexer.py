from sly import Lexer


class SQLLexer(Lexer):
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
        IN, LIKE,

        # Data types
        ID, INTEGER, FLOAT, STRING, NULL, TRUE, FALSE }
    ignore = ' \t\n'

    # Tokens
    ON = r'ON'
    ASC = r'ASC'
    DESC = r'DESC'
    NULLS_FIRST = r'NULLS FIRST'
    NULLS_LAST = r'NULLS LAST'
    SELECT = r'SELECT'
    DISTINCT = r'DISTINCT'
    FROM = r'FROM'
    AS = r'AS'
    WHERE = r'WHERE'
    LIMIT = r'LIMIT'
    OFFSET = r'OFFSET'
    GROUP_BY = r'GROUP BY'
    HAVING = r'HAVING'
    ORDER_BY = r'ORDER BY'
    STAR = r'\*'

    INNER_JOIN = r'INNER JOIN'
    OUTER_JOIN = r'OUTER JOIN'
    CROSS_JOIN = r'CROSS JOIN'
    LEFT_JOIN = r'LEFT JOIN'
    RIGHT_JOIN = r'RIGHT JOIN'
    FULL_JOIN = r'FULL JOIN'


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
    IS = r'IS'
    LIKE = r'LIKE'
    IN = r'IN'

    # Data types
    ID = r'[a-zA-Z][a-zA-Z_.0-9]+'
    NULL = r'NULL'
    TRUE = r'TRUE'
    FALSE = r'FALSE'

    @_(r'\d+\.\d+')
    def FLOAT(self, t):
        t.value = float(t.value)
        return t

    @_(r'\d+')
    def INTEGER(self, t):
        t.value = int(t.value)
        return t

    @_(r'"[^"]*"')
    def STRING(self, t):
        t.value = t.value.strip('\"')
        return t
