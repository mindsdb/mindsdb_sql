import re
from sly import Lexer

RESERVED_KEYWORDS = ['DATABASE']

class SQLLexer(Lexer):
    reflags = re.IGNORECASE
    ignore = ' \t\n\r'

    tokens = {
        USE, DROP, CREATE, DESCRIBE,

        # Misc
        SET, START, TRANSACTION, COMMIT, ROLLBACK, ALTER, EXPLAIN,
        ISOLATION, LEVEL, REPEATABLE, READ, WRITE, UNCOMMITTED, COMMITTED,
        SERIALIZABLE, ONLY, CONVERT, USING,

        # SHOW Keywords/DDL Keywords

        SHOW, SCHEMAS, SCHEMA, DATABASES, DATABASE, TABLES, TABLE, FULL,
        VIEW, VARIABLES, SESSION, STATUS,
        GLOBAL, PROCEDURE, FUNCTION, INDEX, WARNINGS,
        ENGINES, CHARSET, COLLATION, PLUGINS, CHARACTER,
        PERSIST, PERSIST_ONLY, DEFAULT,

        IF_EXISTS,

        # SELECT Keywords
        WITH, SELECT, DISTINCT, FROM, WHERE, AS,
        LIMIT, OFFSET, ASC, DESC, NULLS_FIRST, NULLS_LAST,
        GROUP_BY, HAVING, ORDER_BY,
        STAR, FOR, UPDATE,

        JOIN, INNER, OUTER, CROSS, LEFT, RIGHT, ON,

        UNION, ALL,

        # DML
        INSERT, INTO, VALUES,

        # Special
        DOT, COMMA, LPAREN, RPAREN, PARAMETER,

        # Operators
        PLUS, MINUS, DIVIDE, MODULO,
        EQUALS, NEQUALS, GREATER, GEQ, LESS, LEQ,
        AND, OR, NOT, IS,
        IN, LIKE, CONCAT, BETWEEN, WINDOW, OVER, PARTITION_BY,

        # Data types
        CAST, ID, INTEGER, FLOAT, QUOTE_STRING, DQUOTE_STRING, NULL, TRUE, FALSE}

    # Misc
    SET = r'\bSET\b'
    START = r'\bSTART\b'
    TRANSACTION = r'\bTRANSACTION\b'
    COMMIT = r'\bCOMMIT\b'
    ROLLBACK = r'\bROLLBACK\b'
    EXPLAIN = r'\bEXPLAIN\b'
    ALTER = r'\bALTER\b'
    ISOLATION = r'\bISOLATION\b'
    LEVEL = r'\bLEVEL\b'
    REPEATABLE = r'\bREPEATABLE\b'
    READ = r'\bREAD\b'
    WRITE = r'\bWRITE\b'
    UNCOMMITTED = r'\bUNCOMMITTED\b'
    COMMITTED = r'\bCOMMITTED\b'
    SERIALIZABLE = r'\bSERIALIZABLE\b'
    ONLY = r'\bONLY\b'
    CONVERT = r'\bCONVERT\b'
    USING = r'\bUSING\b'

    USE = r'\bUSE\b'
    DESCRIBE = r'\bDESCRIBE\b'

    # SHOW
    SHOW = r'\bSHOW\b'
    SCHEMAS = r'\bSCHEMAS\b'
    SCHEMA = r'\bSCHEMA\b'
    DATABASES = r'\bDATABASES\b'
    DATABASE = r'\bDATABASE\b'
    TABLES = r'\bTABLES\b'
    TABLE = r'\bTABLE\b'
    VIEW = r'\bVIEW\b'
    FULL = r'\bFULL\b'
    VARIABLES = r'\bVARIABLES\b'
    SESSION = r'\bSESSION\b'
    STATUS = r'\bSTATUS\b'
    GLOBAL = r'\bGLOBAL\b'
    PROCEDURE = r'\bPROCEDURE\b'
    FUNCTION = r'\bFUNCTION\b'
    INDEX = r'\bINDEX\b'
    DROP = r'\bDROP\b'
    CREATE = r'\bCREATE\b'
    WARNINGS = r'\bWARNINGS\b'
    ENGINES = r'\bENGINES\b'
    CHARSET = r'\bCHARSET\b'
    CHARACTER = r'\bCHARACTER\b'
    COLLATION = r'\bCOLLATION\b'
    PLUGINS = r'\bPLUGINS\b'
    PERSIST = r'\bPERSIST\b'
    PERSIST_ONLY = r'\bPERSIST_ONLY\b'
    DEFAULT = r'\bDEFAULT\b'
    IF_EXISTS = r'\bIF[\s]+EXISTS\b'

    # SELECT

    ON = r'\bON\b'
    ASC = r'\bASC\b'
    DESC = r'\bDESC\b'
    NULLS_FIRST = r'\bNULLS FIRST\b'
    NULLS_LAST = r'\bNULLS LAST\b'
    WITH = r'\bWITH\b'
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
    FOR = r'\bFOR\b'
    UPDATE = r'\bUPDATE\b'

    JOIN = r'\bJOIN\b'
    INNER = r'\bINNER\b'
    OUTER = r'\bOUTER\b'
    CROSS = r'\bCROSS\b'
    LEFT = r'\bLEFT\b'
    RIGHT = r'\bRIGHT\b'

    # UNION

    UNION = r'\bUNION\b'
    ALL = r'\bALL\b'

    # DML
    INSERT = r'\bINSERT\b'
    INTO = r'\bINTO\b'
    VALUES = r'\bVALUES\b'

    # Special
    DOT = r'\.'
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
    OVER = r'\bOVER\b'
    PARTITION_BY = r'\bPARTITION BY\b'

    # Data types
    NULL = r'\bNULL\b'
    TRUE = r'\bTRUE\b'
    FALSE = r'\bFALSE\b'

    @_(r'(?:([a-zA-Z_$0-9]*[a-zA-Z_$]+[a-zA-Z_$0-9]*)|(?:`([^`]+)`))(?:\.(?:([a-zA-Z_$0-9]*[a-zA-Z_$]+[a-zA-Z_$0-9]*)|(?:`([^`]+)`)))*')
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

    @_(r"'[^']*'")
    def QUOTE_STRING(self, t):
        t.value = t.value.strip('\'')
        return t

    @_(r'"[^"]*"')
    def DQUOTE_STRING(self, t):
        t.value = t.value.strip('\"')
        return t

