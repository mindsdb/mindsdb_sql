import json
import re
from sly import Lexer

"""
Unfortunately we can't inherit from base SQLLexer, because the order of rules is important.
If we do, like in MySQL lexer, the new rules like `DATASOURCE = r'\bDATASOURCE\b'` are added to the end of the rule list.
Then, for an input `DATASOURCE`, the last matched regexp is `STRING`, and the token is incorrectly classified 
as a string.
"""
class MindsDBLexer(Lexer):
    reflags = re.IGNORECASE
    ignore = ' \t\n\r'

    tokens = {
        USE, DROP, CREATE, DESCRIBE, RETRAIN,

        # Misc
        SET, AUTOCOMMIT, START, TRANSACTION, COMMIT, ROLLBACK, ALTER, EXPLAIN,

        # Mindsdb special

        PREDICTOR, PREDICTORS, DATASOURCE, INTEGRATION, INTEGRATIONS,
        STREAM, STREAMS, PUBLICATION, PUBLICATIONS, VIEW, VIEWS,

        LATEST, HORIZON, USING,
        ENGINE, TRAIN, TEST, PREDICT, MODEL, PARAMETERS,


        # SHOW Keywords

        SHOW, SCHEMAS, DATABASES, TABLES, TABLE, FULL,
        VARIABLES, SESSION, STATUS,
        GLOBAL, PROCEDURE, FUNCTION, INDEX, WARNINGS,
        ENGINES, CHARSET, COLLATION, PLUGINS, CHARACTER,


        # SELECT Keywords
        WITH, SELECT, DISTINCT, FROM, WHERE, AS,
        LIMIT, OFFSET, ASC, DESC, NULLS_FIRST, NULLS_LAST,
        GROUP_BY, HAVING, ORDER_BY,
        STAR,

        JOIN, INNER, OUTER, CROSS, LEFT, RIGHT, ON,

        UNION, ALL,

        # Special
        DOT, COMMA, LPAREN, RPAREN, PARAMETER,

        # Operators
        PLUS, MINUS, DIVIDE, MODULO,
        EQUALS, NEQUALS, GREATER, GEQ, LESS, LEQ,
        AND, OR, NOT, IS,
        IN, LIKE, CONCAT, BETWEEN, WINDOW,

        # Data types
        CAST, ID, INTEGER, FLOAT, STRING, NULL, TRUE, FALSE,

        JSON,

    }

    RETRAIN = r'\bRETRAIN\b'
    # Custom commands

    USE = r'\bUSE\b'
    ENGINE = r'\bENGINE\b'
    TRAIN = r'\bTRAIN\b'
    TEST = r'\bTEST\b'
    PREDICT = r'\bPREDICT\b'
    MODEL = r'\bMODEL\b'
    DROP = r'\bDROP\b'
    PARAMETERS = r'\bPARAMETERS\b'
    HORIZON = r'\bHORIZON\b'
    USING = r'\bUSING\b'
    VIEW = r'\bVIEW\b'
    VIEWS = r'\bVIEWS\b'
    STREAM = r'\bSTREAM\b'
    STREAMS = r'\bSTREAMS\b'
    PREDICTOR = r'\bPREDICTOR\b'
    PREDICTORS = r'\bPREDICTORS\b'
    DATASOURCE = r'\bDATASOURCE\b'
    INTEGRATION = r'\bINTEGRATION\b'
    INTEGRATIONS = r'\bINTEGRATIONS\b'
    PUBLICATION = r'\bPUBLICATION\b'
    PUBLICATIONS = r'\bPUBLICATIONS\b'
    LATEST = r'\bLATEST\b'

    # Misc
    SET = r'\bSET\b'
    AUTOCOMMIT = r'\bAUTOCOMMIT\b'
    START = r'\bSTART\b'
    TRANSACTION = r'\bTRANSACTION\b'
    COMMIT = r'\bCOMMIT\b'
    ROLLBACK = r'\bROLLBACK\b'
    EXPLAIN = r'\bEXPLAIN\b'
    ALTER = r'\bALTER\b'

    DESCRIBE = r'\bDESCRIBE\b'

    # SHOW
    SHOW = r'\bSHOW\b'
    SCHEMAS = r'\bSCHEMAS\b'
    DATABASES = r'\bDATABASES\b'
    TABLES = r'\bTABLES\b'
    TABLE = r'\bTABLE\b'
    FULL = r'\bFULL\b'
    VARIABLES = r'\bVARIABLES\b'
    SESSION = r'\bSESSION\b'
    STATUS = r'\bSTATUS\b'
    GLOBAL = r'\bGLOBAL\b'
    PROCEDURE = r'\bPROCEDURE\b'
    FUNCTION = r'\bFUNCTION\b'
    INDEX = r'\bINDEX\b'
    CREATE = r'\bCREATE\b'
    WARNINGS = r'\bWARNINGS\b'
    ENGINES = r'\bENGINES\b'
    CHARSET = r'\bCHARSET\b'
    CHARACTER = r'\bCHARACTER\b'
    COLLATION = r'\bCOLLATION\b'
    PLUGINS = r'\bPLUGINS\b'


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

    JOIN = r'\bJOIN\b'
    INNER = r'\bINNER\b'
    OUTER = r'\bOUTER\b'
    CROSS = r'\bCROSS\b'
    LEFT = r'\bLEFT\b'
    RIGHT = r'\bRIGHT\b'

    # UNION

    UNION = r'\bUNION\b'
    ALL = r'\bALL\b'

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

    @_(r'\{.*\}')
    def JSON(self, t):
        t.value = json.loads(t.value)
        return t

    @_(r'"[^"]*"',
       r"'[^']*'")
    def STRING(self, t):
        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        else:
            t.value = t.value.strip('\'')
        return t
