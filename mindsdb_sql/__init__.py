from mindsdb_sql.parser import SQLParser
from mindsdb_sql.lexer import SQLLexer


def parse_sql(sql):
    tokens = SQLLexer().tokenize(sql)
    ast = SQLParser().parse(tokens)
    return ast
