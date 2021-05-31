from mindsdb_sql.exceptions import ParsingException


def parse_sql(sql, dialect='sqlite'):
    if dialect == 'sqlite':
        from mindsdb_sql.parser import SQLParser
        from mindsdb_sql.lexer import SQLLexer
        lexer, parser = SQLLexer(), SQLParser()
    elif dialect == 'mysql':
        from mindsdb_sql.dialects.mysql.lexer import MySQLLexer
        from mindsdb_sql.dialects.mysql.parser import MySQLParser
        lexer, parser = MySQLLexer(), MySQLParser()
    else:
        raise ParsingException(f'Unknown dialect {dialect}. Available options are: sqlite, mysql.')
    tokens = lexer.tokenize(sql)
    ast = parser.parse(tokens)
    return ast
