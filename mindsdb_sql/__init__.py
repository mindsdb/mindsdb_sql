from mindsdb_sql.exceptions import ParsingException


def get_lexer_parser(dialect):
    if dialect == 'sqlite':
        from mindsdb_sql.parser import SQLParser
        from mindsdb_sql.lexer import SQLLexer
        lexer, parser = SQLLexer(), SQLParser()
    elif dialect == 'mysql':
        from mindsdb_sql.dialects.mysql.lexer import MySQLLexer
        from mindsdb_sql.dialects.mysql.parser import MySQLParser
        lexer, parser = MySQLLexer(), MySQLParser()
    elif dialect == 'mindsdb':
        from mindsdb_sql.dialects.mindsdb.lexer import MindsDBLexer
        from mindsdb_sql.dialects.mindsdb.parser import MindsDBParser
        lexer, parser = MindsDBLexer(), MindsDBParser()
    else:
        raise ParsingException(f'Unknown dialect {dialect}. Available options are: sqlite, mysql.')
    return lexer, parser


def parse_sql(sql, dialect='sqlite'):
    lexer, parser = get_lexer_parser(dialect)
    tokens = lexer.tokenize(sql)
    ast = parser.parse(tokens)
    return ast
