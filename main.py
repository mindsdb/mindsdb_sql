from mindsdb_sql.parser.lexer import SQLLexer
from mindsdb_sql.parser import SQLParser

if __name__ == '__main__':
    lexer = SQLLexer()
    parser = SQLParser()
    while True:
        try:
            text = input('sql > ')
        except EOFError:
            break
        if text:
            for tok in lexer.tokenize(text):
                print('type=%r, value=%r' % (tok.type, tok.value))

            # parser.parse(lexer.tokenize(text))