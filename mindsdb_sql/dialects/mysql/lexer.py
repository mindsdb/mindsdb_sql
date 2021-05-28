from mindsdb_sql.lexer import SQLLexer


class MySQLLexer(SQLLexer):
    tokens = SQLLexer.tokens.union({VARIABLE})

    @_(r'@[a-zA-Z_.$]+',
       r"@'[a-zA-Z_.$][^']*'",
       r"@`[a-zA-Z_.$][^`]*`",
       r'@"[a-zA-Z_.$][^"]*"'
       )
    def VARIABLE(self, t):
        t.value = t.value[1:]

        if t.value[0] == '"':
            t.value = t.value.strip('\"')
        elif t.value[0] == "'":
            t.value = t.value.strip('\'')
        elif t.value[0] == "`":
            t.value = t.value.strip('`')
        return t



