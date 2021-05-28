from mindsdb_sql.parser import SQLParser
from mindsdb_sql.dialects.mysql.lexer import MySQLLexer
from mindsdb_sql.dialects.mysql.variable import Variable



class MySQLParser(SQLParser):
    tokens = MySQLLexer.tokens

    @_('variable')
    def table_or_subquery(self, p):
        return p.variable

    @_('variable')
    def expr(self, p):
        return p.variable

    @_('VARIABLE')
    def variable(self, p):
        return Variable(value=p.VARIABLE)
