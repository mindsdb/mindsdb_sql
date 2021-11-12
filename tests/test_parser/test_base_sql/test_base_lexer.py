import pytest
from mindsdb_sql.parser.lexer import SQLLexer
from mindsdb_sql.parser.dialects.mysql.lexer import MySQLLexer
from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer


@pytest.mark.parametrize('lexer', [SQLLexer(), MySQLLexer(), MindsDBLexer()])
class TestLexer:
    def test_select_basic(self, lexer):
        sql = f'SELECT 1'
        tokens = list(lexer.tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'INTEGER'
        assert tokens[1].value == 1

        sql = f'select 1'
        tokens = list(lexer.tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'INTEGER'
        assert tokens[1].value == 1

        sql = f'select a'
        tokens = list(lexer.tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'ID'
        assert tokens[1].value == 'a'

    def test_select_basic_ignored_symbols(self, lexer):
        sql = f'SELECT \t\r\n1'
        tokens = list(lexer.tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'INTEGER'
        assert tokens[1].value == 1

        sql = f'select 1'
        tokens = list(lexer.tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'INTEGER'
        assert tokens[1].value == 1

        sql = f'select a'
        tokens = list(lexer.tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'ID'
        assert tokens[1].value == 'a'

    def test_select_identifiers(self, lexer):
        sql = 'SELECT abcd123, __whatisthi123s__, `spaces in id`'
        tokens = list(lexer.tokenize(sql))
        assert tokens[0].type == 'SELECT'

        for i, t in enumerate(tokens[1:]):
            print(i, t)
            if i % 2 != 0:
                assert t.type == 'COMMA'
            else:
                assert t.type == 'ID'

    def test_select_identifiers_dots(self, lexer):
        sql = 'SELECT multiple.parts.__whoa, `multiple`.`parts`.`with brackets` '
        tokens = list(lexer.tokenize(sql))
        assert tokens[0].type == 'SELECT'

        for i, t in enumerate(tokens[1:]):
            print(i, t)
            if i % 2 != 0:
                assert t.type in ['COMMA', 'DOT']
            else:
                assert t.type == 'ID'

    def test_select_float(self, lexer):
        for float in [0.0, 1.000, 0.1, 1.0, 99999.9999]:
            sql = f'SELECT {float}'
            tokens = list(SQLLexer().tokenize(sql))

            assert tokens[0].type == 'SELECT'
            assert tokens[0].value == 'SELECT'

            assert tokens[1].type == 'FLOAT'
            assert tokens[1].value == float

    def test_select_strings(self, lexer):
        sql = 'SELECT "a", "b", "c"'
        tokens = list(SQLLexer().tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'STRING'
        assert tokens[1].value == 'a'
        assert tokens[2].type == 'COMMA'
        assert tokens[3].type == 'STRING'
        assert tokens[3].value == 'b'
        assert tokens[5].type == 'STRING'
        assert tokens[5].value == 'c'

        sql = "SELECT 'a', 'b', 'c'"
        tokens = list(SQLLexer().tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'STRING'
        assert tokens[1].value == 'a'
        assert tokens[2].type == 'COMMA'
        assert tokens[3].type == 'STRING'
        assert tokens[3].value == 'b'
        assert tokens[5].type == 'STRING'
        assert tokens[5].value == 'c'

    def test_select_strings_nested(self, lexer):
        sql = "SELECT '\"a\"', \"'b'\" "
        tokens = list(SQLLexer().tokenize(sql))
        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'STRING'
        assert tokens[1].value == '"a"'
        assert tokens[2].type == 'COMMA'
        assert tokens[3].type == 'STRING'
        assert tokens[3].value == "'b'"

    def test_binary_ops(self, lexer):
        for op, expected_type in [
            ('+', 'PLUS'),
            ('-', 'MINUS'),
            ('/', 'DIVIDE'),
            ('*', 'STAR'),
            ('%', 'MODULO'),
            ('=', 'EQUALS'),
            ('!=', 'NEQUALS'),
            ('>', 'GREATER'),
            ('>=', 'GEQ'),
            ('<', 'LESS'),
            ('<=', 'LEQ'),
            ('AND', 'AND'),
            ('OR', 'OR'),
            ('IS', 'IS'),
            # ('IS NOT', 'ISNOT'),
            ('LIKE', 'LIKE'),
            ('IN', 'IN'),
        ]:
            sql = f'SELECT 1 {op} 2'
            tokens = list(SQLLexer().tokenize(sql))
            assert tokens[0].type == 'SELECT'
            assert tokens[0].value == 'SELECT'

            assert tokens[1].type == 'INTEGER'
            assert tokens[1].value == 1

            assert tokens[2].type == expected_type
            assert tokens[2].value == op

            assert tokens[3].type == 'INTEGER'
            assert tokens[3].value == 2

    def test_binary_ops_not(self, lexer):
        for op, expected_type in [
            ('IS NOT', 'IS NOT'),
            ('NOT IN', 'NOT IN'),
        ]:
            sql = f'SELECT 1 {op} 2'
            tokens = list(SQLLexer().tokenize(sql))
            assert tokens[0].type == 'SELECT'
            assert tokens[0].value == 'SELECT'

            assert tokens[1].type == 'INTEGER'
            assert tokens[1].value == 1

            assert tokens[2].type + ' ' + tokens[3].type == expected_type

            assert tokens[4].type == 'INTEGER'
            assert tokens[4].value == 2

    def test_select_from(self, lexer):
        sql = f'SELECT column AS other_column FROM db.schema.tab'
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'ID'
        assert tokens[1].value == 'column'

        assert tokens[2].type == 'AS'
        assert tokens[2].value == 'AS'

        assert tokens[3].type == 'ID'
        assert tokens[3].value == 'other_column'

        assert tokens[4].type == 'FROM'
        assert tokens[4].value == 'FROM'

        assert tokens[5].type == 'ID'
        assert tokens[5].value == 'db'
        assert tokens[6].type == 'DOT'
        assert tokens[7].type == 'ID'
        assert tokens[7].value == 'schema'
        assert tokens[8].type == 'DOT'
        assert tokens[9].type == 'ID'
        assert tokens[9].value == 'tab'

    def test_select_star(self, lexer):
        sql = f'SELECT * FROM tab'
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'STAR'
        assert tokens[1].value == '*'

        assert tokens[2].type == 'FROM'
        assert tokens[2].value == 'FROM'

        assert tokens[3].type == 'ID'
        assert tokens[3].value == 'tab'

    def test_select_where(self, lexer):
        sql = f'SELECT column FROM tab WHERE column = "something"'
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'ID'
        assert tokens[2].type == 'FROM'
        assert tokens[3].type == 'ID'
        assert tokens[4].type == 'WHERE'
        assert tokens[4].value == 'WHERE'
        assert tokens[5].type == 'ID'
        assert tokens[5].value == 'column'
        assert tokens[6].type == 'EQUALS'
        assert tokens[6].value == '='
        assert tokens[7].type == 'STRING'
        assert tokens[7].value == 'something'

    def test_select_group_by(self, lexer):
        sql = f'SELECT column, sum(column2) FROM tab GROUP BY column'
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'ID'
        assert tokens[2].type == 'COMMA'
        assert tokens[3].type == 'ID'
        assert tokens[4].type == 'LPAREN'
        assert tokens[5].type == 'ID'
        assert tokens[6].type == 'RPAREN'
        assert tokens[7].type == 'FROM'
        assert tokens[8].type == 'ID'
        assert tokens[9].type == 'GROUP_BY'
        assert tokens[9].value == 'GROUP BY'
        assert tokens[10].type == 'ID'
        assert tokens[10].value == 'column'

    def test_select_order_by(self, lexer):
        for order_dir in ['ASC', 'DESC']:
            sql = f'SELECT column, sum(column2) FROM tab ORDER BY column {order_dir}'
            tokens = list(SQLLexer().tokenize(sql))

            assert tokens[0].type == 'SELECT'
            assert tokens[1].type == 'ID'
            assert tokens[2].type == 'COMMA'
            assert tokens[3].type == 'ID'
            assert tokens[4].type == 'LPAREN'
            assert tokens[5].type == 'ID'
            assert tokens[6].type == 'RPAREN'
            assert tokens[7].type == 'FROM'
            assert tokens[8].type == 'ID'
            assert tokens[9].type == 'ORDER_BY'
            assert tokens[9].value == 'ORDER BY'
            assert tokens[10].type == 'ID'
            assert tokens[10].value == 'column'
            assert tokens[11].type == order_dir
            assert tokens[11].value == order_dir

    def test_as_ones(self, lexer):
        sql = "SELECT *, (SELECT 1) AS ones FROM t1"
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'STAR'
        assert tokens[2].type == 'COMMA'
        assert tokens[3].type == 'LPAREN'
        assert tokens[4].type == 'SELECT'
        assert tokens[5].type == 'INTEGER'
        assert tokens[6].type == 'RPAREN'
        assert tokens[7].type == 'AS'
        assert tokens[8].type == 'ID'
        assert tokens[9].type == 'FROM'
        assert tokens[10].type == 'ID'

        sql = "SELECT *, (SELECT 1) AS ones FROM t1".lower()
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[1].type == 'STAR'
        assert tokens[2].type == 'COMMA'
        assert tokens[3].type == 'LPAREN'
        assert tokens[4].type == 'SELECT'
        assert tokens[5].type == 'INTEGER'
        assert tokens[6].type == 'RPAREN'
        assert tokens[7].type == 'AS'
        assert tokens[8].type == 'ID'
        assert tokens[9].type == 'FROM'
        assert tokens[10].type == 'ID'

    def test_select_parameter(self, lexer):
        sql = f'SELECT ?'
        tokens = list(lexer.tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'PARAMETER'
        assert tokens[1].value == '?'

    def test_show_character_set(self, lexer):
        lexer = SQLLexer()
        sql = "show character set where charset = 'utf8mb4'"
        tokens = list(lexer.tokenize(sql))

        assert tokens[0].type == 'SHOW'
        assert tokens[1].type == 'CHARACTER'
        assert tokens[2].type == 'SET'
        assert tokens[3].type == 'WHERE'
        assert tokens[4].type == 'CHARSET'
        assert tokens[4].value == 'charset'
        assert tokens[5].value == '='
        assert tokens[6].type == 'STRING'
        assert tokens[6].value == 'utf8mb4'

