from mindsdb_sql.lexer import SQLLexer


class TestLexer:
    def test_select_basic(self):
        sql = f'SELECT 1'
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'INTEGER'
        assert tokens[1].value == 1

    def test_select_float(self):
        for float in [0.0, 1.000, 0.1, 1.0, 99999.9999]:
            sql = f'SELECT {float}'
            tokens = list(SQLLexer().tokenize(sql))

            assert tokens[0].type == 'SELECT'
            assert tokens[0].value == 'SELECT'

            assert tokens[1].type == 'FLOAT'
            assert tokens[1].value == float

    def test_select_strings(self):
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

    def test_binary_ops(self):
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

    def test_binary_ops_not(self):
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

    def test_select_from(self):
        sql = f'SELECT column AS other_column FROM db.schema.table'
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
        assert tokens[5].value == 'db.schema.table'

    def test_select_star(self):
        sql = f'SELECT * FROM table'
        tokens = list(SQLLexer().tokenize(sql))

        assert tokens[0].type == 'SELECT'
        assert tokens[0].value == 'SELECT'

        assert tokens[1].type == 'STAR'
        assert tokens[1].value == '*'

        assert tokens[2].type == 'FROM'
        assert tokens[2].value == 'FROM'

        assert tokens[3].type == 'ID'
        assert tokens[3].value == 'table'

    def test_select_where(self):
        sql = f'SELECT column FROM table WHERE column = "something"'
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

    def test_select_group_by(self):
        sql = f'SELECT column, sum(column2) FROM db.schema.table GROUP BY column'
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

    def test_select_order_by(self):
        for order_dir in ['ASC', 'DESC']:
            sql = f'SELECT column, sum(column2) FROM db.schema.table ORDER BY column {order_dir}'
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
