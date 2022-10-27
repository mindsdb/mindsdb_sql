from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *
from mindsdb_sql.parser.utils import to_single_line


class TestCreateMLEngine:
    def test_create_predictor_full(self):
        sql = """
            CREATE ML_ENGINE name FROM ml_handler_name USING a=2, f=3 
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateMLEngine(
            name=Identifier('name'),
            handler='ml_handler_name',
            params={
                'a': 2,
                'f': 3
            }
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()


class TestDropMLEngine:
    def test_create_predictor_full(self):
        sql = """
            DROP ML_ENGINE name
        """
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropMLEngine(
            name=Identifier('name'),
        )
        assert to_single_line(str(ast)) == to_single_line(str(expected_ast))
        assert ast.to_tree() == expected_ast.to_tree()
