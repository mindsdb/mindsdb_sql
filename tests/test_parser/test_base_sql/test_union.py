import itertools
import pytest
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.utils import JoinType


@pytest.mark.parametrize('dialect', ['sqlite', ])# 'mysql', 'mindsdb'])
class TestUnion:
    def test_single_select_error(self, dialect):
        sql = "SELECT col FROM tab UNION"
        with pytest.raises(ParsingException):
            parse_sql(sql, dialect=dialect)

    def test_union(self, dialect):
        sql = """SELECT col1 FROM tab1 
        UNION 
        SELECT col1 FROM tab2"""

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Union(unique=True,
                             left=Select(targets=[Identifier('col1')],
                                         from_table=Identifier(parts=['tab1']),
                                         ),
                             right=Select(targets=[Identifier('col1')],
                                         from_table=Identifier(parts=['tab2']),
                                         ),
                             )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_union_aliased_selects(self, dialect):
        sql = """(SELECT col1 FROM tab1) AS alias1
        UNION 
        (SELECT col1 FROM tab2) AS alias2"""

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Union(unique=True,
                             left=Select(alias='alias1',
                                         targets=[Identifier('col1')],
                                         from_table=Identifier(parts=['tab1']),
                                         ),
                             right=Select(alias='alias2', targets=[Identifier('col1')],
                                          from_table=Identifier(parts=['tab2']),
                                          ),
                             )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_union_all(self, dialect):
        sql = """SELECT col1 FROM tab1 
        UNION ALL
        SELECT col1 FROM tab2"""

        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Union(unique=False,
                             left=Select(targets=[Identifier('col1')],
                                         from_table=Identifier(parts=['tab1']),
                                         ),
                             right=Select(targets=[Identifier('col1')],
                                         from_table=Identifier(parts=['tab2']),
                                         ),
                             )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

