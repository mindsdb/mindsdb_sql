import pytest
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.exceptions import ParsingException


class TestUnion:
    def test_single_select_error(self):
        sql = "SELECT col FROM tab UNION"
        with pytest.raises(ParsingException):
            parse_sql(sql)

    def test_union_base(self):
        for keyword, cls in {'union': Union, 'intersect': Intersect, 'except': Except}.items():
            sql = f"""SELECT col1 FROM tab1 
            {keyword} 
            SELECT col1 FROM tab2"""

            ast = parse_sql(sql)
            expected_ast = cls(unique=True,
                                 left=Select(targets=[Identifier('col1')],
                                             from_table=Identifier(parts=['tab1']),
                                             ),
                                 right=Select(targets=[Identifier('col1')],
                                             from_table=Identifier(parts=['tab2']),
                                             ),
                                 )
            assert ast.to_tree() == expected_ast.to_tree()
            assert str(ast) == str(expected_ast)

    def test_union_all(self):
        for keyword, cls in {'union': Union, 'intersect': Intersect, 'except': Except}.items():
            sql = f"""SELECT col1 FROM tab1 
            {keyword} ALL
            SELECT col1 FROM tab2"""

            ast = parse_sql(sql)
            expected_ast = cls(unique=False,
                                 left=Select(targets=[Identifier('col1')],
                                             from_table=Identifier(parts=['tab1']),
                                             ),
                                 right=Select(targets=[Identifier('col1')],
                                             from_table=Identifier(parts=['tab2']),
                                             ),
                                 )
            assert ast.to_tree() == expected_ast.to_tree()
            assert str(ast) == str(expected_ast)

    def test_union_alias(self):
        sql = """SELECT * FROM (
            SELECT col1 FROM tab1
            UNION 
            SELECT col1 FROM tab2
            UNION 
            SELECT col1 FROM tab3
        ) AS alias"""

        ast = parse_sql(sql)
        expected_ast = Select(targets=[Star()],
                              from_table=Union(
                                   unique=True,
                                   alias=Identifier('alias'),
                                   left=Union(
                                       unique=True,
                                       left=Select(
                                                   targets=[Identifier('col1')],
                                                   from_table=Identifier(parts=['tab1']),),
                                       right=Select(targets=[Identifier('col1')],
                                                    from_table=Identifier(parts=['tab2']),),
                                   ),
                                   right=Select(targets=[Identifier('col1')],
                                                from_table=Identifier(parts=['tab3']),),
                                )
                              )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

