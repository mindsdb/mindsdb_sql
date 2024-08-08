import itertools
import pytest
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.parser.utils import JoinType


@pytest.mark.parametrize('dialect', ['sqlite', 'mysql', 'mindsdb'])
class TestSelectStructure:
    def test_no_select(self, dialect):
        query = ""
        with pytest.raises(ParsingException):
            parse_sql(query, dialect=dialect)

    def test_select_number(self, dialect):
        for value in [1, 1.0]:
            sql = f'SELECT {value}'
            ast = parse_sql(sql, dialect=dialect)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Constant)
            assert ast.targets[0].value == value
            assert str(ast).lower() == sql.lower()

    def test_select_string(self, dialect):
        sql = f"SELECT 'string'"
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Constant)
        assert ast.targets[0].value == 'string'
        assert str(ast) == sql

    def test_select_identifier(self, dialect):
        sql = f'SELECT column'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert str(ast.targets[0]) == 'column'
        assert str(ast).lower() == sql.lower()

    def test_select_identifier_with_dashes(self, dialect):
        sql = f'SELECT `column-with-dashes`'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts == ['column-with-dashes']
        assert str(ast.targets[0]) == '`column-with-dashes`'
        assert str(ast).lower() == sql.lower()

    def test_select_identifier_alias(self, dialect):
        sql_queries = ['SELECT column AS column_alias',
                       'SELECT column column_alias']
        for sql in sql_queries:
            ast = parse_sql(sql, dialect=dialect)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Identifier)
            assert ast.targets[0].parts == ['column']
            assert ast.targets[0].alias.parts[0] == 'column_alias'
            assert str(ast).lower().replace('as ', '') == sql.lower().replace('as ', '')



    def test_select_identifier_alias_complex(self, dialect):
        sql = f'SELECT column AS `column alias spaces`'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts == ['column']
        assert ast.targets[0].alias.parts[0] == 'column alias spaces'
        assert str(ast).lower() == sql.lower()

    def test_select_multiple_identifiers(self, dialect):
        sql = f'SELECT column1, column2'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 2
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts[0] == 'column1'
        assert isinstance(ast.targets[1], Identifier)
        assert ast.targets[1].parts[0] == 'column2'
        assert str(ast).lower() == sql.lower()

    def test_select_from_table(self, dialect):
        sql = f'SELECT column FROM tab'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts[0] == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.parts[0] == 'tab'

        assert str(ast).lower() == sql.lower()

    def test_select_from_table_long(self, dialect):
        query = "SELECT 1 FROM integration.database.schema.tab"
        expected_ast = Select(
            targets=[Constant(1)],
            from_table=Identifier(parts=['integration', 'database', 'schema', 'tab'])
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_distinct(self, dialect):
        sql = """SELECT DISTINCT column1 FROM t1"""
        assert str(parse_sql(sql, dialect=dialect)) == sql
        assert parse_sql(sql, dialect=dialect).distinct

    def test_select_multiple_from_table(self, dialect):
        sql = f'SELECT column1, column2, 1 AS renamed_constant FROM tab'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 3
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts[0] == 'column1'
        assert ast.targets[1].parts[0] == 'column2'
        assert ast.targets[2].value == 1
        assert ast.targets[2].alias.parts[0] == 'renamed_constant'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.parts[0] == 'tab'

        assert str(ast).lower() == sql.lower()

    def test_select_from_elaborate(self, dialect):
        query = """SELECT *, column1, column1 AS aliased, column1 + column2 FROM t1"""

        assert str(parse_sql(query)) == query
        assert str(parse_sql(query)) == str(Select(targets=[Star(),
                                                            Identifier(parts=["column1"]),
                                                            Identifier(parts=["column1"], alias=Identifier('aliased')),
                                                            BinaryOperation(op="+",
                                                                            args=(Identifier(parts=['column1']),
                                                                                   Identifier(parts=['column2']))
                                                                            )
                                                            ],
                                                   from_table=Identifier(parts=['t1'])))

    def test_select_from_aliased(self, dialect):
        sql_queries = ["SELECT * FROM t1 AS table_alias", "SELECT * FROM t1 table_alias"]
        expected_ast = Select(targets=[Star()],
                              from_table=Identifier(parts=['t1'], alias=Identifier('table_alias')))
        for query in sql_queries:
            assert parse_sql(query, dialect=dialect) == expected_ast

    def test_from_table_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab FROM tab'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_where(self, dialect):
        sql = f'SELECT column FROM tab WHERE column != 1'
        ast = parse_sql(sql, dialect=dialect)
        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts[0] == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.parts[0] == 'tab'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == '!='

        assert str(ast).lower() == sql.lower()

    def test_select_where_constants(self, dialect):
        sql = f'SELECT column FROM pred WHERE 1 = 0'
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Identifier('column')],
                                       from_table=Identifier('pred'),
                                       where=BinaryOperation(op="=",
                                                             args=[Constant(1), Constant(0)]))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_from_where_elaborate(self, dialect):
        query = """SELECT column1, column2 FROM t1 WHERE column1 = 1"""

        assert str(parse_sql(query)) == query

        assert str(parse_sql(query)) == str(Select(targets=[Identifier(parts=["column1"]), Identifier(parts=["column2"])],
                                                   from_table=Identifier(parts=['t1']),
                                                   where=BinaryOperation(op="=",
                                                                         args=(Identifier(parts=['column1']), Constant(1))
                                                                         )))

        query = """SELECT column1, column2 FROM t1 WHERE column1 = \'1\'"""

        assert str(parse_sql(query)) == query

        assert str(parse_sql(query)) == str(Select(targets=[Identifier(parts=["column1"]), Identifier(parts=["column2"])],
                                                   from_table=Identifier(parts=['t1']),
                                                   where=BinaryOperation(op="=",
                                                                         args=(Identifier(parts=['column1']), Constant("1"))
                                                                         )))

    def test_select_from_where_elaborate_lowercase(self, dialect):
        sql = """select column1, column2 from t1 where column1 = 1"""
        assert str(parse_sql(sql, dialect=dialect)) == str(Select(targets=[Identifier(parts=["column1"]), Identifier(parts=["column2"])],
                                                   from_table=Identifier(parts=['t1']),
                                                   where=BinaryOperation(op="=",
                                                                         args=(Identifier(parts=['column1']), Constant(1))
                                                                         )))


    def test_where_raises_nofrom(self, dialect):
        sql = f'SELECT column WHERE column != 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_where_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab WHERE column != 1 WHERE column > 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_where_raises_as(self, dialect):
        sql = f'SELECT column FROM tab WHERE column != 1 AS somealias'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_where_and(self, dialect):
        sql = f'SELECT column FROM tab WHERE column != 1 and column > 10'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts[0] == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.parts[0] == 'tab'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == 'and'

        assert isinstance(ast.where.args[0], BinaryOperation)
        assert ast.where.args[0].op == '!='
        assert isinstance(ast.where.args[1], BinaryOperation)
        assert ast.where.args[1].op == '>'

    def test_select_where_must_be_an_op(self, dialect):
        sql = f'SELECT column FROM tab WHERE column'

        with pytest.raises(ParsingException) as excinfo:
            ast = parse_sql(sql, dialect=dialect)

        assert "WHERE must contain an operation that evaluates to a boolean" in str(excinfo.value)

    def test_select_group_by(self, dialect):
        sql = f'SELECT column FROM tab WHERE column != 1 GROUP BY column1'
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()

        sql = f'SELECT column FROM tab WHERE column != 1 GROUP BY column1, column2'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].parts[0] == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.parts[0] == 'tab'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == '!='

        assert isinstance(ast.group_by, list)
        assert isinstance(ast.group_by[0], Identifier)
        assert ast.group_by[0].parts[0] == 'column1'
        assert isinstance(ast.group_by[1], Identifier)
        assert ast.group_by[1].parts[0] == 'column2'

        assert str(ast).lower() == sql.lower()

    def test_select_group_by_elaborate(self, dialect):
        query = """SELECT column1, column2, sum(column3) AS total FROM t1 GROUP BY column1, column2"""

        assert str(parse_sql(query)) == query

        assert str(parse_sql(query)) == str(Select(targets=[Identifier(parts=["column1"]),
                                                            Identifier(parts=["column2"]),
                                                            Function(op="sum",
                                                                         args=[Identifier(parts=["column3"])],
                                                                         alias=Identifier('total'))],
                                                   from_table=Identifier(parts=['t1']),
                                                   group_by=[Identifier(parts=["column1"]), Identifier(parts=["column2"])]))

    def test_group_by_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab GROUP BY col GROUP BY col'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_having(self, dialect):
        sql = f'SELECT column FROM tab WHERE column != 1 GROUP BY column1'
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()

        sql = f'SELECT column FROM tab WHERE column != 1 GROUP BY column1, column2 HAVING column1 > 10'
        ast = parse_sql(sql, dialect=dialect)

        assert isinstance(ast, Select)

        assert isinstance(ast.having, BinaryOperation)
        assert isinstance(ast.having.args[0], Identifier)
        assert ast.having.args[0].parts[0] == 'column1'
        assert ast.having.args[1].value == 10

        assert str(ast).lower() == sql.lower()

    def test_select_group_by_having_elaborate(self, dialect):
        sql = """SELECT column1 FROM t1 GROUP BY column1 HAVING column1 != 1"""
        assert str(parse_sql(sql, dialect=dialect)) == sql

    def test_select_order_by_elaborate(self, dialect):
        sql = """SELECT * FROM t1 ORDER BY column1 ASC, column2, column3 DESC NULLS FIRST"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Star()],
                               from_table=Identifier(parts=['t1']),
                               order_by=[
                                   OrderBy(Identifier(parts=['column1']), direction='ASC'),
                                   OrderBy(Identifier(parts=['column2'])),
                                   OrderBy(Identifier(parts=['column3']), direction='DESC',
                                           nulls='NULLS FIRST')],
                               )

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_aliases_order_by(self, dialect):
        sql = "select max(name) as `max(name)` from tbl order by `max(name)`"

        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Function('max', args=[Identifier('name')], alias=Identifier('max(name)'))],
                              from_table=Identifier('tbl'),
                              order_by=[OrderBy(Identifier('max(name)'))])

        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_limit_offset_elaborate(self, dialect):
        sql = """SELECT * FROM t1 LIMIT 1 OFFSET 2"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Star()],
                                                   from_table=Identifier(parts=['t1']),
                                                   limit=Constant(1),
                                                   offset=Constant(2))

        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_limit_two_arguments(self, dialect):
        sql = """SELECT * FROM t1 LIMIT 2, 1"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Star()],
                                                   from_table=Identifier(parts=['t1']),
                                                   limit=Constant(1),
                                                   offset=Constant(2))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_limit_two_arguments_and_offset_error(self, dialect):
        sql = """SELECT * FROM t1 LIMIT 2, 1 OFFSET 2"""
        with pytest.raises(ParsingException):
            parse_sql(sql, dialect=dialect)

    def test_having_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab GROUP BY col HAVING col > 1 HAVING col > 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_order_by(self, dialect):
        sql = f'SELECT column1 FROM tab ORDER BY column2'
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()

        assert len(ast.order_by) == 1
        assert isinstance(ast.order_by[0], OrderBy)
        assert isinstance(ast.order_by[0].field, Identifier)
        assert ast.order_by[0].field.parts[0] == 'column2'
        assert ast.order_by[0].direction == 'default'

        sql = f'SELECT column1 FROM tab ORDER BY column2, column3 ASC, column4 DESC'
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()

        assert len(ast.order_by) == 3

        assert isinstance(ast.order_by[0], OrderBy)
        assert isinstance(ast.order_by[0].field, Identifier)
        assert ast.order_by[0].field.parts[0] == 'column2'
        assert ast.order_by[0].direction == 'default'

        assert isinstance(ast.order_by[1], OrderBy)
        assert isinstance(ast.order_by[1].field, Identifier)
        assert ast.order_by[1].field.parts[0] == 'column3'
        assert ast.order_by[1].direction == 'ASC'

        assert isinstance(ast.order_by[2], OrderBy)
        assert isinstance(ast.order_by[2].field, Identifier)
        assert ast.order_by[2].field.parts[0] == 'column4'
        assert ast.order_by[2].direction == 'DESC'

    def test_order_by_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab ORDER BY col1 ORDER BY col1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_limit_offset(self, dialect):
        sql = f'SELECT column FROM tab LIMIT 5 OFFSET 3'
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()

        assert ast.limit == Constant(value=5)
        assert ast.offset == Constant(value=3)

    def test_select_limit_offset_raises_nonint(self, dialect):
        sql = f'SELECT column FROM tab OFFSET 3.0'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

        sql = "SELECT column FROM tab LIMIT \'string\'"
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_limit_offset_raises_wrong_order(self, dialect):
        sql = f'SELECT column FROM tab OFFSET 3 LIMIT 5 '
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_limit_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab LIMIT 1 LIMIT 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_offset_raises_duplicate(self, dialect):
        sql = f'SELECT column FROM tab OFFSET 1 OFFSET 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_limit_raises_before_order_by(self, dialect):
        sql = f'SELECT column FROM tab LIMIT 1 ORDER BY column ASC'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_offset_raises_before_order_by(self, dialect):
        sql = f'SELECT column FROM tab OFFSET 1 ORDER BY column ASC'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql, dialect=dialect)

    def test_select_order(self, dialect):
        components = ['FROM tab',
                      'WHERE column = 1',
                      'GROUP BY column',
                      'HAVING column != 2',
                      'ORDER BY column ASC',
                      'LIMIT 1',
                      'OFFSET 1']

        good_sql = 'SELECT column ' + '\n'.join(components)
        ast = parse_sql(good_sql, dialect)
        assert ast

        for perm in itertools.permutations(components):
            bad_sql = 'SELECT column ' + '\n'.join(perm)
            if bad_sql == good_sql:
                continue

            with pytest.raises(ParsingException) as excinfo:
                ast = parse_sql(bad_sql, dialect)
            if dialect == 'sqlite':
                # TODO fix message for mindsdb dialect
                assert 'must go before' in str(excinfo.value) or ' requires ' in str(excinfo.value)

    def test_select_from_inner_join(self, dialect):
        sql = """SELECT * FROM t1 INNER JOIN t2 ON t1.x1 = t2.x2 and t1.x2 = t2.x2"""

        expected_ast = Select(targets=[Star()],
                              from_table=Join(join_type=JoinType.INNER_JOIN,
                                              left=Identifier(parts=['t1']),
                                              right=Identifier(parts=['t2']),
                                              condition=
                                              BinaryOperation(op='and',
                                                              args=[
                                                                  BinaryOperation(op='=',
                                                                                  args=(
                                                                                      Identifier(
                                                                                          parts=['t1','x1']),
                                                                                      Identifier(
                                                                                          parts=['t2','x2']))),
                                                                  BinaryOperation(op='=',
                                                                                  args=(
                                                                                      Identifier(
                                                                                          parts=['t1','x2']),
                                                                                      Identifier(
                                                                                          parts=['t2','x2']))),
                                                              ])

                                              ))
        ast = parse_sql(sql, dialect=dialect)

        assert ast == expected_ast

    def test_select_from_implicit_join(self, dialect):
        sql = """SELECT * FROM t1, t2"""

        expected_ast = Select(targets=[Star()],
                                                   from_table=Join(left=Identifier(parts=['t1']),
                                                                   right=Identifier(parts=['t2']),
                                                                   join_type=JoinType.INNER_JOIN,
                                                                   implicit=True,
                                                                   condition=None))
        ast = parse_sql(sql, dialect=dialect)
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_from_different_join_types(self, dialect):
        join_types = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']
        for join in join_types:
            sql = f"""SELECT * FROM t1 {join} t2 ON t1.x1 = t2.x2"""
            expected_ast = Select(targets=[Star()],
                                  from_table=Join(join_type=join,
                                                  left=Identifier(parts=['t1']),
                                                  right=Identifier(parts=['t2']),
                                                  condition=
                                                  BinaryOperation(op='=',
                                                                  args=(
                                                                      Identifier(
                                                                          parts=['t1','x1']),
                                                                      Identifier(
                                                                          parts=['t2','x2']))),

                                                  ))

            ast = parse_sql(sql, dialect=dialect)
            assert ast == expected_ast

    def test_select_from_subquery(self, dialect):
        sql = f"""SELECT * FROM (SELECT column1 FROM t1) AS sub"""
        expected_ast = Select(targets=[Star()],
                                                   from_table=Select(targets=[Identifier(parts=['column1'])],
                                                              from_table=Identifier(parts=['t1']),
                                                              alias=Identifier('sub'),
                                                              parentheses=True))
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert ast == expected_ast

        sql = f"""SELECT * FROM (SELECT column1 FROM t1)"""
        expected_ast = Select(targets=[Star()],
                              from_table=Select(targets=[Identifier(parts=['column1'])],
                                                from_table=Identifier(parts=['t1']),
                                                parentheses=True))
        ast = parse_sql(sql, dialect=dialect)
        assert str(ast).lower() == sql.lower()
        assert ast == expected_ast

    def test_select_subquery_target(self, dialect):
        sql = f"""SELECT *, (SELECT 1) FROM t1"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Star(), Select(targets=[Constant(1)], parentheses=True)],
                              from_table=Identifier(parts=['t1']))
        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT *, (SELECT 1) AS ones FROM t1"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Star(), Select(targets=[Constant(1)], alias=Identifier('ones'), parentheses=True)],
                              from_table=Identifier(parts=['t1']))
        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_subquery_where(self, dialect):
        sql = f"""SELECT * FROM tab1 WHERE column1 in (SELECT column2 FROM t2)"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Star()],
                              from_table=Identifier(parts=['tab1']),
                              where=BinaryOperation(op='in',
                                                    args=(
                                                        Identifier(parts=['column1']),
                                                        Select(targets=[Identifier(parts=['column2'])],
                                                               from_table=Identifier(parts=['t2']),
                                                               parentheses=True)
                                                    )))
        assert str(ast).lower() == sql.lower()
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_type_cast(self, dialect):
        sql = f"""SELECT CAST(4 AS int64) AS result"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[TypeCast(type_name='int64', arg=Constant(4), alias=Identifier('result'))])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT CAST(column1 AS float) AS result"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[TypeCast(type_name='float', arg=Identifier(parts=['column1']), alias=Identifier('result'))])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT CAST((column1 + column2) AS float) AS result"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[TypeCast(type_name='float', arg=BinaryOperation(op='+', parentheses=True, args=[
            Identifier(parts=['column1']), Identifier(parts=['column2'])]), alias=Identifier('result'))])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT CAST(a AS CHAR(10))"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[
            TypeCast(type_name='CHAR', arg=Identifier('a'), length=10)
        ])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_in_tuple(self, dialect):
        sql = "SELECT col FROM tab WHERE col in (1, 2)"
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[Identifier(parts=['col'])],
                              from_table=Identifier(parts=['tab']),
                              where=BinaryOperation(op='in',
                                                    args=(
                                                        Identifier(parts=['col']),
                                                        Tuple(items=[Constant(1), Constant(2)])
                                                    )))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_count_distinct(self, dialect):
        sql = "SELECT COUNT(DISTINCT survived) AS uniq_survived FROM titanic"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Function(op='COUNT', distinct=True,
                              args=(Identifier(parts=['survived']),), alias=Identifier('uniq_survived'))],
            from_table=Identifier(parts=['titanic'])
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_where_not_order(self, dialect):
        sql = "SELECT col1 FROM tab WHERE NOT col1 = \'FAMILY\'"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier(parts=['col1'])],
                              from_table=Identifier(parts=['tab']),
                              where=UnaryOperation(op='NOT',
                                   args=(
                                      BinaryOperation(op='=',
                                                      args=(Identifier(parts=['col1']), Constant('FAMILY'))),
                                   )
                              )
                          )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_backticks(self, dialect):
        sql = "SELECT `name`, `status` FROM `mindsdb`.`wow stuff predictors`.`even-dashes-work`.`nice`"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier(parts=['name']), Identifier(parts=['status'])],
                              from_table=Identifier(parts=['mindsdb', 'wow stuff predictors', 'even-dashes-work', 'nice']),
                              )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_partial_backticks(self, dialect):
        sql = "SELECT `integration`.`some table`.column"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier(parts=['integration', 'some table', 'column']),],)

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_backticks_in_str(self, dialect):
        sql = "SELECT `my column name` FROM tab WHERE `other column name` = 'bla bla ``` bla'"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier(parts=['my column name'])],
                              from_table=Identifier(parts=['tab']),
                              where=BinaryOperation(op='=', args=(
                                      Identifier(parts=['other column name']),
                                      Constant('bla bla ``` bla')
                                  )
                              ))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_parameter(self, dialect):
        sql = "SELECT ? = ? FROM ?"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[BinaryOperation(op='=', args=(Parameter('?'), Parameter('?')))],
                              from_table=Parameter('?'),
                              )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_from_tables(self, dialect):
        sql = "SELECT * FROM tables"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Star()],
                              from_table=Identifier('tables'))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_tricky_tables_case(self, dialect):
        sql = "SELECT TABLES.table_name AS Tables_in_mindsdb FROM TABLES WHERE TABLES.table_schema = 'MINDSDB' AND TABLES.table_type = 'BASE TABLE'"
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(targets=[Identifier('TABLES.table_name', alias=Identifier('Tables_in_mindsdb'))],
                              from_table=Identifier('TABLES'),
                              where=BinaryOperation('and', args=[
                                  BinaryOperation('=', args=[Identifier('TABLES.table_schema'), Constant('MINDSDB')]),
                                  BinaryOperation('=', args=[Identifier('TABLES.table_type'), Constant('BASE TABLE')]),
                              ]))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_double_aliased_table(self, dialect):
        sql = "select * from table1 zzzzz alias1"

        with pytest.raises(ParsingException):
            parse_sql(sql, dialect=dialect)

    def test_window_function(self, dialect):
        query = "select SUM(col0) OVER (PARTITION BY col1 order by col2) as al from table1 "
        expected_ast = Select(
            targets=[
                WindowFunction(
                    function=Function(op='sum', args=[Identifier('col0')]),
                    partition=[Identifier('col1')],
                    order_by=[OrderBy(field=Identifier('col2'))],
                    alias=Identifier('al')
                )
            ],
            from_table=Identifier('table1')
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # no partition
        query = "select SUM(col0) OVER (order by col2) from table1 "
        expected_ast = Select(
            targets=[
                WindowFunction(
                    function=Function(op='sum', args=[Identifier('col0')]),
                    order_by=[OrderBy(field=Identifier('col2'))],
                )
            ],
            from_table=Identifier('table1')
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # no order by
        query = "select SUM(col0) OVER (PARTITION BY col1) from table1 "
        expected_ast = Select(
            targets=[
                WindowFunction(
                    function=Function(op='sum', args=[Identifier('col0')]),
                    partition=[Identifier('col1')],
                )
            ],
            from_table=Identifier('table1')
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # just over()
        query = "select SUM(col0) OVER () from table1 "
        expected_ast = Select(
            targets=[
                WindowFunction(
                    function=Function(op='sum', args=[Identifier('col0')]),
                )
            ],
            from_table=Identifier('table1')
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_is_not_precedence(self, dialect):
        query = "select * from t1 where a is not null and b = c"
        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier('t1'),
            where=BinaryOperation(op='and', args=[
                BinaryOperation(op='is not', args=[
                    Identifier('a'),
                    NullConstant()
                ]),
                BinaryOperation(op='=', args=[
                    Identifier('b'),
                    Identifier('c')
                ]),
            ])
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


@pytest.mark.parametrize('dialect', ['mysql', 'mindsdb'])
class TestSelectStructureNoSqlite:
    def test_select_from_plugins(self, dialect):
        query = "select * from information_schema.plugins"
        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier(parts=['information_schema', 'plugins'])
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        query = "select * from plugins"
        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier(parts=['plugins'])
        )
        ast = parse_sql(query, dialect=dialect)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()


    def test_type_convert(self, dialect):
        sql = f"""SELECT CONVERT(column1, float)"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[TypeCast(type_name='float', arg=Identifier(parts=['column1']))])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT CONVERT((column1 + column2) USING float)"""
        ast = parse_sql(sql, dialect=dialect)
        expected_ast = Select(targets=[TypeCast(type_name='float', arg=BinaryOperation(op='+', parentheses=True, args=[
            Identifier(parts=['column1']), Identifier(parts=['column2'])]))])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_for_update(self, dialect):
        sql = f'SELECT * FROM tab for update'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier(parts=['tab']),
            mode='FOR UPDATE'
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_keywords(self, dialect):
        sql = f'SELECT COLLATION_NAME AS Collation, CHARACTER_SET_NAME AS Charset,\
                       ID AS Id, IS_COMPILED AS Compiled, PLUGINS,  MASTER, STATUS, ONLY\
                FROM INFORMATION_SCHEMA.COLLATIONS'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[
                Identifier('COLLATION_NAME', alias=Identifier('Collation')),
                Identifier('CHARACTER_SET_NAME', alias=Identifier('Charset')),
                Identifier('ID', alias=Identifier('Id')),
                Identifier('IS_COMPILED', alias=Identifier('Compiled')),
                Identifier('PLUGINS'),
                Identifier('MASTER'),
                Identifier('STATUS'),
                Identifier('ONLY'),
            ],
            from_table=Identifier('INFORMATION_SCHEMA.COLLATIONS')
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_table_star(self, dialect):
        sql = f'select *, t.* From table1 '
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[
                Star(),
                Identifier(parts=['t', Star()]),
            ],
            from_table=Identifier('table1')
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_from_select(self, dialect):
        sql = f'select * from (select a from tab1) as sub '
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[
                Star(),
            ],
            from_table=Select(
                alias=Identifier('sub'),
                parentheses=True,
                targets=[
                    Identifier('a')
                ],
                from_table=Identifier('tab1')
            )
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_function_star(self, dialect):
        sql = f'select count(*) from tab1'
        ast = parse_sql(sql, dialect=dialect)

        expected_ast = Select(
            targets=[
                Function(op='count', args=[
                    Star()
                ])
            ],
            from_table=Identifier('tab1')
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

class TestMindsdb:

    def test_case(self):
        sql = f'''SELECT
                    CASE
                        WHEN R.DELETE_RULE = 'CASCADE' THEN 0
                        WHEN R.DELETE_RULE = 'SET NULL' THEN 2
                        ELSE 3
                    END AS DELETE_RULE,
                    sum(
                      CASE
                        WHEN 1 = 1 THEN 1
                        ELSE 0
                      END
                    )
                   FROM INFORMATION_SCHEMA.COLLATIONS'''
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[
                Case(
                    rules=[
                        [
                            BinaryOperation(op='=', args=[
                                Identifier('R.DELETE_RULE'),
                                Constant('CASCADE')
                            ]),
                            Constant(0)
                        ],
                        [
                            BinaryOperation(op='=', args=[
                                Identifier('R.DELETE_RULE'),
                                Constant('SET NULL')
                            ]),
                            Constant(2)
                        ]
                    ],
                    default=Constant(3),
                    alias=Identifier('DELETE_RULE')
                ),
                Function(
                    op='sum',
                    args=[
                        Case(
                            rules=[
                                [
                                    BinaryOperation(op='=', args=[Constant(1), Constant(1)]),
                                    Constant(1)
                                ],
                            ],
                            default=Constant(0)
                        )
                    ]
                )
            ],
            from_table=Identifier('INFORMATION_SCHEMA.COLLATIONS')
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_left(self):
        sql = f'select left(a, 1) from tab1'
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[
                Function(op='left', args=[
                    Identifier('a'),
                    Constant(1)
                ])
            ],
            from_table=Identifier('tab1')
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_not_in_precedence(self):
        query = "select * from t1 where a = 1 and b not in (1, 2)"
        expected_ast = Select(
            targets=[Star()],
            from_table=Identifier('t1'),
            where=BinaryOperation(op='and', args=[
                BinaryOperation(op='=', args=[
                    Identifier('a'),
                    Constant(1)
                ]),
                BinaryOperation(op='not in', args=[
                    Identifier('b'),
                    Tuple([Constant(1), Constant(2)])
                ]),
            ])
        )
        ast = parse_sql(query)
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_select_from_subquery_with_columns(self):
        sql = f"""
            SELECT * FROM 
              (SELECT col1, col2 b
               FROM t1) 
              AS sub (c1, c2)
            """
        expected_ast = Select(
            targets=[Star()],
            from_table=Select(
                targets=[
                    Identifier(parts=['col1'], alias=Identifier('c1')),
                    Identifier(parts=['col2'], alias=Identifier('c2'))
                ],
                from_table=Identifier(parts=['t1']),
                alias=Identifier('sub'),
                parentheses=True
            )
        )
        ast = parse_sql(sql)
        assert str(ast) == str(expected_ast)

    def test_substring(self):
        expected_ast = Select(
            targets=[Function(
                op='substring',
                args=[Identifier('phone'), Constant(1), Constant(2)]
            )],
        )

        for sql in (
                'SELECT substring(phone from 1 for 2)',
                'SELECT substring(phone, 1, 2)'
            ):

            ast = parse_sql(sql)
            assert str(ast) == str(expected_ast)

    def test_alternative_casting(self):

        # int
        expected_ast = Select(targets=[
            TypeCast(type_name='int64', arg=Constant('1998')),
            TypeCast(type_name='int64', arg=Identifier('col1'), alias=Identifier('col2'))
        ])

        sql = f"SELECT '1998'::int64, col1::int64 col2"
        ast = parse_sql(sql)
        assert str(ast) == str(expected_ast)

        # date
        expected_ast = Select(targets=[
            TypeCast(type_name='date', arg=Constant('1998-12-01')),
            TypeCast(type_name='date', arg=Identifier('col1'), alias=Identifier('col2'))
        ])

        sql = f"SELECT '1998-12-01'::date, col1::date col2"
        ast = parse_sql(sql)
        assert str(ast) == str(expected_ast)

        #
        expected_ast = Select(targets=[
            TypeCast(type_name='date', arg=Constant('1998-12-01')),
        ])

        sql = f"SELECT date '1998-12-01'"
        ast = parse_sql(sql)
        assert str(ast) == str(expected_ast)

