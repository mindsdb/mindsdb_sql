import itertools

import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.ast import Identifier, Constant, Select, BinaryOperation, UnaryOperation, TypeCast
from mindsdb_sql.ast.join import Join
from mindsdb_sql.ast.operation import Function
from mindsdb_sql.ast.order_by import OrderBy
from mindsdb_sql.ast.tuple import Tuple
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.lexer import SQLLexer
from mindsdb_sql.parser import SQLParser


class TestSelectStructure:
    def test_no_select(self):
        query = ""
        with pytest.raises(ParsingException):
            parse_sql(query)

    def test_select_constant(self):
        for value in [1, 1.0, 'string']:
            sql = f'SELECT {value}' if not isinstance(value, str) else f"SELECT \"{value}\""
            ast = parse_sql(sql)

            assert isinstance(ast, Select)
            assert len(ast.targets) == 1
            assert isinstance(ast.targets[0], Constant)
            assert ast.targets[0].value == value
            assert str(ast) == sql

    def test_select_identifier(self):
        sql = f'SELECT column'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'
        assert str(ast) == sql


    def test_select_identifier_alias(self):
        sql = f'SELECT column AS column_alias'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'
        assert ast.targets[0].alias == 'column_alias'
        assert str(ast) == sql

    def test_select_multiple_identifiers(self):
        sql = f'SELECT column1, column2'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 2
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column1'
        assert isinstance(ast.targets[1], Identifier)
        assert ast.targets[1].value == 'column2'
        assert str(ast) == sql

    def test_select_from_table(self):
        sql = f'SELECT column FROM table'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert str(ast) == sql

    def test_select_from_table_long(self):
        query = "SELECT 1 FROM database.schema.table"

        assert str(parse_sql(query)) == str(Select(
            targets=[Constant(1)],
            from_table=Identifier('database.schema.table')
        ))

    def test_select_distinct(self):
        sql = """SELECT DISTINCT column1 FROM t1"""
        assert str(parse_sql(sql)) == sql
        assert parse_sql(sql).distinct

    def test_select_multiple_from_table(self):
        sql = f'SELECT column1, column2, 1 AS renamed_constant FROM table'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 3
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column1'
        assert ast.targets[1].value == 'column2'
        assert ast.targets[2].value == 1
        assert ast.targets[2].alias == 'renamed_constant'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert str(ast) == sql

    def test_select_from_elaborate(self):
        query = """SELECT *, column1, column1 AS aliased, column1 + column2 FROM t1"""

        assert str(parse_sql(query)) == query
        assert str(parse_sql(query)) == str(Select(targets=[Identifier("*"),
                                                            Identifier("column1"),
                                                            Identifier("column1", alias='aliased'),
                                                            BinaryOperation(op="+",
                                                                            args=(Identifier('column1'),
                                                                                   Identifier('column2'))
                                                                            )
                                                            ],
                                                   from_table=Identifier('t1')))

    def test_from_table_raises_duplicate(self):
        sql = f'SELECT column FROM table FROM table'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_where(self):
        sql = f'SELECT column FROM table WHERE column != 1'
        ast = parse_sql(sql)
        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == '!='

        assert str(ast) == sql

    def test_select_from_where_elaborate(self):
        query = """SELECT column1, column2 FROM t1 WHERE column1 = 1"""

        assert str(parse_sql(query)) == query

        assert str(parse_sql(query)) == str(Select(targets=[Identifier("column1"), Identifier("column2")],
                                                   from_table=Identifier('t1'),
                                                   where=BinaryOperation(op="=",
                                                                         args=(Identifier('column1'), Constant(1))
                                                                         )))

        query = """SELECT column1, column2 FROM t1 WHERE column1 = \"1\""""

        assert str(parse_sql(query)) == query

        assert str(parse_sql(query)) == str(Select(targets=[Identifier("column1"), Identifier("column2")],
                                                   from_table=Identifier('t1'),
                                                   where=BinaryOperation(op="=",
                                                                         args=(Identifier('column1'), Constant("1"))
                                                                         )))

    def test_select_from_where_elaborate_lowercase(self):
        sql = """select column1, column2 from t1 where column1 = 1"""
        assert str(parse_sql(sql)) == str(Select(targets=[Identifier("column1"), Identifier("column2")],
                                                   from_table=Identifier('t1'),
                                                   where=BinaryOperation(op="=",
                                                                         args=(Identifier('column1'), Constant(1))
                                                                         )))


    def test_where_raises_nofrom(self):
        sql = f'SELECT column WHERE column != 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_where_raises_duplicate(self):
        sql = f'SELECT column FROM table WHERE column != 1 WHERE column > 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_where_raises_as(self):
        sql = f'SELECT column FROM table WHERE column != 1 AS somealias'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_where_and(self):
        sql = f'SELECT column FROM table WHERE column != 1 AND column > 10'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == 'AND'

        assert isinstance(ast.where.args[0], BinaryOperation)
        assert ast.where.args[0].op == '!='
        assert isinstance(ast.where.args[1], BinaryOperation)
        assert ast.where.args[1].op == '>'
        assert str(ast) == sql

    def test_select_where_must_be_an_op(self):
        sql = f'SELECT column FROM table WHERE column'

        with pytest.raises(ParsingException) as excinfo:
            ast = parse_sql(sql)

        assert "WHERE must contain an operation that evaluates to a boolean" in str(excinfo.value)

    def test_select_group_by(self):
        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1'
        ast = parse_sql(sql)
        assert str(ast) == sql

        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1, column2'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)
        assert len(ast.targets) == 1
        assert isinstance(ast.targets[0], Identifier)
        assert ast.targets[0].value == 'column'

        assert isinstance(ast.from_table, Identifier)
        assert ast.from_table.value == 'table'

        assert isinstance(ast.where, BinaryOperation)
        assert ast.where.op == '!='

        assert isinstance(ast.group_by, list)
        assert isinstance(ast.group_by[0], Identifier)
        assert ast.group_by[0].value == 'column1'
        assert isinstance(ast.group_by[1], Identifier)
        assert ast.group_by[1].value == 'column2'

        assert str(ast) == sql

    def test_select_group_by_elaborate(self):
        query = """SELECT column1, column2, sum(column3) AS total FROM t1 GROUP BY column1, column2"""

        assert str(parse_sql(query)) == query

        assert str(parse_sql(query)) == str(Select(targets=[Identifier("column1"),
                                                            Identifier("column2"),
                                                            Function(op="sum",
                                                                         args=(Identifier("column3"),),
                                                                         alias='total')],
                                                   from_table=Identifier('t1'),
                                                   group_by=[Identifier("column1"), Identifier("column2")]))

    def test_group_by_raises_duplicate(self):
        sql = f'SELECT column FROM table GROUP BY col GROUP BY col'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_having(self):
        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1'
        ast = parse_sql(sql)
        assert str(ast) == sql

        sql = f'SELECT column FROM table WHERE column != 1 GROUP BY column1, column2 HAVING column1 > 10'
        ast = parse_sql(sql)

        assert isinstance(ast, Select)

        assert isinstance(ast.having, BinaryOperation)
        assert isinstance(ast.having.args[0], Identifier)
        assert ast.having.args[0].value == 'column1'
        assert ast.having.args[1].value == 10

        assert str(ast) == sql

    def test_select_group_by_having_elaborate(self):
        sql = """SELECT column1 FROM t1 GROUP BY column1 HAVING column1 != 1"""
        assert str(parse_sql(sql)) == sql

    def test_select_order_by_elaborate(self):
        sql = """SELECT * FROM t1 ORDER BY column1 ASC, column2, column3 DESC NULLS FIRST"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Identifier("*")],
                                                   from_table=Identifier('t1'),
                                                   order_by=[
                                                       OrderBy(Identifier('column1'), direction='ASC'),
                                                       OrderBy(Identifier('column2')),
                                                       OrderBy(Identifier('column3'), direction='DESC',
                                                               nulls='NULLS FIRST')],
                                                   )


        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_limit_offset_elaborate(self):
        sql = """SELECT * FROM t1 LIMIT 1 OFFSET 2"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Identifier("*")],
                                                   from_table=Identifier('t1'),
                                                   limit=Constant(1),
                                                   offset=Constant(2))

        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_having_raises_duplicate(self):
        sql = f'SELECT column FROM table GROUP BY col HAVING col > 1 HAVING col > 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_order_by(self):
        sql = f'SELECT column1 FROM table ORDER BY column2'
        ast = parse_sql(sql)
        assert str(ast) == sql

        assert len(ast.order_by) == 1
        assert isinstance(ast.order_by[0], OrderBy)
        assert isinstance(ast.order_by[0].field, Identifier)
        assert ast.order_by[0].field.value == 'column2'
        assert ast.order_by[0].direction == 'default'

        sql = f'SELECT column1 FROM table ORDER BY column2, column3 ASC, column4 DESC'
        ast = parse_sql(sql)
        assert str(ast) == sql

        assert len(ast.order_by) == 3

        assert isinstance(ast.order_by[0], OrderBy)
        assert isinstance(ast.order_by[0].field, Identifier)
        assert ast.order_by[0].field.value == 'column2'
        assert ast.order_by[0].direction == 'default'

        assert isinstance(ast.order_by[1], OrderBy)
        assert isinstance(ast.order_by[1].field, Identifier)
        assert ast.order_by[1].field.value == 'column3'
        assert ast.order_by[1].direction == 'ASC'

        assert isinstance(ast.order_by[2], OrderBy)
        assert isinstance(ast.order_by[2].field, Identifier)
        assert ast.order_by[2].field.value == 'column4'
        assert ast.order_by[2].direction == 'DESC'

    def test_order_by_raises_duplicate(self):
        sql = f'SELECT column FROM table ORDER BY col1 ORDER BY col1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_limit_offset(self):
        sql = f'SELECT column FROM table LIMIT 5 OFFSET 3'
        ast = parse_sql(sql)
        assert str(ast) == sql

        assert ast.limit == Constant(value=5)
        assert ast.offset == Constant(value=3)

    def test_select_limit_offset_raises_nonint(self):
        sql = f'SELECT column FROM table OFFSET 3.0'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

        sql = "SELECT column FROM table LIMIT \"string\""
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_limit_offset_raises_wrong_order(self):
        sql = f'SELECT column FROM table OFFSET 3 LIMIT 5 '
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_limit_raises_duplicate(self):
        sql = f'SELECT column FROM table LIMIT 1 LIMIT 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_offset_raises_duplicate(self):
        sql = f'SELECT column FROM table OFFSET 1 OFFSET 1'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_limit_raises_before_order_by(self):
        sql = f'SELECT column FROM table LIMIT 1 ORDER BY column ASC'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_offset_raises_before_order_by(self):
        sql = f'SELECT column FROM table OFFSET 1 ORDER BY column ASC'
        with pytest.raises(ParsingException):
            ast = parse_sql(sql)

    def test_select_order(self):
        components = ['FROM table',
                      'WHERE column = 1',
                      'GROUP BY column',
                      'HAVING column != 2',
                      'ORDER BY column ASC',
                      'LIMIT 1',
                      'OFFSET 1']

        good_sql = 'SELECT column ' + '\n'.join(components)
        ast = parse_sql(good_sql)
        assert ast

        for perm in itertools.permutations(components):
            bad_sql = 'SELECT column ' + '\n'.join(perm)
            if bad_sql == good_sql:
                continue

            with pytest.raises(ParsingException) as excinfo:
                ast = parse_sql(bad_sql)
            assert 'must go after' in str(excinfo.value) or ' requires ' in str(excinfo.value)

    def test_select_from_inner_join(self):
        sql = """SELECT * FROM t1 INNER JOIN t2 ON t1.x1 = t2.x2 AND t1.x2 = t2.x2"""

        expected_ast = Select(targets=[Identifier("*")],
                              from_table=Join(join_type='INNER JOIN',
                                              left=Identifier('t1'),
                                              right=Identifier('t2'),
                                              condition=
                                              BinaryOperation(op='AND',
                                                              args=[
                                                                  BinaryOperation(op='=',
                                                                                  args=(
                                                                                      Identifier(
                                                                                          't1.x1'),
                                                                                      Identifier(
                                                                                          't2.x2'))),
                                                                  BinaryOperation(op='=',
                                                                                  args=(
                                                                                      Identifier(
                                                                                          't1.x2'),
                                                                                      Identifier(
                                                                                          't2.x2'))),
                                                              ])

                                              ))
        ast = parse_sql(sql)

        assert ast == expected_ast

    def test_select_from_implicit_join(self):
        sql = """SELECT * FROM t1, t2"""

        expected_ast = Select(targets=[Identifier("*")],
                                                   from_table=Join(left=Identifier('t1'),
                                                                   right=Identifier('t2'),
                                                                   join_type='INNER JOIN',
                                                                   implicit=True,
                                                                   condition=None))
        ast = parse_sql(sql)
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_from_different_join_types(self):
        join_types = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']
        for join in join_types:
            sql = f"""SELECT * FROM t1 {join} t2 ON t1.x1 = t2.x2"""
            expected_ast = Select(targets=[Identifier("*")],
                                  from_table=Join(join_type=join,
                                                  left=Identifier('t1'),
                                                  right=Identifier('t2'),
                                                  condition=
                                                  BinaryOperation(op='=',
                                                                  args=(
                                                                      Identifier(
                                                                          't1.x1'),
                                                                      Identifier(
                                                                          't2.x2'))),

                                                  ))

            ast = parse_sql(sql)
            assert ast == expected_ast

    def test_select_from_subquery(self):
        sql = f"""SELECT * FROM (SELECT column1 FROM t1) AS sub"""
        expected_ast = Select(targets=[Identifier("*")],
                                                   from_table=Select(targets=[Identifier('column1')],
                                                              from_table=Identifier('t1'),
                                                              alias='sub'))
        ast = parse_sql(sql)
        assert str(ast) == sql
        assert ast == expected_ast

        sql = f"""SELECT * FROM (SELECT column1 FROM t1)"""
        expected_ast = Select(targets=[Identifier("*")],
                              from_table=Select(targets=[Identifier('column1')],
                                                from_table=Identifier('t1'),
                                                parentheses=True))
        ast = parse_sql(sql)
        assert str(ast) == sql
        assert ast == expected_ast

    def test_select_subquery_target(self):
        sql = f"""SELECT *, (SELECT 1) FROM t1"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Identifier("*"), Select(targets=[Constant(1)], parentheses=True)],
                              from_table=Identifier('t1'))
        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT *, (SELECT 1) AS ones FROM t1"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Identifier("*"), Select(targets=[Constant(1)], alias='ones', parentheses=True)],
                              from_table=Identifier('t1'))
        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_select_subquery_where(self):
        sql = f"""SELECT * FROM tab1 WHERE column1 IN (SELECT column2 FROM t2)"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Identifier("*")],
                              from_table=Identifier('tab1'),
                              where=BinaryOperation(op='IN',
                                                    args=(
                                                        Identifier('column1'),
                                                        Select(targets=[Identifier('column2')],
                                                               from_table=Identifier('t2'),
                                                               parentheses=True)
                                                    )))
        assert str(ast) == sql
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_type_cast(self):
        sql = f"""SELECT CAST(4 AS int64) AS result"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[TypeCast(type_name='int64', arg=Constant(4), alias='result')])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT CAST(column1 AS float) AS result"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[TypeCast(type_name='float', arg=Identifier(value='column1'), alias='result')])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = f"""SELECT CAST((column1 + column2) AS float) AS result"""
        ast = parse_sql(sql)
        expected_ast = Select(targets=[TypeCast(type_name='float', arg=BinaryOperation(op='+', parentheses=True, args=[
            Identifier(value='column1'), Identifier(value='column2')]), alias='result')])
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_in_tuple(self):
        sql = "SELECT col FROM tab WHERE col IN (1, 2)"
        ast = parse_sql(sql)
        expected_ast = Select(targets=[Identifier('col')],
                              from_table=Identifier('tab'),
                              where=BinaryOperation(op='IN',
                                                    args=(
                                                        Identifier('col'),
                                                        Tuple(items=[Constant(1), Constant(2)])
                                                    )))
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_count_distinct(self):
        sql = "SELECT COUNT(DISTINCT survived) AS uniq_survived FROM titanic"
        ast = parse_sql(sql)

        expected_ast = Select(
            targets=[Function(op='COUNT', distinct=True,
                              args=(Identifier(value='survived'),), alias='uniq_survived')],
            from_table=Identifier(value='titanic')
        )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_where_not_order(self):
        sql = "SELECT col1 FROM tab WHERE NOT col1 = \"FAMILY\""
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier('col1')],
                              from_table=Identifier('tab'),
                              where=UnaryOperation(op='NOT',
                                   args=(
                                      BinaryOperation(op='=',
                                                      args=(Identifier('col1'), Constant('FAMILY'))),
                                   )
                              )
                          )
        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

    def test_backticks(self):
        sql = "SELECT `name`, `status` FROM `mindsdb`.`wow stuff predictors`"
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier('name'), Identifier('status')],
                              from_table=Identifier('mindsdb.wow stuff predictors'),

                              )

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

        sql = "SELECT `my column name` FROM table WHERE `other column name` = 'bla bla ``` bla'"
        ast = parse_sql(sql)

        expected_ast = Select(targets=[Identifier('my column name')],
                              from_table=Identifier('table'),
                              where=BinaryOperation(op='=', args=(
                                      Identifier('other column name'),
                                      Constant('bla bla ``` bla')
                                  )
                              ))

        assert ast.to_tree() == expected_ast.to_tree()
        assert str(ast) == str(expected_ast)

