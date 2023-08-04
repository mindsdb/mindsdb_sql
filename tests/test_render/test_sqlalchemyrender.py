import copy
import inspect
import datetime as dt

from mindsdb_sql.parser.ast import *
from mindsdb_sql import parse_sql
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb_sql.planner.utils import query_traversal

from tests.test_parser.test_base_sql import (
    test_select_operations,
    test_delete,
    test_insert,
    test_select_common_table_expression,
    test_select_structure,
    test_union,
)

modules = (
    test_select_operations,
    test_delete,
    test_insert,
    test_select_common_table_expression,
    test_select_structure,
    test_union,
)


def parse_sql2(sql, dialect='sqlite'):
    # convert to ast
    query = parse_sql(sql, dialect)

    # skip

    # step1: use mysql dialect and parse again
    try:
        sql2 = SqlalchemyRender('mysql').get_string(query, with_failback=False)
    except NotImplementedError:
        # skip not implemented, immediately exit
        return query

    # remove generated join condition
    sql2 = sql2.replace('ON 1=1', '')

    # workarounds for joins
    if 'INNER JOIN' not in sql:
        sql2 = sql2.replace('INNER JOIN', 'JOIN')

    if 'LEFT OUTER JOIN' not in sql:
        sql2 = sql2.replace('LEFT OUTER JOIN', 'LEFT JOIN')

    if 'FULL OUTER JOIN' not in sql:
        sql2 = sql2.replace('FULL OUTER JOIN', 'FULL JOIN')

    if 'RIGHT JOIN' in sql:
        # TODO skip now, but fix later
        return query

    # cast
    # TODO fix parse error 'SELECT CAST(4 AS SIGNED INTEGER)'
    if ' CAST(4 AS SIGNED INTEGER)' in sql2:
        return query
    sql2 = sql2.replace(' FLOAT', ' float')

    query2 = parse_sql(sql2, 'mysql')

    # exclude cases when sqlalchemy replaces some parts of sql
    if not (
        'not a=' in sql  # replaced to a!=
        or 'NOT col1 =' in sql  # replaced to col1!=
        or ' || ' in sql  # replaced to concat(
        or 'current_user()' in sql  # replaced to CURRENT_USER
        or 'user()' in sql  # replaced to USER
    ):

        # sqlalchemy could add own aliases for constant
        def clear_target_aliases(node, **args):
            # clear target aliases
            if isinstance(node, Select):
                if node.targets is not None:
                    for target in node.targets:
                        if isinstance(target, Constant) \
                                or isinstance(target, Select) \
                                or isinstance(target, WindowFunction) \
                                or isinstance(target, Function):
                            target.alias = None

                # clear subselect alias
                if isinstance(node.from_table, Select):
                    node.from_table.alias = None

        query_ = copy.deepcopy(query)
        query_traversal(query_, clear_target_aliases)
        query_traversal(query2, clear_target_aliases)

        # and compare with ast before render
        assert query2.to_tree() == query_.to_tree()

    # step 2: render to different dialects
    dialects = ('postgresql', 'sqlite', 'mssql', 'oracle')

    for dialect2 in dialects:

        try:
            SqlalchemyRender(dialect2).get_string(query, with_failback=False)
        except Exception as e:
            # skips for dialects
            if dialect2 == 'oracle' \
                    and 'does not support in-place multirow inserts' in str(e):
                pass
            elif dialect2 == 'mssql' \
                    and 'requires an order_by when using an OFFSET or a non-simple LIMIT clause' in str(e):
                pass
            elif dialect2 == 'sqlite' and 'extract(MONTH' in sql:
                pass
            else:
                print(dialect2, query.to_string())
                raise

    # keep original behavior
    return query


class TestFromParser:

    def test_from_parser(self):

        for module in modules:
            # inject function
            module.parse_sql = parse_sql2

            for class_name, klass in inspect.getmembers(module, predicate=inspect.isclass):
                if not class_name.startswith('Test'):
                    continue

                tests = klass()
                for test_name, test_method in inspect.getmembers(tests, predicate=inspect.ismethod):
                    if not test_name.startswith('test_') or test_name.endswith('_error'):
                        continue
                    sig = inspect.signature(test_method)
                    args = []
                    # add dialect
                    if 'dialect' in sig.parameters:
                        args.append('mysql')
                    test_method(*args)


class TestRender:
    def test_create_table(self):

        query = CreateTable(
            name='tbl1',
            columns=[
                TableColumn(name='a', type='DATE'),
                TableColumn(name='b', type='INTEGER'),
            ]
        )

        sql = SqlalchemyRender('mysql').get_string(query, with_failback=False)

        sql2 = '''CREATE TABLE tbl1 (a DATE, b INTEGER)'''

        assert sql.replace('\n', '').replace('\t', '').replace('  ', ' ') == sql2

    def test_datetype(self):
        query = Select(targets=[Constant(value=dt.datetime(2011, 1, 1))])

        sql = SqlalchemyRender('mysql').get_string(query, with_failback=False)

        sql2 = '''SELECT '2011-01-01 00:00:00' AS `2011-01-01 00:00:00`'''
        assert sql == sql2

        query = Select(targets=[Star()],
                       from_table=Identifier('tb1'),
                       where=BinaryOperation(op='in', args=[
                           Identifier('x'),
                           Tuple(items=[Constant(value=dt.datetime(2011, 1, 1)),
                                        Constant(value=dt.datetime(2011, 1, 2))])
                       ]))
        sql = SqlalchemyRender('mysql').get_string(query, with_failback=False)

        sql2 = '''SELECT * FROM tb1 WHERE x IN ('2011-01-01 00:00:00', '2011-01-02 00:00:00')'''
        assert sql.replace('\n', '').replace('\t', '').replace('  ', ' ') == sql2



