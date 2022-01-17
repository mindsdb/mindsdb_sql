import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.query import aliased
from sqlalchemy.dialects import mysql, postgresql, sqlite, mssql

from mindsdb_sql.parser import ast


class SqlalchemyRender():

    def __init__(self, dialect_name):
        dialects = {
            'mysql': mysql,
            'postgresql': postgresql,
            'postgres': postgresql,
            'sqlite': sqlite,
            'mssql': mssql,
        }

        self.dialect = dialects[dialect_name].dialect()

    def to_column(self, parts):
        # because sqlalchemy doesn't allow columns consist from parts therefore we do it manually
        parts2 = [
            str(sa.column(i).compile(dialect=self.dialect))
            for i in parts
        ]

        return sa.column('.'.join(parts2), is_literal=True)

    def get_alias(self, alias):
        if alias is None or len(alias.parts) == 0:
            return None
        if len(alias.parts) > 1:
            raise NotImplementedError(f'Multiple alias {alias.parts}')
        return alias.parts[0]

    def to_expression(self, t):

        if isinstance(t, ast.Star):
            col = '*'
        elif isinstance(t, ast.Constant):
            col = sa.literal(t.value)
            if t.alias:
                alias = self.get_alias(t.alias)
            else:
                alias = str(t.value)
            col = col.label(alias)
        elif isinstance(t, ast.Identifier):
            col = self.to_column(t.parts)
            if t.alias:
                col = col.label(self.get_alias(t.alias))
        elif isinstance(t, ast.Select):
            sub_stmt = self.prepare_select(t)
            col = sub_stmt.scalar_subquery()
            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)
        elif isinstance(t, ast.Function):
            op = getattr(sa.func, t.op)
            args = [
                self.to_expression(i)
                for i in t.args
            ]
            if t.distinct:
                # set first argument to distinct
                args[0] = args[0].distinct()
            col = op(*args)

            if t.alias:
                alias = self.get_alias(t.alias)
            else:
                alias = str(t.op)
            col = col.label(alias)
        elif isinstance(t, ast.BinaryOperation):
            methods = {
                "+": "__add__",
                "-": "__sub__",
                "/": "__div__",
                "*": "__mul__",
                "%": "__mod__",
                "=": "__eq__",
                "!=": "__ne__",
                ">": "__gt__",
                "<": "__lt__",
                ">=": "__ge__",
                "<=": "__le__",
                "is": "is_",
                "is not": "is_not",
                "like": "like",
                "in": "in_",
                "not in": "notin_",
                "||": "concat",
            }
            functions = {
                "and": sa.and_,
                "or": sa.or_,
            }

            arg0 = self.to_expression(t.args[0])
            arg1 = self.to_expression(t.args[1])

            method = methods.get(t.op.lower())
            if method is not None:
                sa_op = getattr(arg0, method)

                col = sa_op(arg1)
            else:
                func = functions[t.op.lower()]
                col = func(arg0, arg1)

            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)

        elif isinstance(t, ast.UnaryOperation):
            # not or munus
            opmap = {
                "NOT": "__invert__",
                "-": "__neg__",
            }
            arg = self.to_expression(t.args[0])

            method = opmap[t.op.upper()]
            col = getattr(arg, method)()
            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)

        elif isinstance(t, ast.BetweenOperation):
            col0 = self.to_expression(t.args[0])
            lim_down = self.to_expression(t.args[1])
            lim_up = self.to_expression(t.args[2])

            col = sa.between(col0, lim_down, lim_up)
        elif isinstance(t, ast.WindowFunction):
            func = self.to_expression(t.function)

            partition = None
            if t.partition is not None:
                partition = [
                    self.to_expression(i)
                    for i in t.partition
                ]

            order_by = None
            if t.order_by is not None:
                order_by = []
                for f in t.order_by:
                    col0 = self.to_expression(f.field)
                    if f.direction == 'DESC':
                        col0 = col0.desc()
                    order_by.append(col0)

            col = sa.over(
                func,
                partition_by=partition,
                order_by=order_by
            )

            if t.alias:
                col = col.label(self.get_alias(t.alias))
        elif isinstance(t, ast.TypeCast):
            arg = self.to_expression(t.arg)
            # TODO how to get type
            typename =  t.type_name.upper()
            if typename == 'INT64':
                typename = 'BIGINT'
            type = getattr(sa.types, typename)
            col = sa.cast(arg, type)

            if t.alias:
                alias = self.get_alias(t.alias)
                col = col.label(alias)
        elif isinstance(t, ast.Parameter):
            col = sa.column(t.value, is_literal=True)
            if t.alias: raise Exception()
        elif isinstance(t, ast.Tuple):
            col = [
                self.to_expression(i)
                for i in t.items
            ]
        elif isinstance(t, ast.Variable):
            col = sa.column(t.to_string(), is_literal=True)
        elif isinstance(t, ast.Latest):
            col = sa.column(t.to_string(), is_literal=True)
        else:
            # some other complex object?
            raise NotImplementedError(f'Column {t}')

        return col

    def prepare_join(self, join):
        # join tree to table list

        if isinstance(join.right, ast.Join):
            raise NotImplementedError('Wrong join AST')

        items = []

        if isinstance(join.left, ast.Join):
            # dive to next level
            items.extend(self.prepare_join(join.left))
        else:
            # this is first table
            items.append(dict(
                table=join.left
            ))

        # all properties set to right table
        items.append(dict(
            table=join.right,
            join_type=join.join_type,
            is_implicit=join.implicit,
            condition=join.condition
        ))

        return items

    def to_table(self, node):
        if isinstance(node, ast.Identifier):
            parts = node.parts

            if len(parts) > 2:
                # TODO tests is failing
                raise NotImplementedError(f'Path to long: {node.parts}')

            schema = None
            if len(parts) == 2:
                schema = parts[-2]

            table_name = parts[-1]

            table = sa.table(table_name, schema=schema)

            if node.alias:
                table = aliased(table, name=self.get_alias(node.alias))

        elif isinstance(node, ast.Select):
            sub_stmt = self.prepare_select(node)
            alias = None
            if node.alias:
                alias = self.get_alias(node.alias)
            table = sub_stmt.subquery(alias)

        else:
            # TODO tests are failing
            raise NotImplementedError(f'Table {node.__name__}')

        return table

    def prepare_select(self, node):

        cols = []
        for t in node.targets:
            col = self.to_expression(t)
            cols.append(col)

        query = sa.select(cols)

        if node.cte is not None:
            for cte in node.cte:
                stmt = self.prepare_select(cte.query)
                alias = cte.name

                query = query.add_cte(stmt.cte(self.get_alias(alias), nesting=True))

        if node.distinct:
            query = query.distinct()

        if node.from_table is not None:
            from_table = node.from_table

            if isinstance(from_table, ast.Join):
                join_list = self.prepare_join(from_table)
                # first table
                table = self.to_table(join_list[0]['table'])
                query = query.select_from(table)

                # other tables
                for item in join_list[1:]:
                    table = self.to_table(item['table'])
                    if item['is_implicit']:
                        # add to from clause
                        query = query.select_from(table)
                    else:
                        if item['condition'] is None:
                            # otherwise, sqlalchemy raises "Don't know how to join to ..."
                            condition = sa.text('1==1')
                        else:
                            condition = self.to_expression(item['condition'])

                        join_type = item['join_type']
                        method = 'join'
                        is_full = False
                        if join_type == 'LEFT JOIN':
                            method = 'outerjoin'
                        if join_type == 'FULL JOIN':
                            is_full = True

                        # perform join
                        query = getattr(query, method)(
                            table,
                            condition,
                            full=is_full
                        )
            elif isinstance(from_table, ast.Union):
                if not(isinstance(from_table.left, ast.Select) and isinstance(from_table.right, ast.Select)):
                    raise NotImplementedError(f'Unknown UNION {from_table.left.__name__}, {from_table.right.__name__}')

                left = self.prepare_select(from_table.left)
                right = self.prepare_select(from_table.right)

                alias = None
                if from_table.alias:
                    alias = self.get_alias(from_table.alias)

                table = left.union(right).subquery(alias)
                query = query.select_from(table)

            elif isinstance(from_table, ast.Select):
                table = self.to_table(from_table)
                query = query.select_from(table)

            elif isinstance(from_table, ast.Identifier):
                table = self.to_table(from_table)
                query = query.select_from(table)
            else:
                raise NotImplementedError(f'Select from {from_table}')

        if node.where is not None:
            query = query.filter(
                self.to_expression(node.where)
            )

        if node.group_by is not None:
            cols = [
                self.to_expression(i)
                for i in node.group_by
            ]
            query = query.group_by(*cols)

        if node.having is not None:
            query = query.having(self.to_expression(node.having))

        if node.order_by is not None:
            order_by = []
            for f in node.order_by:
                col0 = self.to_expression(f.field)
                if f.direction == 'DESC':
                    col0 = col0.desc()
                order_by.append(col0)

            query = query.order_by(*order_by)

        if node.limit is not None:
            query = query.limit(node.limit.value)

        if node.offset is not None:
            query = query.offset(node.offset.value)

        if node.mode is not None:
            if node.mode == 'FOR UPDATE':
                query = query.with_for_update()
            else:
                raise NotImplementedError(f'Select mode: {node.mode}')

        return query

    def get_string(self, ast_query, with_failback=True):
        try:
            if isinstance(ast_query, ast.Select):
                stmt = self.prepare_select(ast_query)
            else:
                raise NotImplementedError(f'Unknown statement: {ast_query.__name__}')

            return str(stmt.compile(dialect=self.dialect, compile_kwargs={'literal_binds': True}))

        except (SQLAlchemyError, NotImplementedError) as e:
            if not with_failback:
                raise e

            sql_query = str(ast_query)
            if self.dialect.name == 'postgresql':
                sql_query = sql_query.replace('`', '')
            return sql_query
