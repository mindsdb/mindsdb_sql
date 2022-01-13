from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent
from mindsdb_sql.parser import ast

import sqlalchemy as sa
from sqlalchemy.orm.query import aliased

class Select(ASTNode):

    def __init__(self,
                 targets,
                 distinct=False,
                 from_table=None,
                 where=None,
                 group_by=None,
                 having=None,
                 order_by=None,
                 limit=None,
                 offset=None,
                 cte=None,
                 mode=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.targets = targets
        self.distinct = distinct
        self.from_table = from_table
        self.where = where
        self.group_by = group_by
        self.having = having
        self.order_by = order_by
        self.limit = limit
        self.offset = offset
        self.cte = cte
        self.mode = mode

        if self.alias:
            self.parentheses = True

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        cte_str = ''
        if self.cte:
            cte_trees = ',\n'.join([t.to_tree(level=level + 2) for t in self.cte])
            cte_str = f'\n{ind1}cte=[\n{cte_trees}\n{ind1}],'

        alias_str = f'\n{ind1}alias={self.alias.to_tree()},' if self.alias else ''
        distinct_str = f'\n{ind1}distinct={repr(self.distinct)},' if self.distinct else ''
        parentheses_str = f'\n{ind1}parentheses={repr(self.parentheses)},' if self.parentheses else ''

        target_trees = ',\n'.join([t.to_tree(level=level+2) for t in self.targets])
        targets_str = f'\n{ind1}targets=[\n{target_trees}\n{ind1}],'

        from_str = f'\n{ind1}from_table=\n{self.from_table.to_tree(level=level+2)},' if self.from_table else ''
        where_str = f'\n{ind1}where=\n{self.where.to_tree(level=level+2)},' if self.where else ''

        group_by_str = ''
        if self.group_by:
            group_by_trees = ',\n'.join([t.to_tree(level=level+2) for t in self.group_by])
            group_by_str = f'\n{ind1}group_by=[\n{group_by_trees}\n{ind1}],'

        having_str = f'\n{ind1}having=\n{self.having.to_tree(level=level+2)},' if self.having else ''

        order_by_str = ''
        if self.order_by:
            order_by_trees = ',\n'.join([t.to_tree(level=level + 2) for t in self.order_by])
            order_by_str = f'\n{ind1}order_by=[\n{order_by_trees}\n{ind1}],'
        limit_str = f'\n{ind1}limit={self.limit.to_tree(level=0)},' if self.limit else ''
        offset_str = f'\n{ind1}offset={self.offset.to_tree(level=0)},' if self.offset else ''
        mode_str = f'\n{ind1}mode={self.mode},' if self.mode else ''

        out_str = f'{ind}Select(' \
                  f'{cte_str}' \
                  f'{alias_str}' \
                  f'{distinct_str}' \
                  f'{parentheses_str}' \
                  f'{targets_str}' \
                  f'{from_str}' \
                  f'{where_str}' \
                  f'{group_by_str}' \
                  f'{having_str}' \
                  f'{order_by_str}' \
                  f'{limit_str}' \
                  f'{offset_str}' \
                  f'{mode_str}' \
                  f'\n{ind})'
        return out_str

    def to_column(self, parts):
        if len(parts) > 3:
            raise not NotImplementedError(f'Path to long: {parts}')

        colname = parts[-1]
        col = sa.column(colname)

        if len(parts) == 1:
            return col

        schema = None
        if len(parts) == 3:
            schema = parts[0]

        table = parts[-2]

        return sa.table(table, col, schema=schema).c[colname]

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
                col = col.label(self.get_alias(t.alias))
        elif isinstance(t, ast.Identifier):
            col = self.to_column(t.parts)
            if t.alias:
                col = col.label(self.get_alias(t.alias))
        elif isinstance(t, Select):
            sub_stmt = t.to_statement()
            col = sub_stmt.scalar_subquery()
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
        elif isinstance(t, ast.BinaryOperation):
            opmap = {
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
                "IS NOT": "is_not",
                "like": "like",
                "in": "in_",
                "and": "and_",
                "or": "or_",
                "||": "concat",
            }
            arg0 = self.to_expression(t.args[0])
            arg1 = self.to_expression(t.args[1])

            sa_op = getattr(arg0, opmap[t.op])

            col = sa_op(arg1)
        elif isinstance(t, ast.UnaryOperation):
            # not or munus
            opmap = {
                "NOT": "__invert__",
                "-": "__neg__",
            }
            arg = self.to_expression(t.args[0])

            col = getattr(arg, opmap[t.op])()
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
            type = getattr(sa.types, t.type_name.upper())
            col = sa.cast(arg, type)
        else:
            # some other complex object?
            raise NotImplementedError(f'Column {t}')
            col = sa.text(t.get_string(dialect=dialect, *args, **kwargs))

        return col

    def to_statement(self):

        cols = []
        for t in self.targets:
            col = self.to_expression(t)
            cols.append(col)

        query = sa.select(cols)

        if self.cte is not None:
            for cte in self.cte:
                stmt = cte.query.to_statement()
                alias = cte.name

                query = query.add_cte(stmt.cte(self.get_alias(alias)))

        if self.distinct:
            query = query.distinct()

        def to_table(node):
            if isinstance(node, ast.Identifier):
                table = sa.table('.'.join(node.parts))
                if node.alias:
                    table = aliased(table, name=self.get_alias(node.alias))

            elif isinstance(node, Select):
                sub_stmt = node.to_statement()
                alias = None
                if node.alias:
                    alias = self.get_alias(node.alias)
                table = sub_stmt.subquery(alias)

            else:
                raise NotImplementedError(f'Table {node}')

            return table

        if self.from_table is not None:

            if isinstance(self.from_table, ast.Join):
                join_list = self.prepare_join(self.from_table)
                # first table
                table = to_table(join_list[0]['table'])
                query = query.select_from(table)

                # other tables
                for item in join_list[1:]:
                    table = to_table(item['table'])
                    if item['is_implicit']:
                        # add to from clause
                        query = query.select_from(table)
                    else:
                        if item['condition'] is None:
                            # otherwise sqlalchemy raises "Don't know how to join to ..."
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

            else:
                table = to_table(self.from_table)
                query = query.select_from(table)

        if self.where is not None:
            query = query.filter(
                self.to_expression(self.where)
            )

        if self.group_by is not None:
            cols = [
                self.to_expression(i)
                for i in self.group_by
            ]
            query = query.group_by(*cols)

        if self.having is not None:
            query = query.having(self.to_expression(self.having))

        if self.order_by is not None:
            order_by = []
            for f in self.order_by:
                col0 = self.to_expression(f.field)
                if f.direction == 'DESC':
                    col0 = col0.desc()
                order_by.append(col0)

            query = query.order_by(*order_by)

        if self.limit is not None:
            query = query.limit(self.limit)

        if self.offset is not None:
            query = query.offset(self.offset)

        if self.mode is not None:
            if self.mode == 'FOR UPDATE':
                query = query.with_for_update()
            else:
                raise NotImplementedError(f'Select mode: {self.mode}')

        return query

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

    def get_string(self, *args, **kwargs):
        stmt = self.to_statement()
        from sqlalchemy.dialects import mysql
        # print(stmt.compile(dialect=mysql.dialect(), compile_kwargs={'literal_binds': True}))

        out_str = ''
        if self.cte is not None:
            cte_str = ', '.join([out.to_string() for out in self.cte])
            out_str += f'WITH {cte_str} '

        out_str += "SELECT"

        if self.distinct:
            out_str += ' DISTINCT'

        targets_str = ', '.join([out.to_string() for out in self.targets])
        out_str += f' {targets_str}'

        if self.from_table is not None:
            from_table_str = str(self.from_table)
            out_str += f' FROM {from_table_str}'

        if self.where is not None:
            out_str += f' WHERE {self.where.to_string()}'

        if self.group_by is not None:
            group_by_str = ', '.join([out.to_string() for out in self.group_by])
            out_str += f' GROUP BY {group_by_str}'

        if self.having is not None:
            having_str = str(self.having)
            out_str += f' HAVING {having_str}'

        if self.order_by is not None:
            order_by_str = ', '.join([out.to_string() for out in self.order_by])
            out_str += f' ORDER BY {order_by_str}'

        if self.limit is not None:
            out_str += f' LIMIT {self.limit.to_string()}'

        if self.offset is not None:
            out_str += f' OFFSET {self.offset.to_string()}'

        if self.mode is not None:
            out_str += f' {self.mode}'
        return out_str
