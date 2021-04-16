from sql_parser.ast.base import ASTNode


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

    def maybe_add_alias(self, some_str, is_top_select):
        if self.alias:
            return f'({some_str}) as {self.alias}'
        elif is_top_select:
            return some_str
        else:
            return f'({some_str})'

    def to_string(self, *args, is_top_select=False, **kwargs):
        out_str = """SELECT"""

        if self.distinct:
            out_str += ' DISTINCT'

        targets_str = ', '.join([out.to_string() for out in self.targets])
        out_str += f' {targets_str}'

        if self.from_table is not None:
            from_list_str = str(self.from_table)
            out_str += f' FROM {from_list_str}'

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
        return self.maybe_add_alias(out_str, is_top_select=is_top_select)

    def __str__(self):
        return self.to_string(is_top_select=True)