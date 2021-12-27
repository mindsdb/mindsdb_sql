from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Insert(ASTNode):
    def __init__(self,
                 table,
                 columns=None,
                 values=None,
                 from_select=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table
        self.columns = columns

        # TODO require one of [values, from_select] is set
        self.values = values
        self.from_select = from_select

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        ind2 = indent(level + 2)
        if self.columns is not None:
            columns_str = ', '.join([i.to_tree() for i in self.columns])
        else:
            columns_str = ''

        if self.values is not None:
            values = []
            for row in self.values:
                row_str = f'\n'.join([i.to_tree(level=level+3) for i in row])
                values.append(f'{ind2}[\n{row_str}\n{ind2}],\n')
            values_str = ''.join(values)
            values_str = f'{ind1}values=[\n{values_str}{ind1}]\n'
        else:
            values_str = ''

        if self.from_select is not None:
            from_select_str = f'{ind1}from_select=\n{self.from_select.to_tree(level=level+2)}\n'
        else:
            from_select_str = ''

        out_str = f'{ind}Insert(table={self.table.to_tree()}\n' \
                  f'{ind1}columns=[{columns_str}]\n' \
                  f'{values_str}' \
                  f'{from_select_str}' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):
        if self.columns is not None:
            cols = ', '.join(map(str, self.columns))
            columns_str = f'({cols})'
        else:
            columns_str = ''

        if self.values is not None:
            values = []
            for row in self.values:
                row_str = ', '.join(map(str, row))
                values.append(f'({row_str})')
            values_str = 'VALUES ' + ', '.join(values)
        else:
            values_str = ''

        if self.from_select is not None:
            from_select_str = self.from_select
        else:
            from_select_str = ''

        return f'INSERT INTO {str(self.table)}{columns_str} {values_str}{from_select_str}'
