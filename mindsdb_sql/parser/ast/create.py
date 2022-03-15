from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from typing import List


class TableColumn():
    def __init__(self, name, type='integer'):
        self.name = name
        self.type = type


class CreateTable(ASTNode):
    def __init__(self,
                 name,
                 from_select=None,
                 columns: List[TableColumn] = None,
                 is_replace=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.is_replace = is_replace
        self.from_select = from_select
        self.columns = columns

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level + 1)
        ind2 = indent(level + 2)

        replace_str = ''
        if self.is_replace:
            replace_str = f'{ind1}is_replace=True\n'

        from_select_str = ''
        if self.from_select is not None:
            from_select_str = f'{ind1}from_select={self.from_select.to_tree(level=level+1)}\n'

        columns_str = ''
        if self.columns is not None:
            columns = [
                f'{ind2}{col.name}: {col.type}'
                for col in self.columns
            ]

            columns_str = f'{ind1}columns=\n' + '\n'.join(columns)

        out_str = f'{ind}CreateTable(\n' \
                  f'{ind1}name={self.name}\n' \
                  f'{replace_str}' \
                  f'{from_select_str}' \
                  f'{columns_str}\n' \
                  f'{ind})\n'
        return out_str

    def get_string(self, *args, **kwargs):

        replace_str = ''
        if self.is_replace:
            replace_str = f' OR REPLACE'

        columns_str = ''
        if self.columns is not None:
            columns = [
                f'{col.name} {col.type}'
                for col in self.columns
            ]

            columns_str = '({})'.format(', '.join(columns))

        from_select_str = ''
        if self.from_select is not None:
            from_select_str = self.from_select.to_string()

        name_str = str(self.name)
        return f'CREATE{replace_str} TABLE {name_str} {columns_str} {from_select_str}'
