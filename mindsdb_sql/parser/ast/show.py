from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class Show(ASTNode):
    def __init__(self,
                 category,
                 from_table=None,
                 where=None,
                 *args_, **kwargs):
        super().__init__(*args_, **kwargs)
        self.category = category
        self.where = where
        self.from_table = from_table

    def to_tree(self, *args, level=0, **kwargs):

        ind = indent(level)
        ind1 = indent(level+1)
        category_str = f'{ind1}category={repr(self.category)},'
        from_str = f'\n{ind1}from={self.from_table.to_tree(level=level+2)},' if self.from_table else ''
        where_str = f'\n{ind1}where=\n{self.where.to_tree(level=level+2)},' if self.where else ''
        out_str = f'{ind}Show(' \
                  f'{category_str}' \
                  f'{from_str}' \
                  f'{where_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        from_str = f' FROM {str(self.from_table)}' if self.from_table else ''
        where_str = f' WHERE {str(self.where)}' if self.where else ''
        return f'SHOW {self.category}{from_str}{where_str}'
