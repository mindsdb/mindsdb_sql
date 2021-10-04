from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Set(ASTNode):
    def __init__(self,
                 category,
                 arg=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category = category
        self.arg = arg

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        category_str = f'category={self.category}, '
        arg_str = f'arg={self.arg.to_tree(level=level+2)},' if self.arg else ''

        out_str = f'{ind}Set(' \
                  f'{category_str}' \
                  f'{arg_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        arg_str = f' {str(self.arg)}' if self.arg else ''
        return f'SET {self.category}{arg_str}'

