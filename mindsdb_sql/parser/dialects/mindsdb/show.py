from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Show(ASTNode):
    def __init__(self,
                 value,
                 arg=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value
        self.arg = arg

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        value_str = f'value={repr(self.value)},'
        arg_str = f'\n{ind1}arg=\n{self.arg.to_tree(level=level+2)},' if self.arg else ''
        out_str = f'{ind}Show(' \
                  f'{value_str}' \
                  f'{arg_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        arg_str = f' {str(self.arg)}' if self.arg else ''
        return f'SHOW {str(self.value)}{arg_str}'
