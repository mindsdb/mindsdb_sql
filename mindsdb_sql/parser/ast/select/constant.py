from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Constant(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={self.alias.to_tree()}' if self.alias else ''
        return indent(level) + f'Constant(value={repr(self.value)}{alias_str})'

    def get_string(self, *args, **kwargs):
        if isinstance(self.value, str):
            out_str = f"\'{self.value}\'"
        elif isinstance(self.value, bool):
            out_str = 'TRUE' if self.value else 'FALSE'
        else:
            out_str = str(self.value)
        return out_str


class NullConstant(Constant):
    def __init__(self, *args, **kwargs):
        super().__init__(value=None, *args, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        return '\t'*level +  'NullConstant()'

    def get_string(self, *args, **kwargs):
        return 'NULL'
