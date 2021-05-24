from mindsdb_sql.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Constant(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={repr(self.alias)}' if self.alias else ''
        return indent(level) + f'Constant(value={repr(self.value)}{alias_str})'

    def to_string(self, *args, **kwargs):
        if isinstance(self.value, str):
            out_str = f"\"{self.value}\""
        elif isinstance(self.value, bool):
            out_str = 'TRUE' if self.value else 'FALSE'
        else:
            out_str = str(self.value)
        return self.maybe_add_alias(out_str)


class NullConstant(Constant):
    def __init__(self, *args, **kwargs):
        super().__init__(value=None, *args, **kwargs)

    def to_tree(self, *args, level=0, **kwargs):
        return '\t'*level +  'NullConstant()'

    def to_string(self, *args, **kwargs):
        return 'NULL'
