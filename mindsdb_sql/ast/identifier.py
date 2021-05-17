from mindsdb_sql.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Identifier(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={repr(self.alias)}' if self.alias else ''
        return indent(level) + f'Identifier(value={repr(self.value)}{alias_str})'

    def to_string(self, *args, **kwargs):
        return self.maybe_add_alias(str(self.value))

