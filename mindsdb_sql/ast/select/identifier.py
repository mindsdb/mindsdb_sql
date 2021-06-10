from mindsdb_sql.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Identifier(ASTNode):
    def __init__(self, value, wrap=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value
        self.wrap = wrap

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={repr(self.alias)}' if self.alias else ''
        wrap_str = f', wrap={repr(self.wrap)}' if self.wrap else ''
        return indent(level) + f'Identifier(value={repr(self.value)}{alias_str}{wrap_str})'

    def to_string(self, *args, **kwargs):
        wrap_str = self.wrap if self.wrap else ''
        value_str = f'{wrap_str}{str(self.value)}{wrap_str}'
        return self.maybe_add_alias(value_str)

