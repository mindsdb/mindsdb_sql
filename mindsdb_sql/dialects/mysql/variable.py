from mindsdb_sql.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Variable(ASTNode):
    def __init__(self, value, is_system_var=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value
        self.is_system_var = is_system_var

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={repr(self.alias)}' if self.alias else ''
        return indent(level) + f'Variable(value={repr(self.value)}{alias_str}, is_system_var={repr(self.is_system_var)})'

    def to_string(self, *args, **kwargs):
        return self.maybe_add_alias( ('@@' if self.is_system_var else '@') + f'{str(self.value)}')

