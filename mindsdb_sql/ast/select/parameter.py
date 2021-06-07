from mindsdb_sql.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Parameter(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_tree(self, *args, level=0, **kwargs):
        return '\t' * level + f'Parameter({repr(self.value)})'

    def to_string(self, *args, **kwargs):
        return self.value
