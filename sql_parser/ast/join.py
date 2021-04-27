from sql_parser.ast.base import ASTNode


class Join(ASTNode):
    def __init__(self, join_type, left, right, condition=None, implicit=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.join_type = join_type
        self.left = left
        self.right = right
        self.condition = condition
        self.implicit = implicit

    def to_string(self, *args, **kwargs):
        join_type_str = f' {self.join_type} ' if not self.implicit else ', '
        condition_str = f' ON {self.condition.to_string()}' if self.condition else ''
        return f'{self.left.to_string()}{join_type_str}{self.right.to_string()}{condition_str}'
