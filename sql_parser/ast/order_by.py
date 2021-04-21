from sql_parser.ast.base import ASTNode


class OrderBy(ASTNode):
    def __init__(self, field, direction='default', nulls='default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field = field
        self.direction = direction
        self.nulls = nulls

    def to_string(self, *args, **kwargs):
        out_str = self.field.to_string()
        if self.direction != 'default':
            out_str += f' {self.direction}'
        if self.nulls != 'default':
            out_str += f' {self.nulls}'
        return out_str
