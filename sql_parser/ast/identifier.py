from sql_parser.ast.base import ASTNode


class Identifier(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_string(self, *args, **kwargs):
        return self.maybe_add_alias(str(self.value))

