from sql_parser.ast.base import ASTNode


class Constant(ASTNode):
    def __init__(self, value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.value = value

    def to_string(self, *args, **kwargs):
        if isinstance(self.value, str):
            out_str = f"\"{self.value}\""
        else:
            out_str = str(self.value)
        return self.maybe_add_alias(out_str)
