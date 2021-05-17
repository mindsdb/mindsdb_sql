class ASTNode:
    def __init__(self, alias=None, parentheses=False):
        self.alias = alias
        self.parentheses = parentheses

    def maybe_add_alias(self, some_str):
        if self.alias:
            return f'{some_str} AS {self.alias}'
        else:
            return some_str

    def maybe_add_parentheses(self, some_str):
        if self.parentheses:
            return f'({some_str})'
        else:
            return some_str

    def to_tree(self, *args, **kwargs):
        pass

    def to_string(self, *args, level=0, **kwargs):
        pass

    def __str__(self):
        return self.to_string()

    def __eq__(self, other):
        if isinstance(other, ASTNode):
            return str(self) == str(other)
        else:
            return False
