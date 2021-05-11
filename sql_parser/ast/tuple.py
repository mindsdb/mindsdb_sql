from sql_parser.ast.base import ASTNode


class Tuple(ASTNode):
    def __init__(self, items, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items = items

    def to_string(self, *args, **kwargs):
        item_strs = []
        for item in self.items:
            item_strs.append(str(item))

        return f'({", ".join(item_strs)})'
