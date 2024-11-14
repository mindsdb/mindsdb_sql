from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CombiningQuery(ASTNode):
    operation = None

    def __init__(self,
                 left,
                 right,
                 unique=True,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.unique = unique

        if self.alias:
            self.parentheses = True

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        left_str = f'\n{ind1}left=\n{self.left.to_tree(level=level + 2)},'
        right_str = f'\n{ind1}right=\n{self.right.to_tree(level=level + 2)},'

        cls_name = self.__class__.__name__
        out_str = f'{ind}{cls_name}(unique={repr(self.unique)},' \
                  f'{left_str}' \
                  f'{right_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        left_str = str(self.left)
        right_str = str(self.right)
        keyword = self.operation
        if not self.unique:
            keyword += ' ALL'
        out_str = f"""{left_str}\n{keyword}\n{right_str}"""

        return out_str


class Union(CombiningQuery):
    operation = 'UNION'


class Intersect(CombiningQuery):
    operation = 'INTERSECT'


class Except(CombiningQuery):
    operation = 'EXCEPT'
