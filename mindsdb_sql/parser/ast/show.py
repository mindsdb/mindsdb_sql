from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class Show(ASTNode):
    def __init__(self,
                 category,
                 condition=None,
                 expression=None,
                 *args_, **kwargs):
        super().__init__(*args_, **kwargs)
        self.category = category
        self.condition = condition
        self.expression = expression

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        category_str = f'{ind1}category={repr(self.category)},'
        condition_str = f'{ind1}condition={repr(self.condition)},' if self.condition else ''
        expr_str = f'{ind1}expression=\n{self.expression.to_tree(level=level+2)},' if self.expression else ''
        out_str = f'{ind}Show(' \
                  f'{category_str}' \
                  f'{condition_str}' \
                  f'{expr_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        condition_str = f' {self.condition}' if self.condition else ''
        expression_str = f' {str(self.expression)}' if self.expression else ''
        return f'SHOW {self.category}{condition_str}{expression_str}'
