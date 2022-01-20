from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class RetrainPredictor(ASTNode):
    def __init__(self,
                 name,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_tree()},'

        out_str = f'{ind}RetrainPredictor(' \
                  f'{name_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'RETRAIN {str(self.name)}'
        return out_str
