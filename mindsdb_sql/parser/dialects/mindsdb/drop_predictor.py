import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class DropPredictor(ASTNode):
    def __init__(self,
                 name,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={self.name.to_tree()},'

        out_str = f'{ind}DropPredictor(' \
                  f'{name_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP PREDICTOR {str(self.name)}'
        return out_str
