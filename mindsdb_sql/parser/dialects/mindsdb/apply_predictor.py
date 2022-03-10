import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class ApplyPredictor(ASTNode):
    def __init__(self,
                 name,
                 query_str,
                 result_table,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.query_str = query_str
        self.result_table = result_table

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        name_str = f'\n{ind1}name={self.name}'

        query_str = f'\n{ind1}query="{self.query_str}"'

        result_table_str = f'\n{ind1}result_table={self.result_table.to_string()}'

        out_str = f'{ind}ApplyPredictor(' \
                  f'{name_str},' \
                  f'{query_str},' \
                  f'{result_table_str}'\
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):

        out_str = f'APPLY PREDICTOR {self.name} USING ({self.query_str}) INTO TABLE={self.result_table.to_string()}'

        return out_str.strip()
