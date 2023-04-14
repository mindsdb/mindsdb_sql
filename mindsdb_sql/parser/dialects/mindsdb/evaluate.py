from mindsdb_sql.parser.utils import tokens_to_string
from mindsdb_sql.parser.ast.base import ASTNode


class Evaluate(ASTNode):
    def __init__(self,
                 name,
                 data,
                 using=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.using = using
        self.data = data

    def get_string(self, *args, **kwargs):
        if isinstance(self.data, list):
            inner_query_str = tokens_to_string(self.data)
        else:
            inner_query_str = self.data.to_string()
        out_str = f'EVALUATE {self.name.to_string()} from ({inner_query_str})'
        if self.using is not None:
            using_str = ", ".join([f"{k}={v}" for k, v in self.using.items()])
            out_str = f'{out_str} USING {using_str}'
        out_str += ';'
        return out_str
