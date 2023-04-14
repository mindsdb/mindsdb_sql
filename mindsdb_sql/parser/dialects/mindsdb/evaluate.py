from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.base import ASTNode


class Evaluate(ASTNode):
    def __init__(self,
                 name,
                 query_str,
                 using=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.using = using
        self.query_str = query_str

    def get_string(self, *args, **kwargs):
        using_str = ", ".join([f"{k}={v}" for k, v in self.using.items()])
        out_str = f'EVALUATE {self.name.to_string()} from ({self.query_str}) using {using_str}'
        return out_str
