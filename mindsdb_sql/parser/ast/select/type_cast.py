from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class TypeCast(ASTNode):
    def __init__(self, type_name, arg, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.type_name = type_name
        self.arg = arg

    def to_tree(self, *args, level=0, **kwargs):
        out_str = indent(level) + f'TypeCast(type_name={repr(self.type_name)}, arg=\n{indent(level+1)}{self.arg.to_tree()})'
        return out_str

    def get_string(self, *args, **kwargs):
        return f'CAST({str(self.arg)} AS {self.type_name})'
