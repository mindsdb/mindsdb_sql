from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class CreateView(ASTNode):
    def __init__(self,
                 name,
                 query,
                 from_table=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.query = query
        self.from_table = from_table

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={repr(self.name)},'
        from_table_str = f'\n{ind1}from_table=\n{self.from_table.to_tree(level=level+2)},' if self.from_table else ''
        query_str = f'\n{ind1}query=\n{self.query.to_tree(level=level+2)},'

        out_str = f'{ind}CreateView(' \
                  f'{name_str}' \
                  f'{query_str}' \
                  f'{from_table_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        from_str = f'FROM {str(self.from_table)} ' if self.from_table else ''
        out_str = f'CREATE VIEW {str(self.name)} {from_str}AS ( {str(self.query)} )'

        return out_str
