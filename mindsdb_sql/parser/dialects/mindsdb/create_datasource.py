import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateDatasource(ASTNode):
    def __init__(self,
                 name,
                 engine,
                 parameters,
                 is_replace=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.engine = engine
        self.parameters = parameters
        self.is_replace = is_replace

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={repr(self.name)},'
        engine_str = f'\n{ind1}engine={repr(self.engine)},'
        parameters_str = f'\n{ind1}parameters={str(self.parameters)},'

        replace_str = ''
        if self.is_replace:
            replace_str = f'\n{ind1}is_replace=True'

        out_str = f'{ind}CreateDatasource(' \
                  f'{name_str}' \
                  f'{engine_str}' \
                  f'{parameters_str}' \
                  f'{replace_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        replace_str = ''
        if self.is_replace:
            replace_str = f' OR REPLACE'

        out_str = f'CREATE{replace_str} DATASOURCE {str(self.name)} WITH ENGINE = {repr(self.engine)}, PARAMETERS = {json.dumps(self.parameters)}'
        return out_str
