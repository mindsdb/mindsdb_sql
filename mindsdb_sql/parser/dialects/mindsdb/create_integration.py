import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class CreateIntegration(ASTNode):
    def __init__(self,
                 name,
                 engine,
                 parameters,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.engine = engine
        self.parameters = parameters

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)
        name_str = f'\n{ind1}name={repr(self.name)},'
        engine_str = f'\n{ind1}engine={repr(self.engine)},'
        parameters_str = f'\n{ind1}parameters={str(self.parameters)},'

        out_str = f'{ind}CreateIntegration(' \
                  f'{name_str}' \
                  f'{engine_str}' \
                  f'{parameters_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'CREATE INTEGRATION {str(self.name)} WITH ENGINE = {repr(self.engine)}, PARAMETERS = \'{json.dumps(self.parameters)}\''
        return out_str
