from dfsql.sql_parser.base import Statement

MAP_DTYPES = {
    'int4': 'int',
    'float8': 'float'
}

class TypeCast(Statement):
    def __init__(self, type_name, arg, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.type_name = type_name
        self.arg = arg

    def to_string(self, *args, **kwargs):
        return f'CAST({str(self.arg)} AS {self.type_name})'
