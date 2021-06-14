from mindsdb_sql import ParsingException
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent
import re

no_wrap_identifier_regex = re.compile(r'[a-zA-Z][a-zA-Z_.0-9]*')


class Identifier(ASTNode):
    def __init__(self, parts, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(parts, list)
        self.parts = parts

    @classmethod
    def from_path_str(self, value, *args, **kwargs):
        parts = [part.strip('`') for part in value.split('.')]
        return Identifier(parts=parts, *args, **kwargs)

    def parts_to_str(self):
        out_parts = []
        for part in self.parts:
            if not no_wrap_identifier_regex.fullmatch(part):
                out_parts.append(f'`{part}`')
            else:
                out_parts.append(part)
        return '.'.join(out_parts)

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={repr(self.alias)}' if self.alias else ''
        return indent(level) + f'Identifier(value={repr(self.parts)}{alias_str})'

    def to_string(self, *args, **kwargs):
        value_str = self.parts_to_str()
        return self.maybe_add_alias(value_str)


