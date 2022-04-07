from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent
from mindsdb_sql.parser.ast.select import Star

import re

no_wrap_identifier_regex = re.compile(r'[a-zA-Z_][a-zA-Z_0-9]*')
path_str_parts_regex = re.compile(r'(?:(?:(`[^`]+`))|([^.]+))')


def path_str_to_parts(path_str):
    match = re.finditer(path_str_parts_regex, path_str)
    parts = [x[0].strip('`') for x in match]
    return parts


RESERVED_KEYWORDS = {
    'PERSIST', 'IF', 'EXISTS', 'NULLS', 'FIRST', 'LAST',
    'ORDER', 'BY', 'GROUP', 'PARTITION'
}


class Identifier(ASTNode):
    def __init__(self, path_str=None, parts=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        assert path_str or parts, "Either path_str or parts must be provided for an Identifier"
        assert not (path_str and parts), "Provide either path_str or parts, but not both"
        if path_str and not parts:
            parts = path_str_to_parts(path_str)
        assert isinstance(parts, list)
        self.parts = parts

        from mindsdb_sql.parser.lexer import SQLLexer
        from mindsdb_sql.parser.dialects.mindsdb.lexer import MindsDBLexer

        self.reserved = RESERVED_KEYWORDS
        for word in SQLLexer.tokens | MindsDBLexer.tokens:
            if '_' not in word:
                # exclude combinations
                self.reserved.add(word)

    @classmethod
    def from_path_str(self, value, *args, **kwargs):
        parts = path_str_to_parts(value)
        return Identifier(parts=parts, *args, **kwargs)

    def parts_to_str(self):
        out_parts = []
        for part in self.parts:
            if isinstance(part, Star):
                part = str(part)
            else:
                if (
                    not no_wrap_identifier_regex.fullmatch(part)
                  or
                    part.upper() in self.reserved
                ):
                    part = f'`{part}`'

            out_parts.append(part)
        return '.'.join(out_parts)

    def to_tree(self, *args, level=0, **kwargs):
        alias_str = f', alias={self.alias.to_tree()}' if self.alias else ''
        return indent(level) + f'Identifier(parts={[str(i) for i in self.parts]}{alias_str})'

    def get_string(self, *args, **kwargs):
        return self.parts_to_str()


