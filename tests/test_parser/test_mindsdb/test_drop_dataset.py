import pytest

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestDropDataset:
    def test_drop_dataset(self):
        sql = "DROP DATASET dsname"
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropDataset(name=Identifier('dsname'))
        assert str(ast).lower() == sql.lower()
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
