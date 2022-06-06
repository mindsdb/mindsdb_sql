import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast import *


class TestAST:
    def test_copy(self):
        ast = Select(
            targets=[Identifier.from_path_str('col1')],
            from_table=Identifier.from_path_str('tab'),
            where=BinaryOperation(
                op='=',
                args=(
                  Identifier.from_path_str('col3'),
                  Constant(1),
                )
            ),
        )

        ast2 = ast.copy()
        # same tree
        assert ast.to_tree() == ast2.to_tree()
        # not same objects
        assert not ast.to_tree() is ast2.to_tree()

        # change
        ast.where.args[0] = Constant(1)
        assert ast.to_tree() != ast2.to_tree()
