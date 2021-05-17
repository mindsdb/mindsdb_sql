from mindsdb_sql.ast.base import ASTNode
from mindsdb_sql.exceptions import ParsingException
from mindsdb_sql.utils import indent


class Operation(ASTNode):
    def __init__(self, op, args, *args_, **kwargs):
        super().__init__(*args_, **kwargs)

        self.op = op
        self.args = args
        self.assert_arguments()

    def assert_arguments(self):
        if not self.args:
            raise ParsingException(f'Expected arguments for operation "{self.op}"')

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        arg_trees = [arg.to_tree(level=level+2) for arg in self.args]
        arg_trees_str = ",\n".join(arg_trees)
        out_str = f'{ind}{self.__class__.__name__}(op={repr(self.op)},\n{ind1}args=(\n{arg_trees_str}\n{ind1})\n{ind})'
        return out_str

    def to_string(self, *args, **kwargs):
        arg_strs = [arg.to_string() for arg in self.args]
        args_str = ','.join(arg_strs)
        return self.maybe_add_alias(self.maybe_add_parentheses(f'{self.op}({args_str})'))


class BinaryOperation(Operation):
    def to_string(self, *args, **kwargs):
        arg_strs = [arg.to_string() for arg in self.args]
        return self.maybe_add_alias(self.maybe_add_parentheses(f'{arg_strs[0]} {self.op} {arg_strs[1]}'))

    def assert_arguments(self):
        if len(self.args) != 2:
            raise ParsingException(f'Expected two arguments for operation "{self.op}"')


class UnaryOperation(Operation):
    def to_string(self, *args, **kwargs):
        return self.maybe_add_alias(self.maybe_add_parentheses(f'{self.op} {self.args[0].to_string()}'))

    def assert_arguments(self):
        if len(self.args) != 1:
            raise ParsingException(f'Expected one argument for operation "{self.op}"')


class Function(Operation):
    def __init__(self, *args, distinct=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.distinct = distinct

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        arg_trees = [arg.to_tree(level=level+2) for arg in self.args]
        arg_trees_str = ",\n".join(arg_trees)
        out_str = f'{ind}{self.__class__.__name__}(op={repr(self.op)}, distinct={repr(self.distinct)},\n{ind1}args=(\n{arg_trees_str}\n{ind1})\n{ind})'
        return out_str

    def to_string(self, *args, **kwargs):
        args_str = ', '.join([arg.to_string() for arg in self.args])
        distinct_str = 'DISTINCT ' if self.distinct else ''
        return self.maybe_add_alias(self.maybe_add_parentheses(f'{self.op}({distinct_str}{args_str})'))
