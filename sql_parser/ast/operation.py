from sql_parser.ast.base import ASTNode
from sql_parser.exceptions import ParsingException


class Operation(ASTNode):
    def __init__(self, op, args, *args_, **kwargs):
        super().__init__(*args_, **kwargs)

        self.op = op
        self.args = args
        self.assert_arguments()

    def assert_arguments(self):
        if not self.args:
            raise ParsingException(f'Expected arguments for operation "{self.op}"')

    def get_arg_strings(self):
        return [arg.to_string() for arg in self.args]

    def to_string(self, *args, **kwargs):
        arg_strs = self.get_arg_strings()
        args_str = ','.join(arg_strs)
        return self.maybe_add_alias(self.maybe_add_parentheses(f'{self.op}({args_str})'))


class BinaryOperation(Operation):
    def to_string(self, *args, **kwargs):
        arg_strs = arg_strs = self.get_arg_strings()
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
#
#
# class ComparisonPredicate(UnaryOperation):
#     def to_string(self, *args, **kwargs):
#         return self.maybe_add_alias(f'{self.args[0].to_string()} {self.op}')
#
#


class Function(Operation):
    def to_string(self, *args, **kwargs):
        args_str = ', '.join([arg.to_string() for arg in self.args])
        return self.maybe_add_alias(self.maybe_add_parentheses(f'{self.op}({args_str})'))

#
# class AggregateFunction(Function):
#     pass
#
#
# class InOperation(BinaryOperation):
#     def __init__(self, *args, **kwargs):
#         super().__init__(op='IN', *args, **kwargs)
#
#
# def operation_factory(op, args, raw=None):
#     if op == 'IN':
#         return InOperation(args_=args)
#
#     op_class = Operation
#     if len(args) == 2:
#         op_class = BinaryOperation
#     elif len(args) == 1:
#         op_class = UnaryOperation
#
#     return op_class(op=op,
#              args_=args,
#              raw=raw)
