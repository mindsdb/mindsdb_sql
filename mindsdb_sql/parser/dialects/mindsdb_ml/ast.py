import enum
from prefect import flow, task
from prefect.runtime import flow_run, task_run
import operator
from typing import Union

"""
The top level query should initiate a flow, each node should have a task function that calls the tasks of it's sub-nodes.
The sub-nodes can also start flows of thier own. Concurrency should be used when calling concurrent sub nodes.  

"""


def generate_task_name():
    flow_name = flow_run.flow_name
    task_name = task_run.task_name

    parameters = task_run.parameters
    name = parameters["name"]
    limit = parameters["limit"]

    return f"{flow_name}-{task_name}-with-{name}-and-{limit}"


class ASTNode:
    def __init__(self, **kwargs):
        pass
        # raise Exception("ASTNode must have either a value or left and right.")

    def get_aliases(self) -> list:
        # return a list of aliases. Must be implemented by subclasses.
        return []

    def resolve_aliases(self, aliases: dict) -> bool:
        # resolves aliases and returns a True on success. Must be implemented by subclasses.
        return True

    def __str__(self) -> str:
        return ''


class Query(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, branches: list, **kwargs):
        super().__init__(**kwargs)

        self.branches = branches

    @flow
    def execute(self):
        for branch in self.branches:
            branch.execute.submit()

    def __str__(self) -> str:
        return ' '.join([str(branch) for branch in self.branches])


class RawQuery(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, raw_query: str, parentheses: bool = False, **kwargs):
        super().__init__(**kwargs)

        self.raw_query = raw_query
        self.parentheses = parentheses

    def execute(self):
        @task(name='Raw Query',
              description=str(self),
              task_run_name=generate_task_name()
              )
        def task_fn():
            """ submit query to MindsDB SQL Lite database"""
            pass

    def __str__(self) -> str:
        if not self.parentheses:
            return str(self.raw_query)
        else:
            return '(' + str(self.raw_query) + ')'


class NativeQuery(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, integration: str, raw_query: str, **kwargs):
        super().__init__(**kwargs)

        self.integration = integration
        self.raw_query = raw_query

    def execute(self):
        @task(name='Native Query',
              description=str(self),
              task_run_name=generate_task_name()
              )
        def task_fn():
            """ submit query to Native database"""
            pass

    def __str__(self) -> str:
        return str(self.integration) + ' (' + str(self.raw_query) + ')'


class Identifier(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    ID_TYPE = enum.Enum('id_type', ['MINDSDB', 'NATIVE', 'AMBIGUOUS'])

    def __init__(self, column, table=None, alias=None, id_type=None, **kwargs):
        super().__init__(**kwargs)

        # type will be resolved when query is disambiguated.
        self.id_type = id_type

        # column in the table: "table.column"
        self.column = column

        # table in the database: "table.column"
        self.table = table
        self.table_alias = None

        # alias is "AS alias"
        self.alias = alias

    def get_aliases(self):
        if self.alias:
            return [self.alias]
        else:
            return []

    def resolve_aliases(self, aliases: dict) -> bool:
        self.resolved_table_alias = aliases[self.alias]

        return True

    def __str__(self) -> str:
        if self.table and self.alias:
            return str(self.table) + '.' + str(self.column) + ' AS ' + str(self.alias)
        elif self.table:
            return str(self.table) + '.' + str(self.column)
        elif self.alias:
            return str(self.column) + ' AS ' + str(self.alias)
        else:
            return str(self.column)


class IdentifierList(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    ID_TYPE = enum.Enum('id_type', ['MINDSDB', 'NATIVE', 'AMBIGUOUS'])

    def __init__(self, id_list: list['Identifer'], **kwargs):
        super().__init__(**kwargs)

        self.id_list = id_list

    def get_aliases(self):
        alias_list = []
        for idid in self.id_list:
            alias_list.extend(idid.get_aliases())

        return alias_list

    def resolve_aliases(self, aliases: dict) -> bool:
        return any([idid.resolve_aliases(aliases=aliases) for idid in self.id_list])

    def __str__(self) -> str:
        return ' '.join([str(idid) for idid in self.id_list])


class Select(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, select_clause, from_clause, where_clause, **kwargs):
        super().__init__(**kwargs)

        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause

    def get_aliases(self):
        # Select statements are blocking
        return []

    def resolve_aliases(self, aliases: dict):
        alias_dict = {}

        alias_dict.update(self.select_clause.get_aliases())
        alias_dict.update(self.from_clause.get_aliases())
        alias_dict.update(self.where_clause.get_aliases())

        # TODO: add logic to cross reference aliases with registered symbols. Aliases cannot be overlaoded.

        return any([self.select_clause.resolve_aliases(alias_dict),
                    self.from_clause.resolve_aliases(alias_dict),
                    self.where_clause.resolve_aliases(alias_dict)])

    def execute(self):
        @task(name='Select',
              description=str(self),
              task_run_name=generate_task_name()
              )
        def task_fn():
            self.resolve_aliases()

            # TODO: implement select.
            pass

    def __str__(self) -> str:
        return str(self.select_clause) + ' ' + str(self.from_clause) + ' ' + str(self.where_clause)


class SelectClause(ASTNode):

    def __init__(self, select_arg):
        self.select_arg = select_arg

    def get_aliases(self):
        # Select statements are blocking
        return self.select_arg.get_aliases()

    def resolve_aliases(self, aliases: dict):
        self.select_arg.resolve_aliases(aliases=aliases)

    def __str__(self) -> str:
        return 'SELECT ' + str(self.select_arg)


class FromClause(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, from_arg: Union[Identifier, IdentifierList, NativeQuery, 'JoinClause'], **kwargs):
        super().__init__(**kwargs)

        self.from_arg = from_arg

    def get_aliases(self):
        return self.from_arg.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.from_arg.resolve_aliases(aliases)

    def __str__(self) -> str:
        return 'FROM ' + str(self.from_arg)


class JoinClause(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self,
                 left_arg: Union[Identifier, 'JoinClause'],
                 right_arg: Identifier | NativeQuery,
                 **kwargs):
        super().__init__(**kwargs)

        self.left_arg = left_arg
        self.right_arg = right_arg

    def get_aliases(self):
        return self.left_arg.get_aliases() + self.right_arg.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.left_arg.resolve_aliases(aliases) and self.right_arg.resolve_aliases(aliases)

    def __str__(self) -> str:
        return str(self.left_arg) + ' JOIN ' + str(self.right_arg)


class WhereClause(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, where_conditions: Union['Condition', 'ConditionList', None] = None, limit=None, **kwargs):
        super().__init__(**kwargs)

        self.where_conditions = where_conditions
        self.limit = limit

    def get_aliases(self):
        return self.where_conditions.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.where_conditions.resolve_aliases(aliases)

    def __str__(self) -> str:
        if not self.where_conditions:
            return ''
        elif self.limit:
            return 'WHERE ' + str(self.where_conditions) + ' LIMIT ' + str(self.limit)
        else:
            return 'WHERE ' + str(self.where_conditions)


class ConditionList(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self,
                 left_condition: Union['Condition', 'ConditionList'],
                 boolean: 'Boolean',
                 right_condition: Union['Condition', 'ConditionList'],
                 parantheses: bool = None,
                 **kwargs):
        super().__init__(**kwargs)

        self.left_condition = left_condition
        self.boolean = boolean
        self.right_condition = right_condition
        self.parantheses = parantheses

    def get_aliases(self):
        return self.left_condition.get_aliases() + self.right_condition.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.left_condition.resolve_aliases(aliases) and self.right_condition.resolve_aliases(aliases)

    def __str__(self) -> str:
        core_str = str(self.left_condition) + ' ' + str(self.boolean) + ' ' + str(self.right_condition)
        if self.parantheses:
            core_str = '(' + core_str + ')'

        return core_str


class Boolean(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    OP_TYPE = {'AND': operator.and_,
               'OR': operator.or_,
               'NOT': operator.not_}

    def __init__(self, boolean: str, **kwargs):
        super().__init__(**kwargs)

        self.boolean = boolean
        self.operator = self.OP_TYPE[boolean]

    def __str__(self) -> str:
        return str(self.boolean)


class Condition(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, left_arg: Identifier,
                 comparator: 'Comparator',
                 right_arg: Union[Identifier, 'Value'],
                 parantheses: bool = None,
                 **kwargs):
        super().__init__(**kwargs)

        self.left_arg = left_arg
        self.comparator = comparator
        self.right_arg = right_arg
        self.parantheses = parantheses

    def get_aliases(self):
        return self.left_arg.get_aliases() + self.right_arg.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.left_arg.resolve_aliases(aliases) and self.right_arg.resolve_aliases(aliases)

    def __str__(self) -> str:
        core_str = str(self.left_arg) + ' ' + str(self.comparator) + ' ' + str(self.right_arg)
        if self.parantheses:
            core_str = '(' + core_str + ')'

        return core_str


class Comparator(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    OP_TYPE = {'=': operator.eq,
               '!=': operator.ne,
               '>=': operator.ge,
               '>': operator.gt,
               '<=': operator.le,
               '<': operator.lt
               }

    def __init__(self, comparator: str, **kwargs):
        super().__init__(**kwargs)

        self.comparator = comparator
        self.operator = self.OP_TYPE[comparator]

    def __str__(self) -> str:
        return str(self.comparator)


class Value(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, value: int | float | str | bool, **kwargs):
        super().__init__(**kwargs)

        self.value = value

    def __str__(self) -> str:
        return str(self.value)
