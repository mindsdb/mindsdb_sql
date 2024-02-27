import enum
import datetime
from datetime import timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.runtime import flow_run, task_run
import operator
from typing import Union

"""
The top level query should initiate a flow, each node should have a task function that calls the tasks of it's sub-nodes.
The sub-nodes can also start flows of thier own. Concurrency should be used when calling concurrent sub nodes.  

"""


def generate_flow_run_name(name: str):
    date = datetime.datetime.now(datetime.timezone.utc)

    return f"{date:%A}-{name}"


def generate_task_name():
    flow_name = flow_run.flow_name
    task_name = task_run.task_name

    parameters = task_run.parameters
    name = parameters["name"] if "name" in parameters.keys() else None
    limit = parameters["limit"] if "limit" in parameters.keys() else None

    return f"{flow_name}-{task_name}-with-{name}-and-{limit}"


class ASTNode:
    def __init__(self, **kwargs):
        pass
        # raise Exception("ASTNode must have either a value or left and right.")

    def get_aliases(self) -> dict:
        # return a list of aliases. Must be implemented by subclasses.
        return {}

    def resolve_aliases(self, aliases: dict) -> bool:
        # resolves aliases and returns a True on success. Must be implemented by subclasses.
        return True

    def __str__(self) -> str:
        return ''


class Query(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, clauses: list, **kwargs):
        super().__init__(**kwargs)

        self.clauses = clauses

    def execute(self):
        @flow(name='Query',
              description=str(self),
              flow_run_name=generate_flow_run_name('SELECT'),
              log_prints=True
              )
        def flow_fn(clauses):
            # concurrent execution
            results = [clause.execute() for clause in clauses]

            # await results
            for i, clause in enumerate(clauses):
                if clause.is_task:
                    results[i] = results[i].result()

            return results

        return flow_fn(clauses=self.clauses)

    def __str__(self) -> str:
        return ' '.join([str(branch) for branch in self.clauses])


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

        return task_fn.submit()

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

        return task_fn

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

    def get_aliases(self) -> dict:
        if self.alias:
            return {self.alias: self}
        else:
            return {}

    def resolve_aliases(self, aliases: dict) -> bool:

        if self.table and self.table in aliases:
            self.table_alias = aliases[self.table]

            return True
        elif self.table is None:
            return True
        else:
            return False  # Alias is unresolved.

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

    def get_aliases(self) -> dict:
        alias_dict = {}
        for idid in self.id_list:
            alias_dict.update(idid.get_aliases())
        return alias_dict

    def resolve_aliases(self, aliases: dict) -> bool:
        return any([idid.resolve_aliases(aliases=aliases) for idid in self.id_list])

    def __str__(self) -> str:
        return ', '.join([str(idid) for idid in self.id_list])


class Select(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self,
                 select_clause: 'SelectClause',
                 from_clause: 'FromClause',
                 where_clause: 'WhereClause',
                 **kwargs):
        super().__init__(**kwargs)

        self.select_clause = select_clause
        self.from_clause = from_clause
        self.where_clause = where_clause
        self.is_task = False

    def get_aliases(self) -> dict:
        # Select statements are blocking
        return {}

    def resolve_aliases(self, aliases: dict = {}):
        alias_dict = {}

        alias_dict.update(self.select_clause.get_aliases())
        alias_dict.update(self.from_clause.get_aliases())
        alias_dict.update(self.where_clause.get_aliases())

        self.alias_dict = alias_dict

        # TODO: add logic to cross reference aliases with registered symbols. Aliases cannot be overlaoded.

        return any([self.select_clause.resolve_aliases(alias_dict),
                    self.from_clause.resolve_aliases(alias_dict),
                    self.where_clause.resolve_aliases(alias_dict)])

    def execute(self):
        self.resolve_aliases()

        clauses = [self.select_clause, self.from_clause, self.where_clause]

        @flow(name='Select',
              description=str(self),
              flow_run_name=generate_flow_run_name('SELECT'),
              log_prints=True
              )
        def flow_fn(clauses):
            # concurrent execution
            results = [clause.execute() for clause in clauses]

            # await results
            for i, clause in enumerate(clauses):
                if clause.is_task:
                    results[i] = results[i].result()

            return results

        # update self
        self.select_clause, self.from_clause, self.where_clause = flow_fn(clauses=clauses)

        return

    def __str__(self) -> str:
        return str(self.select_clause) + ' ' + str(self.from_clause) + ' ' + str(self.where_clause)


class SelectClause(ASTNode):

    def __init__(self, select_arg):
        self.select_arg = select_arg
        self.is_task = True

    def get_aliases(self) -> dict:
        # Select statements are blocking
        return self.select_arg.get_aliases()

    def resolve_aliases(self, aliases: dict):
        return self.select_arg.resolve_aliases(aliases=aliases)

    def execute(self):
        @task(name='Select Clause',
              description=str(self),
              task_run_name=generate_task_name()
              )
        def task_fn():
            """Get information on select clause from backend"""
            pass

        return task_fn.submit()

    def __str__(self) -> str:
        return 'SELECT ' + str(self.select_arg)


class FromClause(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, from_arg: Union[Identifier, IdentifierList, NativeQuery, 'JoinClause'], **kwargs):
        super().__init__(**kwargs)

        self.from_arg = from_arg
        self.is_task = False

    def get_aliases(self) -> dict:
        return self.from_arg.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.from_arg.resolve_aliases(aliases)

    def execute(self):
        @flow(name='From Clause',
              description=str(self),
              flow_run_name=generate_flow_run_name('SELECT'),
              log_prints=True
              )
        def flow_fn():
            """Get information on from clause from backend"""
            pass

        return flow_fn()

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
        self.is_task = False

    def get_aliases(self) -> dict:
        return_dict = {}
        return_dict.update(self.left_arg.get_aliases())
        return_dict.update(self.right_arg.get_aliases())
        return return_dict

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.left_arg.resolve_aliases(aliases) and self.right_arg.resolve_aliases(aliases)

    def execute(self):
        @flow(name='Join Clause',
              description=str(self),
              flow_run_name=generate_flow_run_name('SELECT'),
              log_prints=True
              )
        def flow_fn():
            """Get information on from clause from backend"""
            pass

        return flow_fn()

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
        self.is_task = True

    def get_aliases(self) -> dict:
        return self.where_conditions.get_aliases()

    def resolve_aliases(self, aliases: dict) -> bool:
        return self.where_conditions.resolve_aliases(aliases)

    def execute(self):
        @task(name='Where Clause',
              description=str(self),
              task_run_name=generate_task_name()
              )
        def task_fn():
            """Get information on from clause from backend"""
            pass

        return task_fn.submit()

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

    def get_aliases(self) -> dict:
        return_dict = {}
        return_dict.update(self.left_condition.get_aliases())
        return_dict.update(self.right_condition.get_aliases())
        return return_dict

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

    def get_aliases(self) -> dict:
        return_dict = {}
        return_dict.update(self.left_arg.get_aliases())
        return_dict.update(self.right_arg.get_aliases())
        return return_dict

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
