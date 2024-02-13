import enum
from prefect import flow, task

"""
The top level query should initiate a flow, each node should have a task function that calls the tasks of it's sub-nodes.
The sub-nodes can also start flows of thier own. Concurrency should be used when calling concurrent sub nodes.  

"""

class ASTNode:
    def __init__(self, **kwargs):
        pass
        # raise Exception("ASTNode must have either a value or left and right.")


class Query(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    def __init__(self, branches, **kwargs):
        super().__init__(**kwargs)

        self.branches = branches

    @flow
    def execute(self):
        for branch in self.branches:
            branch.execute.submit()

class RawQuery(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    def __init__(self, raw_query: str, **kwargs):
        super().__init__(**kwargs)

        self.raw_query = raw_query

    @task
    def execute(self):
        """ submit query to MindsDB SQL Lite database"""
        pass


class NativeQuery(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    def __init__(self, integration: str, raw_query: str, **kwargs):
        super().__init__(**kwargs)

        self.integration = integration
        self.raw_query = raw_query

    @task
    def execute(self):
        """ submit query to the integration database"""
        pass


class Identifier(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    ID_TYPE = enum.Enum('id_type', ['MINDSDB', 'NATIVE', 'AMBIGUOUS'])

    def __init__(self, id_type, column, table=None, alias=None, **kwargs):
        super().__init__(**kwargs)

        self.id_type = id_type
        self.column = column
        self.table = table
        self.alias = alias


class IdentifierList(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    ID_TYPE = enum.Enum('id_type', ['MINDSDB', 'NATIVE', 'AMBIGUOUS'])

    def __init__(self, id_list, **kwargs):
        super().__init__(**kwargs)

        self.id_list = id_list


class Select(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, select_claus, from_claus, where_clause, **kwargs):
        super().__init__(**kwargs)

        self.select_clause = select_claus
        self.from_clause = from_claus
        self.where_clause = where_clause

    def disambiguate_ids(self):
        alias_dict = {}

        alias_dict.update(self.select_clause.get_aliases())
        alias_dict.update(self.from_clause.get_aliases())
        alias_dict.update(self.where_clause.get_aliases())

        #TODO: add logic to cross reference aliases with registered symbols. Aliases cannot be overlaoded.

        self.select_clause.resolve_aliases(alias_dict)
        self.from_clause.resolve_aliases(alias_dict)
        self.where_clause.resolve_aliases(alias_dict)


    @task
    def execute(self):
        """ execute the select statement """

        self.disambiguate_ids()


        for conditional in self.where_clause.yield_conditions():
            # TODO iteratively combine conditionals
            pass

class FromClause(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """

    def __init__(self, id=None, id_list=None, native_query=None, join=None, **kwargs):
        super().__init__(**kwargs)

        self.id = id
        self.id_list = id_list
        self.native_query = native_query
        self.join = join

    def get_aliases(self):

        if self.id:
            return self.id.alias
        if self.id_list:
            return [id.alias for id in self.id_list]
        if native_query:
            



class Operation(ASTNode):
    """
    A MindsDB Operation.

    """

    def __init__(self, **kwargs):
        super.__init__(**kwargs)
