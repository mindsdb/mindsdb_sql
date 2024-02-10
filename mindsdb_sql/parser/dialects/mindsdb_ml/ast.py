import enum


"""
The top level query should initiate a flow, each node should have a task function that calls the tasks of it's sub-nodes.
The sub-nodes can also start flows of thier own. Concurrency should be used when calling concurrent sub nodes.  

"""

class ASTNode:
    def __init__(self, **kwargs):
        self.value = kwargs.get('value')
        self.left = kwargs.get('left')
        self.right = kwargs.get('right')

        if not (self.value or (self.left and self.right)):
            raise Exception("ASTNode must have either a value or left and right.")


class Identifier(ASTNode):
    """
    A MindsDB Identifier. Terminal Node type.
    """
    ID_TYPE = enum.Enum('id_type', ['MINDSDB', 'NATIVE', 'AMBIGUOUS'])

    def __init__(self, id_type, **kwargs):
        super().__init__(**kwargs)

        self.id_type = id_type
        self.alias = None


class Operation(ASTNode):
    """
    A MindsDB Operation.

    """

    def __init__(self, **kwargs):
        super.__init__(**kwargs)
