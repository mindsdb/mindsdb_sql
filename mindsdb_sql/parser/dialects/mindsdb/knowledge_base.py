from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateKnowledgeBase(ASTNode):
    def __init__(
        self,
        name,
        model,
        storage,
        from_query=None,
        params=None,
        if_not_exists=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.model = model
        self.storage = storage
        self.params = params
        self.if_not_exists = if_not_exists
        self.from_query = from_query

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = f"""
        {ind}CreateKnowledgeBase(
        {ind}    if_not_exists={self.if_not_exists},
        {ind}    name={self.name.to_string()},
        {ind}    from_query={self.from_query.to_tree(level=level+1) if self.from_query else None},
        {ind}    model={self.model.to_string()},
        {ind}    storage={self.storage.to_string()},
        {ind}    params={self.params}
        {ind})
        """
        return out_str

    def get_string(self, *args, **kwargs):
        params = self.params.copy()
        using_ar = [f"{k}={repr(v)}" for k, v in params.items()]
        using_str = ", ".join(using_ar)
        from_query_str = (
            f"FROM ({self.from_query.get_string()})" if self.from_query else ""
        )

        out_str = (
            f"CREATE KNOWLEDGE_BASE {'IF NOT EXISTS' if self.if_not_exists else ''}{self.name.to_string()} "
            f"{from_query_str} "
            f"MODEL {self.model.to_string()} "
            f"STORAGE {self.storage.to_string()} "
            f"USING {using_str}"
        )

        return out_str

    def __repr__(self) -> str:
        return self.to_tree()


class DropKnowledgeBase(ASTNode):
    def __init__(self, name, if_exists=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = (
            f"{ind}DropKnowledgeBase("
            f"{ind}    if_exists={self.if_exists},"
            f"name={self.name.to_string()})"
        )
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP KNOWLEDGE_BASE {"IF EXISTS" if self.if_exists else ""}{self.name.to_string()}'
        return out_str
