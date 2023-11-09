from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.utils import indent


class CreateRAG(ASTNode):
    """
    Create a RAG
    """
    def __init__(
        self,
        name,
        llm,
        knowledge_base_store=None,
        from_select=None,
        params=None,
        if_not_exists=False,
        *args,
        **kwargs,
    ):
        """
        Args:
            name: Identifier -- name of the RAG
            llm: Identifier -- name of the LLM to use
            knowledge_base_store: Identifier -- name of the knowledge_base_store to use
            from_select: SelectStatement -- select statement to use as the source of the RAG
            params: dict -- additional parameters to pass to the RAG.
            if_not_exists: bool -- if True, do not raise an error if the RAG already exists
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.llm = llm
        self.knowledge_base_store = knowledge_base_store
        self.params = params
        self.if_not_exists = if_not_exists
        self.from_query = from_select

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        kb_str = f"{ind} knowledge_base_store={self.knowledge_base_store.to_string()},\n" if self.knowledge_base_store else ""
        out_str = f"""
        {ind}CreateRAG(
        {ind}    if_not_exists={self.if_not_exists},
        {ind}    name={self.name.to_string()},
        {ind}    from_query={self.from_query.to_tree(level=level + 1) if self.from_query else None},
        {ind}    llm={self.llm.to_string()},
        {kb_str}{ind}    params={self.params}
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
        # only add knowledge base if it is provided, else we will use the default
        knowledge_base_str = f"  knowledge_base_store = {self.knowledge_base_store.to_string()}" if self.knowledge_base_store else ""

        out_str = (
            f"CREATE RAG {'IF NOT EXISTS' if self.if_not_exists else ''}{self.name.to_string()} "
            f"{from_query_str} "
            f"USING {using_str},"
            f"  LLM = {self.llm.to_string()}, "
            f"{knowledge_base_str}"
        )

        return out_str

    def __repr__(self) -> str:
        return self.to_tree()


class DropRAG(ASTNode):
    """
    Delete a RAG
    """
    def __init__(self, name, if_exists=False, *args, **kwargs):
        """
        Args:
            name: Identifier -- name of the RAG
            if_exists: bool -- if True, do not raise an error if the RAG does not exist
        """
        super().__init__(*args, **kwargs)
        self.name = name
        self.if_exists = if_exists

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        out_str = (
            f"{ind}DropRAG("
            f"{ind}    if_exists={self.if_exists},"
            f"name={self.name.to_string()})"
        )
        return out_str

    def get_string(self, *args, **kwargs):
        out_str = f'DROP RAG {"IF EXISTS" if self.if_exists else ""}{self.name.to_string()}'
        return out_str
