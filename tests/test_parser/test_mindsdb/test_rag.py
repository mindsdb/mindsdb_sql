import pytest
from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb.rag import (
    CreateRAG,
    DropRAG
)
from mindsdb_sql.parser.ast import (
    Select,
    Identifier,
    Join,
    Show,
    BinaryOperation,
    Constant,
    Star,
    Delete,
    Insert,
    OrderBy,
)


def test_create_rag():
    # create without select
    sql = """
        CREATE RAG my_rag
        USING
            llm=mindsdb.my_llm,
            knowledge_base_store = mindsdb.my_kb
    """
    ast = parse_sql(sql, dialect="mindsdb")
    expected_ast = CreateRAG(
        name=Identifier("my_rag"),
        if_not_exists=False,
        llm=Identifier(parts=["mindsdb", "my_llm"]),
        knowledge_base_store=Identifier(parts=["mindsdb", "my_kb"]),
        from_select=None,
        params={},
    )
    assert ast == expected_ast

    # the order of llm and knowledge_base_store should not matter
    sql = """
        CREATE RAG my_rag
        USING
            knowledge_base_store = mindsdb.my_kb,
            llm = mindsdb.my_llm
    """
    ast = parse_sql(sql, dialect="mindsdb")
    assert ast == expected_ast

    # create without llm
    # we may allow this in the future when we have a default llm
    sql = """
        CREATE RAG my_rag
        USING
            knowledge_base_store = mindsdb.my_kb
    """

    with pytest.raises(Exception):
        _ = parse_sql(sql, dialect="mindsdb")

    # create without knowledge_base_store
    sql = """
        CREATE RAG my_rag
        USING
            llm = mindsdb.my_llm
    """

    expected_ast = CreateRAG(
        name=Identifier("my_rag"),
        if_not_exists=False,
        llm=Identifier(parts=["mindsdb", "my_llm"]),
        from_select=None,
        params={},
    )

    ast = parse_sql(sql, dialect="mindsdb")

    assert ast == expected_ast

    # create if not exists
    sql = """
        CREATE RAG IF NOT EXISTS my_rag
        USING
            llm = mindsdb.my_llm,
            knowledge_base_store = mindsdb.my_kb
    """
    ast = parse_sql(sql, dialect="mindsdb")
    expected_ast = CreateRAG(
        name=Identifier("my_rag"),
        if_not_exists=True,
        llm=Identifier(parts=["mindsdb", "my_llm"]),
        knowledge_base_store=Identifier(parts=["mindsdb", "my_kb"]),
        from_select=None,
        params={},
    )
    assert ast == expected_ast

    # create with params
    sql = """
        CREATE RAG my_rag
        USING
            llm = mindsdb.my_llm,
            knowledge_base_store = mindsdb.my_kb,
            some_param = 'some value',
            other_param = 'other value'
    """
    ast = parse_sql(sql, dialect="mindsdb")
    expected_ast = CreateRAG(
        name=Identifier("my_rag"),
        if_not_exists=False,
        llm=Identifier(parts=["mindsdb", "my_llm"]),
        knowledge_base_store=Identifier(parts=["mindsdb", "my_kb"]),
        from_select=None,
        params={"some_param": "some value", "other_param": "other value"},
    )
    assert ast == expected_ast


def test_drop_rag():
    # drop if exists
    sql = """
        DROP RAG IF EXISTS my_rag
    """
    ast = parse_sql(sql, dialect="mindsdb")
    expected_ast = DropRAG(
        name=Identifier("my_rag"), if_exists=True
    )
    assert ast == expected_ast

    # drop without if exists
    sql = """
        DROP RAG my_rag
    """
    ast = parse_sql(sql, dialect="mindsdb")

    expected_ast = DropRAG(
        name=Identifier("my_rag"), if_exists=False
    )
    assert ast == expected_ast


@pytest.mark.skip(reason="not implemented")
def test_alter_rag():
    ...

@pytest.mark.skip(reason="not working")
def test_show_rag():
    sql = """
        SHOW RAGS
    """
    ast = parse_sql(sql, dialect="mindsdb")
    expected_ast = Show(
        category="RAGS",
    )
    assert ast == expected_ast


def test_select_from_rag():

    sql = """
    SELECT * 
    FROM my_rag
    WHERE question = 'what is the answer?'
    """
    ast = parse_sql(sql, dialect="mindsdb")

    expected_ast = Select(
        targets=[Star()],
        from_table=Identifier("my_rag"),
        where=BinaryOperation(
                    op="=",
                    args=[Identifier("question"), Constant('what is the answer?')],
                )
    )
    assert ast == expected_ast

@pytest.mark.skip(reason="not implemented")
def test_delete_from_rag():
    ...


@pytest.mark.skip(reason="not implemented")
def test_insert_into_rag():
    ...