# test planning for knowledge base  related queries

import pytest
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query
from mindsdb_sql import parse_sql
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import *
from functools import partial


@pytest.fixture
def planner_context():
    integrations = [
        {
            "name": "my_chromadb",
            "type": "data",
        },
        {
            "name": "my_database",
            "type": "data",
        },
    ]

    predictors = [
        {
            "name": "my_model",
            "integration_name": "mindsdb",
        },
    ]

    additional_context = [
        {
            "name": "my_kb",
            "type": "knowledge_base",
            "model": "mindsdb.my_model",
            "storage": "my_chromadb.my_table",
            "search_vector_field": "search_vector",
            "embeddings_field": "embeddings",
            "content_field": "content",
        }
    ]

    return integrations, predictors, additional_context


def plan_sql(sql, *args, **kwargs):
    return plan_query(parse_sql(sql, dialect="mindsdb"), *args, **kwargs)


def test_insert_into_kb(planner_context):
    integration_context, predictor_context, additional_context = planner_context
    _plan_sql = partial(
        plan_sql,
        default_namespace="mindsdb",
        integrations=integration_context,
        predictor_metadata=predictor_context,
        additional_metadata=additional_context,
    )

    # insert into kb with values
    sql = """
    INSERT INTO my_kb
    (id, content, metadata)
    VALUES
    (1, 'hello world', '{"a": 1, "b": 2}'),
    (2, 'hello world', '{"a": 1, "b": 2}'),
    (3, 'hello world', '{"a": 1, "b": 2}');
    """
    plan = _plan_sql(sql)
    assert len(plan.steps) > 0  # TODO: better to specify t the detail of the plan

    # insert into kb with select
    sql = """
    INSERT INTO my_kb
    (id, content, metadata)
    SELECT
    id, content, metadata
    FROM my_database.my_table
    """
    # this will join the subselect with the underlying model
    # then it will dispatch the query to the underlying storage
    equivalent_sql = """
    INSERT INTO my_chromadb.my_table
    (id, content, metadata, embeddings)
    SELECT
    id, content, metadata, embeddings
    FROM (
        SELECT
        id, content, metadata, embeddings
        FROM my_database.my_table 
        JOIN mindsdb.my_model
    )
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps


def test_select_from_kb(planner_context):
    integration_context, predictor_context, additional_context = planner_context
    _plan_sql = partial(
        plan_sql,
        default_namespace="mindsdb",
        integrations=integration_context,
        predictor_metadata=predictor_context,
        additional_metadata=additional_context,
    )

    # select from kb without where
    sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_kb
    """
    # this will dispatch the query to the underlying storage
    equivalent_sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_chromadb.my_table
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps

    # select from kb with search_query
    sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_kb
    WHERE
    search_query = 'hello world'
    """
    # this will dispatch the search_query to the underlying model
    # then it will dispatch the query to the underlying storage
    equivalent_sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_chromadb.my_table
    WHERE
    search_vector = (
        SELECT
        embeddings
        FROM mindsdb.my_model
        WHERE
        content = 'hello world'
    )
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps

    # select from kb with no search_query and just metadata query
    sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_kb
    WHERE
    `metadata.a` = 1
    """
    # this will dispatch the whole query to the underlying storage
    equivalent_sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_chromadb.my_table
    WHERE
    `metadata.a` = 1
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps

    # select from kb with search_query and metadata query
    sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_kb
    WHERE
    search_query = 'hello world'
    AND
    `metadata.a` = 1
    """
    # this will dispatch the search_query to the underlying model
    # then it will dispatch the query to the underlying storage
    equivalent_sql = """
    SELECT
    id, content, embeddings, metadata
    FROM my_chromadb.my_table
    WHERE
    search_vector = (
        SELECT
        embeddings
        FROM mindsdb.my_model
        WHERE
        content = 'hello world'
    )
    AND
    `metadata.a` = 1
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps


@pytest.mark.skip(reason="not implemented")
def test_update_kb():
    ...


def test_delete_from_kb(planner_context):
    integration_context, predictor_context, additional_context = planner_context
    _plan_sql = partial(
        plan_sql,
        default_namespace="mindsdb",
        integrations=integration_context,
        predictor_metadata=predictor_context,
        additional_metadata=additional_context,
    )

    sql = """
    DELETE FROM my_kb
    WHERE
    id = 1
    """
    # this will dispatch the delete to the underlying storage
    equivalent_sql = """
    DELETE FROM my_chromadb.my_table
    WHERE
    id = 1
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps

    sql = """
    DELETE FROM my_kb
    WHERE
        `metadata.a` = 1
    """
    # this will dispatch the delete to the underlying storage
    equivalent_sql = """
    DELETE FROM my_chromadb.my_table
    WHERE
        `metadata.a` = 1
    """
    plan = _plan_sql(sql)
    expected_plan = _plan_sql(equivalent_sql)

    assert plan.steps == expected_plan.steps
