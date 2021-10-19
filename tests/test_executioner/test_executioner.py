import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.executioner import execute_plan
from mindsdb_sql.planner import plan_query
from mindsdb_sql.planner.steps import FetchDataframeStep


class TestExecuteSelectFromIntegration:
    def test_basic_select(self, connection, default_data):
        sql = "SELECT * FROM test.test.googleplaystore"

        expected_count = len(connection.query("SELECT * FROM test.googleplaystore"))
        assert expected_count == 100

        query = parse_sql(sql, dialect='mysql')
        query_plan = plan_query(query, integrations=['test'])

        assert len(query_plan.steps) == 1
        assert isinstance(query_plan.steps[0], FetchDataframeStep)
        assert str(query_plan.steps[0].query) == "SELECT * FROM test.googleplaystore"

        out = execute_plan(query_plan, integration_connections=dict(test=connection))
        assert len(out) == expected_count
