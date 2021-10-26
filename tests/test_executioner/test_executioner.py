import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.executioner import execute_plan
from mindsdb_sql.planner import plan_query
from mindsdb_sql.planner.steps import FetchDataframeStep
from mindsdb_sql.utils import to_single_line


class TestExecuteSelectFromIntegration:
    def test_basic_select(self, connection_db1, default_data_db1):
        sql = "SELECT * FROM test_db1.test.googleplaystore"

        expected_count = len(connection_db1.query("SELECT * FROM test.googleplaystore"))
        assert expected_count == 100

        query = parse_sql(sql, dialect='mysql')
        query_plan = plan_query(query, integrations=['test_db1'])

        assert len(query_plan.steps) == 1
        assert isinstance(query_plan.steps[0], FetchDataframeStep)
        assert str(query_plan.steps[0].query) == "SELECT * FROM test.googleplaystore"

        out = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                           ))
        assert len(out) == expected_count

    def test_join_tables(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT App, Category, Rating, Sentiment
            FROM test_db1.googleplaystore 
            INNER JOIN test_db2.googleplaystore_user_reviews
            ON test_db1.googleplaystore.App = test_db2.googleplaystore_user_reviews.App
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment FROM googleplaystore_user_reviews")
        expected_df = df1.merge(df2, on=['App'], how='inner')

        query = parse_sql(sql, dialect='mysql')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        #
        # assert len(query_plan.steps) == 1
        # assert isinstance(query_plan.steps[0], FetchDataframeStep)
        # assert str(query_plan.steps[0].query) == "SELECT * FROM test.googleplaystore"

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert (out_df == expected_df).all()

    def test_join_tables_specify_databases(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT App, Category, Rating, Sentiment
            FROM test_db1.test.googleplaystore 
            INNER JOIN test_db2.test.googleplaystore_user_reviews
            ON test_db1.test.googleplaystore.App = test_db2.test.googleplaystore_user_reviews.App
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM test.googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment FROM test.googleplaystore_user_reviews")
        expected_df = df1.merge(df2, on=['App'], how='inner')

        query = parse_sql(sql, dialect='mysql')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        #
        # assert len(query_plan.steps) == 1
        # assert isinstance(query_plan.steps[0], FetchDataframeStep)
        # assert str(query_plan.steps[0].query) == "SELECT * FROM test.googleplaystore"

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert (out_df == expected_df).all()

    def test_join_tables_aliased(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT App, Category, Rating, Sentiment
            FROM test_db1.test.googleplaystore AS t1 
            INNER JOIN test_db2.test.googleplaystore_user_reviews AS t2
            ON t1.App = t2.App
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM test.googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment FROM test.googleplaystore_user_reviews")
        expected_df = df1.merge(df2, on=['App'], how='inner')

        query = parse_sql(sql, dialect='mysql')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        #
        # assert len(query_plan.steps) == 1
        # assert isinstance(query_plan.steps[0], FetchDataframeStep)
        # assert str(query_plan.steps[0].query) == "SELECT * FROM test.googleplaystore"

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert (out_df == expected_df).all()

    def test_join_tables_with_filters(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT App, Category, Rating, Sentiment
            FROM test_db1.googleplaystore 
            INNER JOIN test_db2.googleplaystore_user_reviews
            ON test_db1.googleplaystore.App = test_db2.googleplaystore_user_reviews.App
            WHERE App = 'Photo Editor & Candy Camera & Grid & ScrapBook'
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment FROM googleplaystore_user_reviews")
        expected_df = df1.merge(df2, on=['App'], how='inner')
        expected_df = expected_df[expected_df['App'] == 'Photo Editor & Candy Camera & Grid & ScrapBook']

        query = parse_sql(sql, dialect='mysql')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert (out_df == expected_df).all()
