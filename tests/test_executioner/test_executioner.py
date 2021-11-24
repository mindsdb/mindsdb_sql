import pytest
import numpy as np
from mindsdb_sql import parse_sql
from mindsdb_sql.executioner import execute_plan
from mindsdb_sql.planner import plan_query
from mindsdb_sql.planner.steps import *
from mindsdb_sql.utils import to_single_line


class TestExecutioner:
    def test_basic_select(self, connection_db1, default_data_db1):
        sql = "SELECT * FROM test_db1.test.googleplaystore"

        expected_count = len(connection_db1.query("SELECT * FROM test.googleplaystore"))

        query = parse_sql(sql, dialect='mindsdb')
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

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        assert len(query_plan.steps) == 4
        assert isinstance(query_plan.steps[0], FetchDataframeStep)
        assert isinstance(query_plan.steps[1], FetchDataframeStep)
        assert isinstance(query_plan.steps[2], JoinStep)
        assert isinstance(query_plan.steps[3], ProjectStep)

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

    def test_join_tables_column_alias(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT App AS app_column, Category, Rating, Sentiment
            FROM test_db1.googleplaystore 
            INNER JOIN test_db2.googleplaystore_user_reviews
            ON test_db1.googleplaystore.App = test_db2.googleplaystore_user_reviews.App
        """

        df1 = connection_db1.query("SELECT App as app_column, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App as app_column, Sentiment FROM googleplaystore_user_reviews")
        expected_df = df1.merge(df2, on=['app_column'], how='inner')

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.columns.tolist() == ['app_column', 'Category', 'Rating', 'Sentiment']
        assert out_df.shape == expected_df.shape
        assert (out_df == expected_df).all().all()

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

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])

        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

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

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])

        out_df = execute_plan(query_plan,
                              integration_connections=dict(
                                  test_db1=connection_db1,
                                  test_db2=connection_db2,
                              ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

    def test_join_tables_with_filters(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT App, Category, Rating, Sentiment
            FROM test_db1.googleplaystore AS t1
            INNER JOIN test_db2.googleplaystore_user_reviews AS t2
            ON t1.App = t2.App
            WHERE t1.App = 'Photo Editor & Candy Camera & Grid & ScrapBook'
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment FROM googleplaystore_user_reviews")
        expected_df = df1.merge(df2, on=['App'], how='inner')
        expected_df = expected_df[expected_df['App'] == 'Photo Editor & Candy Camera & Grid & ScrapBook']

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

    def test_execute_groupby_having(self, connection_db2, default_data_db2):
        sql = """
            SELECT App, avg(Sentiment_Polarity) AS avg_sentiment_polarity
            FROM test_db2.googleplaystore_user_reviews
            GROUP BY App
            HAVING CAST(avg_sentiment_polarity AS float) > 0.4
        """

        inner_df = connection_db2.query("SELECT App, Sentiment, Sentiment_Polarity FROM googleplaystore_user_reviews")
        inner_df['Sentiment_Polarity'] = inner_df['Sentiment_Polarity'].astype(float)
        expected_df = inner_df.groupby(['App']).agg({'Sentiment_Polarity': 'mean'}).reset_index()
        expected_df.columns = ['App', 'avg_sentiment_polarity']
        expected_df = expected_df[expected_df['avg_sentiment_polarity'] > 0.4]
        expected_df.index = range(0, len(expected_df))

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df['App'] == expected_df['App']).all()
        np.testing.assert_allclose(out_df['avg_sentiment_polarity'].values, expected_df['avg_sentiment_polarity'].values, atol=1e-5)

    def test_execute_join_tables_with_groupby_having(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT t1.App, t2.Sentiment, avg(t2.Sentiment_Polarity) AS avg_sentiment_polarity
            FROM test_db1.googleplaystore AS t1
            INNER JOIN test_db2.googleplaystore_user_reviews AS t2
            ON t1.App = t2.App
            GROUP BY t1.App, t2.Sentiment
            HAVING avg_sentiment_polarity > 0.4
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment, Sentiment_Polarity FROM googleplaystore_user_reviews")
        inner_df = df1.merge(df2, on=['App'], how='inner')
        expected_df = inner_df.groupby(['App', 'Sentiment']).agg({'Sentiment_Polarity': 'mean'}).reset_index()
        expected_df.columns = ['t1.App', 't2.Sentiment', 'avg_sentiment_polarity']
        expected_df = expected_df[expected_df['avg_sentiment_polarity'] > 0.4]
        expected_df = expected_df.reset_index(drop=True)

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

    def test_execute_join_tables_with_groupby_having_limit_offset(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT t1.App, t2.Sentiment, avg(t2.Sentiment_Polarity) AS avg_sentiment_polarity
            FROM test_db1.googleplaystore AS t1
            INNER JOIN test_db2.googleplaystore_user_reviews AS t2
            ON t1.App = t2.App
            GROUP BY t1.App, t2.Sentiment
            HAVING avg_sentiment_polarity > 0.4
            LIMIT 2
            OFFSET 2
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment, Sentiment_Polarity FROM googleplaystore_user_reviews")
        inner_df = df1.merge(df2, on=['App'], how='inner')
        expected_df = inner_df.groupby(['App', 'Sentiment']).agg({'Sentiment_Polarity': 'mean'}).reset_index()
        expected_df.columns = ['t1.App', 't2.Sentiment', 'avg_sentiment_polarity']
        expected_df = expected_df[expected_df['avg_sentiment_polarity'] > 0.4]
        expected_df = expected_df.reset_index(drop=True).iloc[2:4]

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

    def test_execute_join_tables_with_groupby_having_order_by(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
        sql = """
            SELECT t1.App, t2.Sentiment, avg(t2.Sentiment_Polarity) AS avg_sentiment_polarity
            FROM test_db1.googleplaystore AS t1
            INNER JOIN test_db2.googleplaystore_user_reviews AS t2
            ON t1.App = t2.App
            GROUP BY t1.App, t2.Sentiment
            HAVING avg_sentiment_polarity > 0.4
            ORDER BY t2.Sentiment DESC
        """

        df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
        df2 = connection_db2.query("SELECT App, Sentiment, Sentiment_Polarity FROM googleplaystore_user_reviews")
        inner_df = df1.merge(df2, on=['App'], how='inner')
        expected_df = inner_df.groupby(['App', 'Sentiment']).agg({'Sentiment_Polarity': 'mean'}).reset_index()
        expected_df.columns = ['t1.App', 't2.Sentiment', 'avg_sentiment_polarity']
        expected_df = expected_df[expected_df['avg_sentiment_polarity'] > 0.4]
        expected_df = expected_df.reset_index(drop=True).sort_values(by='t2.Sentiment', ascending=False)

        query = parse_sql(sql, dialect='mindsdb')
        assert str(query) == to_single_line(sql)
        query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
        out_df = execute_plan(query_plan,
                           integration_connections=dict(
                               test_db1=connection_db1,
                               test_db2=connection_db2,
                           ))

        assert out_df.shape == expected_df.shape
        assert (out_df.columns == expected_df.columns).all()
        assert (out_df == expected_df).all().all()

    #
    # def test_execute_join_tables_with_subquery_groupby_having(self, connection_db1, default_data_db1, connection_db2, default_data_db2):
    #     sql = """
    #         SELECT App, avg(Sentiment_Polarity) AS avg_sentiment_polarity
    #         FROM (
    #             SELECT App, Sentiment, CAST(Sentiment_Polarity AS float) AS Sentiment_Polarity
    #             FROM test_db1.googleplaystore AS t1
    #             INNER JOIN test_db2.googleplaystore_user_reviews AS t2
    #             ON t1.App = t2.App
    #         )
    #         AS sub
    #         GROUP BY App
    #         HAVING CAST(avg_sentiment_polarity AS float) > 0.4
    #     """
    #
    #     df1 = connection_db1.query("SELECT App, Category, Rating FROM googleplaystore")
    #     df2 = connection_db2.query("SELECT App, Sentiment, Sentiment_Polarity FROM googleplaystore_user_reviews")
    #     inner_df = df1.merge(df2, on=['App'], how='inner')
    #     inner_df['Sentiment_Polarity'] = inner_df['Sentiment_Polarity'].astype(float)
    #     expected_df = inner_df.groupby(['App']).agg({'Sentiment_Polarity': 'mean'}).reset_index()
    #     expected_df.columns = ['App', 'avg_sentiment_polarity']
    #     expected_df = expected_df[expected_df['avg_sentiment_polarity'] > 0.4]
    #
    #     query = parse_sql(sql, dialect='mindsdb')
    #     assert str(query) == to_single_line(sql)
    #     query_plan = plan_query(query, integrations=['test_db1', 'test_db2'])
    #     out_df = execute_plan(query_plan,
    #                        integration_connections=dict(
    #                            test_db1=connection_db1,
    #                            test_db2=connection_db2,
    #                        ))
    #
    #     assert out_df.shape == expected_df.shape
    #     assert (out_df.columns == expected_df.columns).all()
    #     assert (out_df == expected_df).all().all()


