import pytest


class TestExecuteSelectFromIntegration:
    def test_basic_select(self, session, default_data):
        sql = "SELECT * FROM test.test_table"

        print(session.execute(sql))
        #
        # query = parse_sql(sql)
        # plan = plan_query(query,
        #                   integrations=['int'])
        # result = execute_plan(plan)



