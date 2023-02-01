import pytest

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestJobs:
    def test_test_create_job(self):
        sql = '''
            create job proj2.j1 ( 
                select * from pg.tbl1 where b>{{PREVIOUS_START_DATE}}
            )
            start now
            end '2024-01-01'
            every hour
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateJob(
            name=Identifier('proj2.j1'),
            query_str="select * from pg.tbl1 where b>{{PREVIOUS_START_DATE}}",
            start_str="now",
            end_str="2024-01-01",
            repeat_str="hour"
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # 2

        sql = '''
            create job j1 as ( 
                retrain p1; retrain p2
            )
            every '2 hours'
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateJob(
            name=Identifier('j1'),
            query_str="retrain p1; retrain p2",
            repeat_str="2 hours"
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # 3

        sql = '''
            create job j1 ( 
                retrain p1; retrain p2
            )
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateJob(
            name=Identifier('j1'),
            query_str="retrain p1; retrain p2",
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_drop_job(self):
        sql = '''
            drop job proj1.j1
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropJob(
            name=Identifier('proj1.j1'),
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()