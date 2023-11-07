from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestAgents:
    def test_create_agent(self):
        sql = '''
            create agent if not exists my_agent
            using 
            model = 'my_model', 
            skills = ['skill1', 'skill2']
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateAgent(
            name=Identifier('my_agent'),
            model='my_model',
            params={'skills': ['skill1', 'skill2']},
            if_not_exists=True
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
        
        # Parse again after rendering to catch problems with rendering.
        ast = parse_sql(str(ast), dialect='mindsdb')
        assert str(ast) == str(expected_ast)

    def test_update_agent(self):
        sql = '''
            update agent my_agent
            set
            model = 'new_model',
            skills = ['new_skill1', 'new_skill2']
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_params = {
            'model': 'new_model',
            'skills': ['new_skill1', 'new_skill2']
        }
        expected_ast = UpdateAgent(
            name=Identifier('my_agent'),
            updated_params=expected_params
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # Parse again after rendering to catch problems with rendering.
        ast = parse_sql(str(ast), dialect='mindsdb')
        assert str(ast) == str(expected_ast)

    def test_drop_agent(self):
        sql = '''
            drop agent if exists my_agent
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropAgent(
            name=Identifier('my_agent'), if_exists=True
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # Parse again after rendering to catch problems with rendering.
        ast = parse_sql(str(ast), dialect='mindsdb')
        assert str(ast) == str(expected_ast)
